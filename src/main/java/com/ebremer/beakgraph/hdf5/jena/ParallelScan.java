package com.ebremer.beakgraph.hdf5.jena;

import java.lang.ref.Cleaner;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.jena.atlas.iterator.IteratorCloseable;
import org.apache.jena.query.QueryCancelledException;
import org.apache.jena.sparql.engine.iterator.Abortable;

/**
 * Runs independent scan chunks on a shared worker pool and streams their rows
 * to the single consumer thread, batch-wise through a bounded queue. Row order
 * is interleaved across chunks - fine for BGP semantics, which promise no order.
 *
 * <p>Lifecycle is the hard part and is triple-covered, matching how Jena's
 * execution actually tears iterators down:
 * <ul>
 *   <li><b>Cancel</b> (timeout, abort): registered in the solver's kill-list, so
 *       {@code QueryIterAbortable.requestCancel} calls {@link #abort()}; workers
 *       additionally poll the engine's shared cancel signal.</li>
 *   <li><b>Close</b> (LIMIT, normal end): {@code Iter.close} cascades through the
 *       chain (IterMap / IterAbortable / IteratorFlatMap all propagate close) to
 *       {@link #close()}.</li>
 *   <li><b>Abandonment backstop</b>: a {@link Cleaner} stops the workers if an
 *       owning iterator is dropped without either of the above.</li>
 * </ul>
 * Workers never block indefinitely: every queue offer/poll uses a short timeout
 * and rechecks the stop conditions, so a worker parked against a full queue
 * exits within one tick of shutdown. A worker failure is recorded, stops the
 * scan, and is rethrown to the consumer rather than swallowed.
 */
public final class ParallelScan implements IteratorCloseable<BindingNodeId>, Abortable {

    /** Parallel scans started; observability for tests and diagnostics. */
    public static final AtomicLong HITS = new AtomicLong();
    /** Currently running chunk workers; tests assert this drains to 0 on close. */
    public static final AtomicInteger ACTIVE_WORKERS = new AtomicInteger();

    private static final int BATCH = 256;
    private static final long TICK_MS = 25;
    private static final BindingNodeId[] END = new BindingNodeId[0];
    private static final Cleaner CLEANER = Cleaner.create();

    private static final ExecutorService POOL = Executors.newFixedThreadPool(
            Math.max(2, Runtime.getRuntime().availableProcessors()),
            new ThreadFactory() {
                private final AtomicInteger n = new AtomicInteger();
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "beakgraph-scan-" + n.incrementAndGet());
                    t.setDaemon(true);
                    return t;
                }
            });

    private final ArrayBlockingQueue<BindingNodeId[]> queue;
    private final AtomicBoolean stop;
    private final AtomicBoolean cancelSignal; // the engine's; may be null
    private final AtomicReference<RuntimeException> failure = new AtomicReference<>();
    private int workersRemaining;

    private BindingNodeId[] batch;
    private int batchPos;

    ParallelScan(List<Supplier<Iterator<BindingNodeId>>> chunks, AtomicBoolean cancelSignal) {
        this.queue = new ArrayBlockingQueue<>(Math.max(4, chunks.size() * 2));
        this.stop = new AtomicBoolean(false);
        this.cancelSignal = cancelSignal;
        this.workersRemaining = chunks.size();
        HITS.incrementAndGet();
        // The cleaner action must not capture `this`; it shares the stop flag and
        // queue so abandoned scans still release their workers.
        AtomicBoolean stopRef = this.stop;
        ArrayBlockingQueue<BindingNodeId[]> queueRef = this.queue;
        CLEANER.register(this, () -> {
            stopRef.set(true);
            queueRef.clear();
        });
        for (Supplier<Iterator<BindingNodeId>> chunk : chunks) {
            POOL.execute(() -> runChunk(chunk));
        }
    }

    private boolean stopped() {
        return stop.get() || (cancelSignal != null && cancelSignal.get());
    }

    private void runChunk(Supplier<Iterator<BindingNodeId>> chunk) {
        ACTIVE_WORKERS.incrementAndGet();
        try {
            Iterator<BindingNodeId> it = chunk.get();
            BindingNodeId[] buf = new BindingNodeId[BATCH];
            int n = 0;
            while (!stopped() && it.hasNext()) {
                buf[n++] = it.next();
                if (n == BATCH) {
                    if (!offer(buf)) return;
                    buf = new BindingNodeId[BATCH];
                    n = 0;
                }
            }
            if (n > 0 && !stopped()) {
                BindingNodeId[] tail = new BindingNodeId[n];
                System.arraycopy(buf, 0, tail, 0, n);
                offer(tail);
            }
        } catch (RuntimeException e) {
            failure.compareAndSet(null, e);
            stop.set(true);
        } finally {
            offerEnd();
            ACTIVE_WORKERS.decrementAndGet();
        }
    }

    /** Enqueues, rechecking stop each tick. Returns false when shut down. */
    private boolean offer(BindingNodeId[] b) {
        try {
            while (!stopped()) {
                if (queue.offer(b, TICK_MS, TimeUnit.MILLISECONDS)) {
                    return true;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return false;
    }

    /**
     * Best-effort END marker so the consumer can count workers out. When the
     * scan is stopped the consumer no longer relies on END counting, so bailing
     * out is fine.
     */
    private void offerEnd() {
        try {
            while (!queue.offer(END, TICK_MS, TimeUnit.MILLISECONDS)) {
                if (stopped()) {
                    return;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean hasNext() {
        if (batch != null && batchPos < batch.length) {
            return true;
        }
        batch = null;
        while (workersRemaining > 0) {
            RuntimeException e = failure.get();
            if (e != null) {
                shutdown();
                throw e;
            }
            if (stop.get()) {
                return false; // closed under us
            }
            if (cancelSignal != null && cancelSignal.get()) {
                shutdown();
                throw new QueryCancelledException();
            }
            BindingNodeId[] b;
            try {
                b = queue.poll(TICK_MS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                shutdown();
                return false;
            }
            if (b == null) {
                continue;
            }
            if (b == END) {
                workersRemaining--;
                continue;
            }
            batch = b;
            batchPos = 0;
            return true;
        }
        RuntimeException e = failure.get();
        if (e != null) {
            throw e;
        }
        return false;
    }

    @Override
    public BindingNodeId next() {
        if (!hasNext()) throw new NoSuchElementException();
        return batch[batchPos++];
    }

    private void shutdown() {
        stop.set(true);
        queue.clear(); // unblock producers parked on a full queue
    }

    @Override
    public void close() {
        shutdown();
    }

    @Override
    public void abort() {
        shutdown();
    }
}
