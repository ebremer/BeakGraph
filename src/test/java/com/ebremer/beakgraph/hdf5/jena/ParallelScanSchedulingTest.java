package com.ebremer.beakgraph.hdf5.jena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Regression tests for BG-206: chunk workers were submitted eagerly to a fixed
 * platform-thread pool and parked in it while their queue was full. The LEFT
 * operand of MINUS or a hash join is built first and consumed last, so with
 * more chunks than pool threads it filled its queue, parked on every thread,
 * and the RIGHT operand's workers never ran - the query hung until its
 * timeout and every other parallel scan in the JVM starved with it.
 */
@Timeout(90)   // a scheduling regression deadlocks; fail the test, never hang the build
class ParallelScanSchedulingTest {

    private static Supplier<Iterator<BindingNodeId>> rows(int count) {
        return () -> new Iterator<>() {
            int i = 0;
            @Override public boolean hasNext() { return i < count; }
            @Override public BindingNodeId next() { i++; return new BindingNodeId(); }
        };
    }

    private static List<Supplier<Iterator<BindingNodeId>>> chunks(int n, int rowsEach) {
        List<Supplier<Iterator<BindingNodeId>>> out = new ArrayList<>();
        for (int i = 0; i < n; i++) out.add(rows(rowsEach));
        return out;
    }

    private static int drain(ParallelScan scan) {
        int n = 0;
        while (scan.hasNext()) { scan.next(); n++; }
        return n;
    }

    /** BG-64: a chunk whose setup finishes after the scan was closed must not start iterating. */
    @Test
    void chunkSetupThatOutlivesTheScanIsNotIterated() throws Exception {
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        AtomicInteger constructed = new AtomicInteger();
        AtomicInteger iterated = new AtomicInteger();
        List<Supplier<Iterator<BindingNodeId>>> chunks = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            chunks.add(() -> {
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                constructed.incrementAndGet();
                Iterator<BindingNodeId> rows = rows(1000).get();
                return new Iterator<>() {
                    @Override public boolean hasNext() { iterated.incrementAndGet(); return rows.hasNext(); }
                    @Override public BindingNodeId next() { return rows.next(); }
                };
            });
        }
        ParallelScan scan = new ParallelScan(chunks, new AtomicBoolean());
        Thread consumer = new Thread(() -> scan.hasNext(), "consumer");
        consumer.start();
        Thread.sleep(100);          // the workers are inside chunk.get(), parked on the latch
        scan.close();               // ... and the scan is closed under them
        release.countDown();
        consumer.join(15_000);
        assertFalse(consumer.isAlive(), "the consumer returns once the scan is closed");
        long deadline = System.currentTimeMillis() + 10_000;
        while (constructed.get() < 8 && System.currentTimeMillis() < deadline) Thread.sleep(10);
        assertEquals(8, constructed.get(), "every worker finishes its setup");
        assertEquals(0, iterated.get(), "no worker iterates a chunk of a closed scan");
    }

    @AfterEach
    void workersMustDrain() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (ParallelScan.ACTIVE_WORKERS.get() != 0) {
            if (System.currentTimeMillis() > deadline) {
                assertEquals(0, ParallelScan.ACTIVE_WORKERS.get(), "scan workers must stop");
            }
            Thread.sleep(10);
        }
    }

    @Test
    void aScanThatIsNotConsumedHoldsNoWorkers() throws Exception {
        ParallelScan left = new ParallelScan(chunks(16, 100_000), new AtomicBoolean());
        Thread.sleep(150);
        assertEquals(0, ParallelScan.ACTIVE_WORKERS.get(), "workers must not start before the first hasNext()");
        left.close();
        assertFalse(left.hasNext(), "a scan closed before use ends without ever starting");
        assertEquals(0, ParallelScan.ACTIVE_WORKERS.get());
    }

    @Test
    void rightOperandDrainsWhileLeftScanIsParkedOnAFullQueue() throws Exception {
        int chunkCount = 2 * Runtime.getRuntime().availableProcessors() + 8;   // more than any fixed pool
        int rowsPerChunk = 256 * (2 * chunkCount + 4);                          // more batches than the queue holds
        ParallelScan left = new ParallelScan(chunks(chunkCount, rowsPerChunk), new AtomicBoolean());
        assertTrue(left.hasNext());          // start the left: its workers fill the queue and park
        Thread.sleep(200);

        ParallelScan right = new ParallelScan(chunks(chunkCount, 1_000), new AtomicBoolean());
        AtomicInteger drained = new AtomicInteger(-1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread consumer = new Thread(() -> {
            try { drained.set(drain(right)); } catch (Throwable t) { failure.set(t); }
        }, "right-consumer");
        consumer.start();
        consumer.join(20_000);
        boolean hung = consumer.isAlive();
        left.close();
        right.close();
        if (hung) {
            consumer.join(5_000);
            throw new AssertionError("the right scan could not make progress while the left scan was parked: deadlock");
        }
        if (failure.get() != null) throw new AssertionError(failure.get());
        assertEquals(chunkCount * 1_000, drained.get(), "the right operand must be fully drained");
        drain(left);                         // closed: only the in-hand batch remains
    }

    @Test
    void manyConcurrentScansAllComplete() throws Exception {
        int scans = 4 * Runtime.getRuntime().availableProcessors();
        List<ParallelScan> all = new ArrayList<>();
        try {
            for (int i = 0; i < scans; i++) {
                ParallelScan s = new ParallelScan(chunks(8, 3_000), new AtomicBoolean());
                assertTrue(s.hasNext());     // every scan has workers parked or running at once
                all.add(s);
            }
            for (ParallelScan s : all) {
                assertEquals(8 * 3_000, drain(s), "each scan yields all of its rows");
            }
        } finally {
            for (ParallelScan s : all) s.close();
        }
    }
}
