package com.ebremer.beakgraph.huge;

import com.ebremer.beakgraph.core.Futures;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A piece of work handed to a pool by a thread that will want its result
 * later, in its turn: the chunks of a stream decoded ahead of their reader,
 * the blocks of a dictionary encoded ahead of their writer. {@link #join}
 * runs the body on the calling thread when no worker has begun it, so the
 * thread that waits for the oldest piece does that piece instead of sitting
 * on a pool thread while the piece sits in a queue - which, in a pool every
 * thread of which is such a waiter, would be for good.
 *
 * <p>{@link #abandon} keeps a piece that has not begun from running and
 * waits for one that has: afterwards nothing of it touches a file. A running
 * body is not interrupted - the pieces are short, and an interrupt would
 * close the file channel the others read through.
 */
final class PoolTask<T> implements Runnable {

    private final Callable<T> body;
    private final AtomicBoolean claimed = new AtomicBoolean();
    private final CountDownLatch done = new CountDownLatch(1);
    private T result;
    private Throwable failure;

    private PoolTask(Callable<T> body) {
        this.body = body;
    }

    static <T> PoolTask<T> submit(Executor pool, Callable<T> body) {
        PoolTask<T> task = new PoolTask<>(body);
        pool.execute(task);
        return task;
    }

    /** Runs the body unless it has been begun (or abandoned) already. */
    @Override
    public void run() {
        if (!claimed.compareAndSet(false, true)) {
            return;
        }
        try {
            result = body.call();
        } catch (Throwable t) {
            failure = t;
        } finally {
            done.countDown();
        }
    }

    boolean isDone() {
        return done.getCount() == 0;
    }

    /**
     * The result, the body run here if nobody has begun it; a failure is
     * unwrapped as {@link Futures#join} does ({@code what} is a gerund phrase).
     */
    T join(String what) throws IOException {
        run();
        try {
            done.await();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while " + what, ex);
        }
        if (failure != null) {
            throw Futures.unwrap(failure, what);
        }
        return result;
    }

    /** Never to run if it has not begun; waited for if it has. */
    void abandon() {
        if (claimed.compareAndSet(false, true)) {
            done.countDown();
            return;
        }
        boolean interrupted = false;
        while (true) {
            try {
                done.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
