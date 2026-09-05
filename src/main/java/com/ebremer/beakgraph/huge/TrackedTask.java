package com.ebremer.beakgraph.huge;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * A background task whose end can be WAITED for even after cancellation.
 * {@code Future.cancel(true)} interrupts a running body but returns at once,
 * and {@code get()} on a cancelled future throws without waiting - so a
 * sorter that cancelled its spill or merge and then deleted the workspace
 * raced a worker still writing into it (BG-134, BG-110), and a build stage
 * cancelled after a sibling's failure kept touching the sorters (BG-213).
 * {@link #abandon} cancels, then returns only when the body is certainly not
 * running: either it never started (the cancel won the claim) or its
 * completion latch fell. An optional completion hook lets a caller learn of
 * the FIRST task to finish among many.
 *
 * @author Erich Bremer
 */
public final class TrackedTask<T> {

    private final AtomicBoolean claimed = new AtomicBoolean();
    private final CountDownLatch done = new CountDownLatch(1);
    private final Future<T> future;

    private TrackedTask(ExecutorService exec, Callable<T> body, Consumer<TrackedTask<T>> onDone) {
        this.future = exec.submit(() -> {
            if (!claimed.compareAndSet(false, true)) {
                finish(onDone);   // abandoned before it started
                return null;
            }
            try {
                return body.call();
            } finally {
                finish(onDone);
            }
        });
    }

    private void finish(Consumer<TrackedTask<T>> onDone) {
        done.countDown();
        if (onDone != null) {
            onDone.accept(this);
        }
    }

    public static <T> TrackedTask<T> submit(ExecutorService exec, Callable<T> body) {
        return new TrackedTask<>(exec, body, null);
    }

    /** As {@link #submit(ExecutorService, Callable)}; {@code onDone} runs on the worker once the body has ended, however it ended. */
    public static <T> TrackedTask<T> submit(ExecutorService exec, Callable<T> body, Consumer<TrackedTask<T>> onDone) {
        return new TrackedTask<>(exec, body, onDone);
    }

    public T get() throws InterruptedException, ExecutionException {
        return future.get();
    }

    public boolean isDone() {
        return future.isDone();
    }

    /** Cancels (interrupting a running body) and waits until it is not running. */
    public void abandon() {
        future.cancel(true);
        if (claimed.compareAndSet(false, true)) {
            return;   // never started, never will
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
