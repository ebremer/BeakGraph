package com.ebremer.beakgraph.core;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * The one way the writers wait for a task: {@link #join} unwraps an
 * {@link ExecutionException} into the failure it carries - an
 * {@code IOException} (or the one inside an {@code UncheckedIOException})
 * is rethrown as itself, a {@code RuntimeException} or {@code Error}
 * propagates unchanged, anything else becomes an {@code IOException} naming
 * the work - and an interrupt restores the thread's flag before reporting.
 * The same catch block used to be hand-written at twenty-one call sites,
 * several of them forgetting the {@code Error} case (BG-314, BG-140).
 */
public final class Futures {

    private Futures() {}

    /**
     * @param what the work being waited for, as a gerund phrase ("building
     *             index GSPO"): messages read "Interrupted while <what>" and
     *             "<what> failed"
     */
    public static <T> T join(Future<T> future, String what) throws IOException {
        try {
            return future.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while " + what, ex);
        } catch (ExecutionException ex) {
            throw unwrap(ex.getCause(), what);
        }
    }

    /**
     * The failure a task's cause stands for: returns the {@code IOException}
     * to throw, or throws the {@code RuntimeException} / {@code Error} itself.
     * For callers that collected the cause themselves ({@code throw
     * Futures.unwrap(cause, what)}).
     */
    public static IOException unwrap(Throwable cause, String what) {
        if (cause instanceof IOException io) {
            return io;
        }
        if (cause instanceof UncheckedIOException uio) {
            return uio.getCause();
        }
        if (cause instanceof RuntimeException re) {
            throw re;
        }
        if (cause instanceof Error err) {
            throw err;
        }
        return new IOException(what + " failed", cause);
    }
}
