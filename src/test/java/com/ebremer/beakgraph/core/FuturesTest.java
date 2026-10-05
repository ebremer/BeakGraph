package com.ebremer.beakgraph.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

/**
 * BG-314 / BG-140: the one task-join for the writers. An IOException (bare or
 * inside an UncheckedIOException) comes back as itself, a RuntimeException or
 * Error propagates unchanged - an OutOfMemoryError in a spill or merge must
 * never be wrapped as an IOException - anything else names the work, and an
 * interrupt restores the flag.
 */
class FuturesTest {

    private static <T> Future<T> failed(Throwable t) {
        CompletableFuture<T> f = new CompletableFuture<>();
        f.completeExceptionally(t);
        return f;
    }

    @Test
    void ioFailuresComeBackAsThemselves() {
        IOException io = new IOException("disk full");
        assertSame(io, assertThrows(IOException.class, () -> Futures.join(failed(io), "spilling")));
        assertSame(io, assertThrows(IOException.class, () -> Futures.join(failed(new UncheckedIOException("w", io)), "spilling")));
    }

    @Test
    void runtimeExceptionsAndErrorsPropagateUnchanged() {
        IllegalStateException ise = new IllegalStateException("corrupt");
        assertSame(ise, assertThrows(IllegalStateException.class, () -> Futures.join(failed(ise), "merging")));
        OutOfMemoryError oom = new OutOfMemoryError("simulated");
        assertSame(oom, assertThrows(OutOfMemoryError.class, () -> Futures.join(failed(oom), "merging")));
        assertSame(oom, assertThrows(OutOfMemoryError.class, () -> Futures.unwrap(oom, "merging")));
    }

    @Test
    void anythingElseNamesTheWork() throws Exception {
        Exception odd = new Exception("checked and odd");
        IOException ex = assertThrows(IOException.class, () -> Futures.join(failed(odd), "building index GSPO"));
        assertEquals("building index GSPO failed", ex.getMessage());
        assertSame(odd, ex.getCause());
        assertEquals("done", Futures.join(CompletableFuture.completedFuture("done"), "anything"));
    }

    @Test
    void anInterruptIsReportedAndTheFlagRestored() throws Exception {
        Future<String> never = new CompletableFuture<>();
        Thread.currentThread().interrupt();
        try {
            IOException ex = assertThrows(IOException.class, () -> Futures.join(never, "sorting"));
            assertEquals("Interrupted while sorting", ex.getMessage());
            assertTrue(Thread.currentThread().isInterrupted(), "the interrupt flag is restored");
        } finally {
            Thread.interrupted();   // clear it for the next test
        }
    }
}
