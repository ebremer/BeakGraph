package com.ebremer.beakgraph.hdf5.jena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for BG-207 and BG-57: a chunk worker that dies with an
 * {@link Error} (OutOfMemoryError, AssertionError) used to slip past the
 * worker's catch, still count itself out with END, and let the consumer finish
 * normally with that chunk's rows silently missing; and a consumer that saw
 * {@code stop} between a failing worker's two writes ended the scan as if it
 * had been closed under it. A scan with a failing chunk must never complete
 * normally, whatever the failure type and whenever it happens.
 */
class ParallelScanFailureTest {

    private static Supplier<Iterator<BindingNodeId>> rows(int count) {
        return () -> new Iterator<>() {
            int i = 0;
            @Override public boolean hasNext() { return i < count; }
            @Override public BindingNodeId next() { i++; return new BindingNodeId(); }
        };
    }

    /** Yields {@code before} rows, then throws {@code failure} from hasNext(). */
    private static Supplier<Iterator<BindingNodeId>> failing(int before, Throwable failure) {
        return () -> new Iterator<>() {
            int i = 0;
            @Override public boolean hasNext() {
                if (i >= before) {
                    if (failure instanceof RuntimeException r) throw r;
                    throw (Error) failure;
                }
                return true;
            }
            @Override public BindingNodeId next() { i++; return new BindingNodeId(); }
        };
    }

    private static int drain(ParallelScan scan) {
        int n = 0;
        while (scan.hasNext()) {
            scan.next();
            n++;
        }
        return n;
    }

    @AfterEach
    void workersMustDrain() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (ParallelScan.ACTIVE_WORKERS.get() != 0) {
            if (System.currentTimeMillis() > deadline) {
                assertEquals(0, ParallelScan.ACTIVE_WORKERS.get(), "scan workers must stop after a failure");
            }
            Thread.sleep(10);
        }
    }

    @Test
    void errorInAChunkPropagatesInsteadOfTruncating() {
        AssertionError boom = new AssertionError("boom");
        ParallelScan scan = new ParallelScan(List.of(rows(5_000), failing(100, boom), rows(5_000)), new AtomicBoolean());
        AssertionError thrown = assertThrows(AssertionError.class, () -> drain(scan),
                "an Error in a worker must reach the consumer, not end the scan with rows missing");
        assertSame(boom, thrown);
    }

    @Test
    void errorWhileOpeningAChunkPropagates() {
        OutOfMemoryError oom = new OutOfMemoryError("simulated");
        Supplier<Iterator<BindingNodeId>> deadOnArrival = () -> { throw oom; };
        ParallelScan scan = new ParallelScan(List.of(rows(1_000), deadOnArrival), new AtomicBoolean());
        assertSame(oom, assertThrows(OutOfMemoryError.class, () -> drain(scan)));
    }

    @Test
    void runtimeExceptionStillPropagates() {
        IllegalStateException ise = new IllegalStateException("corrupt index");
        ParallelScan scan = new ParallelScan(List.of(rows(1_000), failing(10, ise), rows(1_000)), new AtomicBoolean());
        assertSame(ise, assertThrows(IllegalStateException.class, () -> drain(scan)));
    }

    @Test
    void aFailingChunkNeverLetsTheScanFinishNormally() {
        // Exercises the stop-before-failure window many times over: whether the
        // failure lands before, during or after the healthy chunks' rows, the
        // consumer must always see it.
        for (int round = 0; round < 300; round++) {
            RuntimeException fail = new RuntimeException("round " + round);
            int before = round % 7 == 0 ? 0 : (round % 50);
            ParallelScan scan = new ParallelScan(
                    List.of(rows(2_000), failing(before, fail), rows(200), failing(before + 1, fail)),
                    new AtomicBoolean());
            boolean threw = false;
            try {
                drain(scan);
            } catch (RuntimeException e) {
                threw = true;
                assertSame(fail, e);
            }
            assertTrue(threw, "round " + round + ": scan completed normally despite a failing chunk");
        }
    }

    /**
     * BG-217 (d): the engine's cancel signal flips while every worker is parked
     * on a full queue. The consumer must see QueryCancelledException on its
     * next hasNext() and the workers must exit within a few ticks.
     */
    @Test
    void cancelSignalWhileWorkersAreParkedOnAFullQueue() throws Exception {
        AtomicBoolean cancel = new AtomicBoolean();
        List<Supplier<Iterator<BindingNodeId>>> chunks = new java.util.ArrayList<>();
        for (int i = 0; i < 8; i++) chunks.add(rows(256 * 64));   // 64 batches each: far more than the queue holds
        ParallelScan scan = new ParallelScan(chunks, cancel);
        assertTrue(scan.hasNext());
        scan.next();
        Thread.sleep(200);   // workers fill the queue and park on it
        assertTrue(ParallelScan.ACTIVE_WORKERS.get() > 0, "workers must be running");
        cancel.set(true);
        assertThrows(org.apache.jena.query.QueryCancelledException.class, () -> {
            while (scan.hasNext()) scan.next();
        }, "a cancelled scan must not complete normally");
        long deadline = System.currentTimeMillis() + 5_000;
        while (ParallelScan.ACTIVE_WORKERS.get() != 0 && System.currentTimeMillis() < deadline) Thread.sleep(10);
        assertEquals(0, ParallelScan.ACTIVE_WORKERS.get(), "parked workers must notice the cancel within a few ticks");
        assertFalse(scan.hasNext(), "after the cancel the scan stays ended");
    }

    @Test
    void closingUnderTheConsumerStillEndsCleanly() {
        ParallelScan scan = new ParallelScan(List.of(rows(100_000), rows(100_000)), new AtomicBoolean());
        for (int i = 0; i < 5; i++) {
            assertTrue(scan.hasNext());
            scan.next();
        }
        scan.close();   // LIMIT-style early close: no failure, no exception
        // The batch already handed to the consumer may still drain; after that
        // the "closed under us" branch ends the scan quietly.
        int residual = drain(scan);
        assertTrue(residual < 256, "only the in-hand batch may follow a close, got " + residual);
        assertFalse(scan.hasNext());
    }
}
