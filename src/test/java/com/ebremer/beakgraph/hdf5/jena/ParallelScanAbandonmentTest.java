package com.ebremer.beakgraph.hdf5.jena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * BG-60: a scan whose consumer disappears without close()/abort() - a client
 * that reads a few rows and drops its QueryExecution - is stopped by the
 * Cleaner backstop once the scan is garbage-collected. That requires the
 * workers not to keep the scan reachable: they used to run an instance method
 * of the scan, so an abandoned scan was pinned by its own workers forever and
 * the backstop could never fire.
 */
@Timeout(120)
class ParallelScanAbandonmentTest {

    /** A chunk that never ends: the workers fill the queue and park on it. */
    private static Supplier<Iterator<BindingNodeId>> endless() {
        return () -> new Iterator<>() {
            @Override public boolean hasNext() { return true; }
            @Override public BindingNodeId next() { return new BindingNodeId(); }
        };
    }

    /** Starts a scan, reads one row and returns only a weak reference to it. */
    private static WeakReference<ParallelScan> startAndDrop(int chunks) {
        List<Supplier<Iterator<BindingNodeId>>> suppliers = new ArrayList<>();
        for (int i = 0; i < chunks; i++) suppliers.add(endless());
        ParallelScan scan = new ParallelScan(suppliers, new AtomicBoolean());
        assertTrue(scan.hasNext());
        scan.next();
        return new WeakReference<>(scan);
    }

    private static void awaitWorkersDone(long millis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + millis;
        while (ParallelScan.ACTIVE_WORKERS.get() != 0) {
            if (System.currentTimeMillis() > deadline) {
                assertEquals(0, ParallelScan.ACTIVE_WORKERS.get(), "scan workers must stop");
            }
            Thread.sleep(20);
        }
    }

    @Test
    void anAbandonedScanIsCollectedAndItsWorkersStop() throws Exception {
        awaitWorkersDone(15_000); // a clean baseline: nothing else running
        WeakReference<ParallelScan> ref = startAndDrop(4);
        assertTrue(ParallelScan.ACTIVE_WORKERS.get() > 0, "the workers are running (parked on the full queue)");
        // Only the consumer held the scan, and it just dropped it. If a running
        // worker still referenced the scan it could never become unreachable.
        long deadline = System.currentTimeMillis() + 30_000;
        while (ref.get() != null && System.currentTimeMillis() < deadline) {
            System.gc();
            Thread.sleep(50);
        }
        assertNull(ref.get(), "an abandoned scan must become unreachable: its workers must not reference it");
        // ... and the Cleaner then stops the workers.
        awaitWorkersDone(30_000);
    }
}
