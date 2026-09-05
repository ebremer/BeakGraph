package com.ebremer.beakgraph.hdf5.writers.hugeUltra;

import com.ebremer.beakgraph.core.Futures;
import com.ebremer.beakgraph.huge.TrackedTask;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The spill/merge scaffolding the two -method 4 sorters share (BG-315): the
 * single-consumer guard, the run registry with per-run record counts (for
 * the truncation check at EOF, BG-129), one background spill in flight with
 * {@link #awaitSpill} as the ingestion backpressure, the multi-level fan-in
 * merge whose independent groups run concurrently - at most
 * {@code maxConcurrentMerges} at a time (BG-133) - with siblings abandoned
 * and outputs removed on failure (BG-134), and {@link #close}'s run
 * deletion. {@link PackedLongSorter} (fixed-width primitive keys) and
 * {@link ParallelSpillSorter} (object records under a run format) supply the
 * buffer, the run codec and {@link #mergeGroup}.
 * <p>
 * "Background" is relative: a spill submitted from a pool worker of the
 * SAME ForkJoinPool lands on that worker's queue, so when the pool is
 * saturated by stage tasks the spill effectively runs on the ingesting
 * thread (BG-138). Ingestion is always backpressured on the previous spill.
 */
abstract class AbstractSpillingSorter implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(AbstractSpillingSorter.class);

    protected final Path workDir;
    protected final String tag;
    protected final int fanIn;
    protected final ExecutorService exec;
    /** Merge groups of one level run at most this many at a time (BG-133). */
    protected final int maxConcurrentMerges;
    private final String runSuffix;

    private final List<Path> runs = new ArrayList<>();
    private final Map<Path, Long> runCounts = new ConcurrentHashMap<>();
    private int runCounter = 0;
    private TrackedTask<?> pendingSpill;
    private boolean consumed = false;

    AbstractSpillingSorter(Path workDir, String tag, String runSuffix, int fanIn, ExecutorService exec,
                           int maxConcurrentMerges) {
        this.workDir = workDir;
        this.tag = tag;
        this.runSuffix = runSuffix;
        this.fanIn = Math.max(2, fanIn);
        this.exec = exec;
        this.maxConcurrentMerges = Math.max(1, maxConcurrentMerges);
    }

    protected final void requireNotConsumed() {
        if (consumed) {
            throw new IllegalStateException("Sorter '" + tag + "' already consumed");
        }
    }

    /** {@code sorted()} may be called once. */
    protected final void markConsumed() {
        requireNotConsumed();
        consumed = true;
    }

    /** Registers a run about to be written: its path and the records it will hold. */
    protected final Path registerRun(long records) {
        Path run = workDir.resolve(tag + runSuffix + (runCounter++));
        runs.add(run);
        runCounts.put(run, records);
        return run;
    }

    protected final boolean hasRuns() {
        return !runs.isEmpty();
    }

    /** Records the count of a merge output (or a run whose size is only known after writing). */
    protected final void recordCount(Path run, long records) {
        runCounts.put(run, records);
    }

    /** Records the reader of {@code run} must find, or null when unknown. */
    protected final Long expectedCount(Path run) {
        return runCounts.get(run);
    }

    /**
     * Hands a spill to a worker. Call {@link #awaitSpill()} BEFORE swapping
     * the buffer the body captures: one spill in flight keeps at most two
     * batches live per sorter, which is what the byte budgets assume.
     */
    protected final void spillInBackground(Runnable body) {
        pendingSpill = TrackedTask.submit(exec, () -> {
            body.run();
            return null;
        });
    }

    /** Waits for the spill in flight, if any, and reports its failure. */
    protected final void awaitSpill() throws IOException {
        if (pendingSpill == null) {
            return;
        }
        try {
            pendingSpill.join("spilling sorter '" + tag + "'");
        } finally {
            pendingSpill = null;
        }
    }

    /**
     * Collapses the registered runs level by level until at most {@code fanIn}
     * remain and hands them over (the registry is emptied): the final k-way
     * merge is the caller's cursor.
     */
    protected final List<Path> mergeDownToFanIn() throws IOException {
        while (runs.size() > fanIn) {
            logger.info("Sorter '{}': merging {} runs (fan-in {}, up to {} groups at a time)",
                    tag, runs.size(), fanIn, maxConcurrentMerges);
            List<List<Path>> groups = new ArrayList<>();
            List<Path> next = new ArrayList<>();
            for (int i = 0; i < runs.size(); i += fanIn) {
                List<Path> group = new ArrayList<>(runs.subList(i, Math.min(runs.size(), i + fanIn)));
                if (group.size() == 1) {
                    next.add(group.get(0));
                } else {
                    groups.add(group);
                }
            }
            try {
                for (int w = 0; w < groups.size(); w += maxConcurrentMerges) {
                    next.addAll(mergeWave(groups.subList(w, Math.min(groups.size(), w + maxConcurrentMerges))));
                }
            } catch (IOException | RuntimeException | Error e) {
                // Outputs of the waves that did complete are not in `runs`, so
                // close() would never see them: remove them here.
                for (Path produced : next) {
                    if (!runs.contains(produced)) {
                        try { Files.deleteIfExists(produced); } catch (IOException ignored) { }
                    }
                }
                throw e;
            }
            runs.clear();
            runs.addAll(next);
        }
        List<Path> finalRuns = new ArrayList<>(runs);
        runs.clear();
        return finalRuns;
    }

    /**
     * Merges the groups of one wave concurrently. On any failure the siblings
     * are abandoned - cancelled, then WAITED for - and every output of the
     * wave is removed before the failure is reported: a merge left running
     * under the workspace deletion kept its file open and outlived write()
     * (BG-134). An Error (an OutOfMemoryError in a merge) surfaces unwrapped
     * (BG-140).
     */
    private List<Path> mergeWave(List<List<Path>> wave) throws IOException {
        List<TrackedTask<Path>> tasks = new ArrayList<>(wave.size());
        List<Path> outputs = new ArrayList<>(wave.size());
        for (List<Path> group : wave) {
            final Path merged = workDir.resolve(tag + runSuffix + (runCounter++));
            outputs.add(merged);
            tasks.add(TrackedTask.submit(exec, () -> {
                mergeGroup(group, merged);
                return merged;
            }));
        }
        List<Path> done = new ArrayList<>(wave.size());
        Throwable failure = null;
        for (TrackedTask<Path> t : tasks) {
            try {
                done.add(t.get());
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                failure = ex;
                break;
            } catch (ExecutionException ex) {
                failure = ex.getCause();
                break;
            }
        }
        if (failure == null) {
            return done;
        }
        for (TrackedTask<Path> t : tasks) {
            t.abandon();
        }
        for (Path produced : outputs) {
            try { Files.deleteIfExists(produced); } catch (IOException ignored) { }
        }
        if (failure instanceof InterruptedException ie) {
            throw new IOException("Interrupted while merging sorter '" + tag + "'", ie);
        }
        throw Futures.unwrap(failure, "merging sorter '" + tag + "'");
    }

    /** Merges one group of runs into {@code merged} and records its count with {@link #recordCount}. */
    protected abstract void mergeGroup(List<Path> group, Path merged) throws IOException;

    /** Abandons the spill in flight (closed before it is deleted, BG-134) and removes every registered run. */
    @Override
    public void close() {
        if (pendingSpill != null) {
            pendingSpill.abandon();
            pendingSpill = null;
        }
        for (Path run : runs) {
            try {
                Files.deleteIfExists(run);
            } catch (IOException e) {
                logger.warn("Failed to delete spill run {}", run, e);
            }
        }
        runs.clear();
        runCounts.clear();
    }
}
