package com.ebremer.beakgraph.hdf5.writers.hugeUltra;

import com.ebremer.beakgraph.hdf5.writers.ultra.ParallelRadixSort;
import com.ebremer.beakgraph.huge.TrackedTask;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The disk-based primitive sorter at the heart of -method 4: records are
 * unsigned keys of up to 126 bits (a {@code lo} word plus an optional
 * {@code hi} word), so a "record" is one or two array slots - no objects, no
 * codec, no comparator. RAM runs radix-sort in parallel
 * ({@link ParallelRadixSort}) on a background worker while ingestion
 * continues (double-buffered), spill runs are fixed-width big-endian binary,
 * intermediate merges of independent run groups execute concurrently, and the
 * final k-way merge streams through buffered fixed-width readers.
 *
 * <p>Everything the huge pipeline sorts by value - dictionary-encoded quads
 * and (row, id) join records - packs losslessly into such keys, which is what
 * removes the object churn that dominates the sequential
 * {@code ExternalSorter} at billions of records.
 */
final class PackedLongSorter implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(PackedLongSorter.class);

    /** Allocation-free cursor over sorted keys: call {@link #advance()}, then read the words. */
    interface KeyCursor extends AutoCloseable {
        boolean advance() throws IOException;

        long hi();

        long lo();

        @Override
        void close();
    }

    private final Path workDir;
    private final String tag;
    private final boolean twoWords;
    private final int totalBits;
    private final int batch;
    private final int fanIn;
    private final ForkJoinPool pool;
    private final ExecutorService exec;
    // Merge groups of one level run at most this many at a time (BG-133).
    private final int maxConcurrentMerges;

    private long[] bufLo;
    private long[] bufHi;
    private int fill = 0;
    private long size = 0;
    private final List<Path> runs = new ArrayList<>();
    // Keys per run, checked at EOF: a key cut in the middle throws the same
    // EOFException as a clean end (BG-129).
    private final Map<Path, Long> runCounts = new java.util.concurrent.ConcurrentHashMap<>();
    private int runCounter = 0;
    private TrackedTask<?> pendingSpill;
    private boolean consumed = false;

    PackedLongSorter(Path workDir, String tag, int totalBits, int batch, int fanIn,
                     ForkJoinPool pool, ExecutorService exec) {
        this(workDir, tag, totalBits, batch, fanIn, pool, exec,
                UltraSorterProvider.defaultMergeConcurrency(fanIn, pool.getParallelism()));
    }

    PackedLongSorter(Path workDir, String tag, int totalBits, int batch, int fanIn,
                     ForkJoinPool pool, ExecutorService exec, int maxConcurrentMerges) {
        if (totalBits < 1 || totalBits > 126) {
            throw new IllegalArgumentException("Key width must be 1..126 bits, got " + totalBits);
        }
        this.workDir = workDir;
        this.tag = tag;
        this.totalBits = totalBits;
        this.twoWords = totalBits > 63;
        this.batch = batch;
        this.fanIn = Math.max(2, fanIn);
        this.pool = pool;
        this.exec = exec;
        this.maxConcurrentMerges = Math.max(1, maxConcurrentMerges);
        this.bufLo = new long[batch];
        this.bufHi = twoWords ? new long[batch] : null;
    }

    void add(long hi, long lo) throws IOException {
        if (consumed) {
            throw new IllegalStateException("Sorter '" + tag + "' already consumed");
        }
        bufLo[fill] = lo;
        if (twoWords) {
            bufHi[fill] = hi;
        }
        fill++;
        size++;
        if (fill == batch) {
            spillAsync();
        }
    }

    long size() {
        return size;
    }

    /** Hands the full buffer to a background sort+write; ingestion continues on fresh arrays. */
    private void spillAsync() throws IOException {
        awaitSpill();
        final long[] l = bufLo;
        final long[] h = bufHi;
        final int n = fill;
        bufLo = new long[batch];
        bufHi = twoWords ? new long[batch] : null;
        fill = 0;
        final Path run = workDir.resolve(tag + ".prun" + (runCounter++));
        runs.add(run);
        runCounts.put(run, (long) n);
        pendingSpill = TrackedTask.submit(exec, () -> {
            writeSortedRun(run, l, h, n);
            return null;
        });
    }

    private void writeSortedRun(Path run, long[] l, long[] h, int n) {
        try {
            long[] lo = (n == l.length) ? l : Arrays.copyOf(l, n);
            long[] hi = (h == null) ? null : ((n == h.length) ? h : Arrays.copyOf(h, n));
            long[][] s = ParallelRadixSort.sort(lo, hi, totalBits, pool);
            try (DataOutputStream out = new DataOutputStream(
                    new BufferedOutputStream(Files.newOutputStream(run), 1 << 17))) {
                long[] sl = s[0], sh = s[1];
                for (int i = 0; i < n; i++) {
                    if (twoWords) {
                        out.writeLong(sh[i]);
                    }
                    out.writeLong(sl[i]);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to spill run for sorter '" + tag + "'", e);
        }
    }

    private void awaitSpill() throws IOException {
        if (pendingSpill == null) {
            return;
        }
        try {
            pendingSpill.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while spilling sorter '" + tag + "'", ex);
        } catch (ExecutionException ex) {
            Throwable c = ex.getCause();
            if (c instanceof UncheckedIOException uio) throw uio.getCause();
            if (c instanceof RuntimeException re) throw re;
            if (c instanceof Error err) throw err;
            throw new IOException("Spill failed for sorter '" + tag + "'", c);
        } finally {
            pendingSpill = null;
        }
    }

    KeyCursor sorted() throws IOException {
        if (consumed) {
            throw new IllegalStateException("Sorter '" + tag + "' already consumed");
        }
        consumed = true;
        awaitSpill();
        if (runs.isEmpty()) {
            // All in RAM: one parallel radix sort, no disk at all.
            long[] lo = Arrays.copyOf(bufLo, fill);
            long[] hi = twoWords ? Arrays.copyOf(bufHi, fill) : null;
            bufLo = null;
            bufHi = null;
            long[][] s = ParallelRadixSort.sort(lo, hi, totalBits, pool);
            final long[] sl = s[0], sh = s[1];
            final int n = fill;
            return new KeyCursor() {
                private int i = -1;

                @Override public boolean advance() { return ++i < n; }

                @Override public long hi() { return sh == null ? 0 : sh[i]; }

                @Override public long lo() { return sl[i]; }

                @Override public void close() {}
            };
        }
        if (fill > 0) {
            final long[] l = bufLo;
            final long[] h = bufHi;
            final int n = fill;
            final Path run = workDir.resolve(tag + ".prun" + (runCounter++));
            runs.add(run);
            runCounts.put(run, (long) n);
            writeSortedRun(run, l, h, n);
        }
        bufLo = null;
        bufHi = null;
        // Multi-level merges: independent groups collapse concurrently, at
        // most maxConcurrentMerges at a time - each running merge holds
        // fanIn + 1 open files (BG-133).
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
        return new MergeCursor(finalRuns);
    }

    /** As {@code ParallelSpillSorter.mergeWave}: concurrent groups, siblings abandoned and outputs removed on failure (BG-134). */
    private List<Path> mergeWave(List<List<Path>> wave) throws IOException {
        List<TrackedTask<Path>> tasks = new ArrayList<>(wave.size());
        List<Path> outputs = new ArrayList<>(wave.size());
        for (List<Path> group : wave) {
            final Path merged = workDir.resolve(tag + ".prun" + (runCounter++));
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
        if (failure instanceof InterruptedException ie) throw new IOException("Interrupted while merging sorter '" + tag + "'", ie);
        if (failure instanceof IOException io) throw io;
        if (failure instanceof UncheckedIOException uio) throw uio.getCause();
        if (failure instanceof RuntimeException re) throw re;
        if (failure instanceof Error err) throw err;
        throw new IOException("Merge failed for sorter '" + tag + "'", failure);
    }

    private void mergeGroup(List<Path> group, Path merged) throws IOException {
        long written = 0;
        try (MergeCursor mc = new MergeCursor(group);
             DataOutputStream out = new DataOutputStream(
                     new BufferedOutputStream(Files.newOutputStream(merged), 1 << 17))) {
            while (mc.advance()) {
                if (twoWords) {
                    out.writeLong(mc.hi());
                }
                out.writeLong(mc.lo());
                if ((++written & 0xFFFF) == 0) {
                    com.ebremer.beakgraph.huge.HugeBuildPipeline.checkCancelled("merging sorter '" + tag + "'");
                }
            }
        }
        runCounts.put(merged, written);
    }

    @Override
    public void close() {
        if (pendingSpill != null) {
            pendingSpill.abandon();   // the run being written is closed before it is deleted (BG-134)
            pendingSpill = null;
        }
        bufLo = null;
        bufHi = null;
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

    private final class RunReader {
        final Path path;
        final DataInputStream in;
        final Long expected;
        long read = 0;
        long hi;
        long lo;

        RunReader(Path path) throws IOException {
            this.path = path;
            this.expected = runCounts.get(path);
            this.in = new DataInputStream(new BufferedInputStream(Files.newInputStream(path), 1 << 17));
        }

        boolean advance() throws IOException {
            try {
                if (twoWords) {
                    hi = in.readLong();
                }
                lo = in.readLong();
                read++;
                return true;
            } catch (EOFException eof) {
                closeAndDelete();
                if (expected != null && read != expected) {
                    throw new IOException("Spill run " + path + " of sorter '" + tag + "' truncated: read "
                            + read + " of " + expected + " keys");
                }
                return false;
            }
        }

        void closeAndDelete() {
            try { in.close(); } catch (IOException ignored) {}
            try { Files.deleteIfExists(path); } catch (IOException ignored) {}
        }
    }

    private final class MergeCursor implements KeyCursor {
        private final PriorityQueue<RunReader> heap;
        private final List<RunReader> readers = new ArrayList<>();
        private long hi;
        private long lo;

        MergeCursor(List<Path> runPaths) throws IOException {
            this.heap = new PriorityQueue<>(Math.max(2, runPaths.size()), (a, b) -> {
                int c = Long.compareUnsigned(a.hi, b.hi);
                return (c != 0) ? c : Long.compareUnsigned(a.lo, b.lo);
            });
            try {
                for (Path p : runPaths) {
                    RunReader r = new RunReader(p);
                    readers.add(r);
                    if (r.advance()) {
                        heap.add(r);
                    }
                }
            } catch (IOException e) {
                close();
                throw e;
            }
        }

        @Override
        public boolean advance() throws IOException {
            RunReader r = heap.poll();
            if (r == null) {
                return false;
            }
            hi = r.hi;
            lo = r.lo;
            if (r.advance()) {
                heap.add(r);
            }
            return true;
        }

        @Override public long hi() { return hi; }

        @Override public long lo() { return lo; }

        @Override
        public void close() {
            for (RunReader r : readers) {
                r.closeAndDelete();
            }
            heap.clear();
        }
    }
}
