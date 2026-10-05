package com.ebremer.beakgraph.hdf5.writers.hugeUltra;

import com.ebremer.beakgraph.hdf5.writers.ultra.ParallelRadixSort;
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
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

/**
 * The disk-based primitive sorter at the heart of -method 4: records are
 * unsigned keys of up to 126 bits (a {@code lo} word plus an optional
 * {@code hi} word), so a "record" is one or two array slots - no objects, no
 * codec, no comparator. RAM runs radix-sort in parallel
 * ({@link ParallelRadixSort}) on a background worker while ingestion
 * continues (one spill in flight; see {@link AbstractSpillingSorter}), spill
 * runs are fixed-width big-endian binary, intermediate merges of independent
 * run groups execute concurrently, and the final k-way merge streams through
 * buffered fixed-width readers.
 *
 * <p>Everything the huge pipeline sorts by value - dictionary-encoded quads
 * and (row, id) join records - packs losslessly into such keys, which is what
 * removes the object churn that dominates the sequential
 * {@code ExternalSorter} at billions of records.
 */
final class PackedLongSorter extends AbstractSpillingSorter {

    /** Allocation-free cursor over sorted keys: call {@link #advance()}, then read the words. */
    interface KeyCursor extends AutoCloseable {
        boolean advance() throws IOException;

        long hi();

        long lo();

        @Override
        void close();
    }

    private final boolean twoWords;
    private final int totalBits;
    private final int batch;
    private final ForkJoinPool pool;

    private long[] bufLo;
    private long[] bufHi;
    private int fill = 0;
    private long size = 0;

    PackedLongSorter(Path workDir, String tag, int totalBits, int batch, int fanIn,
                     ForkJoinPool pool, ExecutorService exec) {
        this(workDir, tag, totalBits, batch, fanIn, pool, exec,
                UltraSorterProvider.defaultMergeConcurrency(fanIn, pool.getParallelism()));
    }

    PackedLongSorter(Path workDir, String tag, int totalBits, int batch, int fanIn,
                     ForkJoinPool pool, ExecutorService exec, int maxConcurrentMerges) {
        super(workDir, tag, ".prun", fanIn, exec, maxConcurrentMerges);
        if (totalBits < 1 || totalBits > 126) {
            throw new IllegalArgumentException("Key width must be 1..126 bits, got " + totalBits);
        }
        this.totalBits = totalBits;
        this.twoWords = totalBits > 63;
        this.batch = batch;
        this.pool = pool;
        this.bufLo = new long[batch];
        this.bufHi = twoWords ? new long[batch] : null;
    }

    void add(long hi, long lo) throws IOException {
        requireNotConsumed();
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
        final Path run = registerRun(n);
        spillInBackground(() -> writeSortedRun(run, l, h, n));
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

    KeyCursor sorted() throws IOException {
        markConsumed();
        awaitSpill();
        if (!hasRuns()) {
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
            final Path run = registerRun(fill);
            writeSortedRun(run, bufLo, bufHi, fill);
        }
        bufLo = null;
        bufHi = null;
        return new MergeCursor(mergeDownToFanIn());
    }

    @Override
    protected void mergeGroup(List<Path> group, Path merged) throws IOException {
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
        recordCount(merged, written);
    }

    @Override
    public void close() {
        bufLo = null;
        bufHi = null;
        super.close();
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
            this.expected = expectedCount(path);
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
