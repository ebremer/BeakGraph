package com.ebremer.beakgraph.hdf5.writers.hugeUltra;

import com.ebremer.beakgraph.huge.RecordSorter;
import com.ebremer.beakgraph.huge.TrackedTask;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.ToLongFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The -method 4 object-record external sorter (used for the term columns,
 * whose records carry RDF nodes and therefore cannot be bit-packed). Same
 * contract as the sequential {@code ExternalSorter}, plus:
 *
 * <ul>
 * <li><b>background spilling</b> - a full buffer is sorted and written by a
 *     worker while ingestion continues into a fresh buffer;</li>
 * <li><b>concurrent intermediate merges</b> - independent run groups collapse
 *     in parallel;</li>
 * <li><b>pluggable run format</b> - the term sorter's format groups
 *     consecutive equal terms as (term, count, rows...), writing each term's
 *     text once per run instead of once per occurrence (Zipfian repeats make
 *     this the single biggest I/O reduction of the build).</li>
 * </ul>
 */
final class ParallelSpillSorter<T> implements RecordSorter<T> {

    private static final Logger logger = LoggerFactory.getLogger(ParallelSpillSorter.class);

    /** Serializes a whole SORTED run and streams it back record by record. */
    interface RunFormat<T> {
        void writeRun(DataOutput out, Object[] sorted, int n) throws IOException;

        /** A fresh (possibly stateful) per-run decoder. */
        RunStream<T> newStream();

        /** Reads the next record; throws {@link EOFException} at run end. */
        interface RunStream<T> {
            T read(DataInput in) throws IOException;
        }
    }

    private final Path workDir;
    private final String tag;
    private final Comparator<? super T> comparator;
    private final RunFormat<T> format;
    private final int batch;
    private final int fanIn;
    private final ExecutorService exec;
    // Optional byte budget: a run also spills when the sizer's estimates of
    // the buffered records reach maxBytes - the record cap alone let a batch
    // of multi-KB literals grow to gigabytes (BG-125). With background
    // spilling two batches can be live per sorter; the callers' default
    // budget accounts for that.
    private final ToLongFunction<? super T> sizer;
    private final long maxBytes;

    // Merge groups of one level run at most this many at a time (BG-133).
    private final int maxConcurrentMerges;

    private Object[] buffer;
    private int fill = 0;
    private long bufferedBytes = 0;
    private long size = 0;
    private final List<Path> runs = new ArrayList<>();
    // Records per run, checked at EOF: a record cut in the middle throws the
    // same EOFException as a clean end (BG-129).
    private final Map<Path, Long> runCounts = new java.util.concurrent.ConcurrentHashMap<>();
    private int runCounter = 0;
    private int runsSpilled = 0;
    private TrackedTask<?> pendingSpill;
    private boolean consumed = false;

    ParallelSpillSorter(Path workDir, String tag, Comparator<? super T> comparator,
                        RunFormat<T> format, int batch, int fanIn, ExecutorService exec) {
        this(workDir, tag, comparator, format, batch, fanIn, exec, null, Long.MAX_VALUE);
    }

    ParallelSpillSorter(Path workDir, String tag, Comparator<? super T> comparator,
                        RunFormat<T> format, int batch, int fanIn, ExecutorService exec,
                        ToLongFunction<? super T> sizer, long maxBytes) {
        this(workDir, tag, comparator, format, batch, fanIn, exec, sizer, maxBytes,
                UltraSorterProvider.defaultMergeConcurrency(fanIn, Runtime.getRuntime().availableProcessors()));
    }

    ParallelSpillSorter(Path workDir, String tag, Comparator<? super T> comparator,
                        RunFormat<T> format, int batch, int fanIn, ExecutorService exec,
                        ToLongFunction<? super T> sizer, long maxBytes, int maxConcurrentMerges) {
        this.workDir = workDir;
        this.tag = tag;
        this.comparator = comparator;
        this.format = format;
        this.batch = batch;
        this.fanIn = Math.max(2, fanIn);
        this.exec = exec;
        this.sizer = sizer;
        this.maxBytes = maxBytes;
        this.maxConcurrentMerges = Math.max(1, maxConcurrentMerges);
        this.buffer = new Object[batch];
    }

    @Override
    public void add(T record) throws IOException {
        if (consumed) {
            throw new IllegalStateException("Sorter '" + tag + "' already consumed");
        }
        buffer[fill++] = record;
        size++;
        if (sizer != null) {
            bufferedBytes += sizer.applyAsLong(record);
        }
        if (fill == batch || bufferedBytes >= maxBytes) {
            spillAsync();
        }
    }

    @Override
    public long size() {
        return size;
    }

    /** Runs written so far by a full buffer (not counting intermediate merges). */
    int runsSpilled() {
        return runsSpilled;
    }

    private void spillAsync() throws IOException {
        awaitSpill();
        final Object[] arr = buffer;
        final int n = fill;
        buffer = new Object[batch];
        fill = 0;
        bufferedBytes = 0;
        runsSpilled++;
        final Path run = workDir.resolve(tag + ".orun" + (runCounter++));
        runs.add(run);
        runCounts.put(run, (long) n);
        pendingSpill = TrackedTask.submit(exec, () -> {
            writeSortedRun(run, arr, n);
            return null;
        });
    }

    @SuppressWarnings("unchecked")
    private void writeSortedRun(Path run, Object[] arr, int n) {
        try {
            Arrays.parallelSort(arr, 0, n, (Comparator<Object>) comparator);
            try (DataOutputStream out = new DataOutputStream(
                    new BufferedOutputStream(Files.newOutputStream(run), 1 << 17))) {
                format.writeRun(out, arr, n);
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

    @Override
    @SuppressWarnings("unchecked")
    public SortedCursor<T> sorted() throws IOException {
        if (consumed) {
            throw new IllegalStateException("Sorter '" + tag + "' already consumed");
        }
        consumed = true;
        awaitSpill();
        if (runs.isEmpty()) {
            final Object[] arr = buffer;
            final int n = fill;
            buffer = null;
            Arrays.parallelSort(arr, 0, n, (Comparator<Object>) comparator);
            return new SortedCursor<>() {
                private int i = 0;

                @Override public boolean hasNext() { return i < n; }

                @Override public T next() {
                    if (i >= n) throw new NoSuchElementException();
                    return (T) arr[i++];
                }

                @Override public void close() {}
            };
        }
        if (fill > 0) {
            final Path run = workDir.resolve(tag + ".orun" + (runCounter++));
            runs.add(run);
            runCounts.put(run, (long) fill);
            writeSortedRun(run, buffer, fill);
        }
        buffer = null;
        // Multi-level merges: independent groups collapse concurrently, at
        // most maxConcurrentMerges at a time. Every running merge holds
        // fanIn + 1 open files and a batch-sized re-buffer of live records,
        // so an unthrottled level (~cores merges at -cores 32, fan-in 128)
        // held ~4000 descriptors and 32 batches of TermRows at once (BG-133).
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
        return new MergeIterator(finalRuns);
    }

    /**
     * Merges the groups of one wave concurrently. On any failure the siblings
     * are abandoned - cancelled, then WAITED for - and every output of the
     * wave is removed before the failure is reported: a merge left running
     * under the workspace deletion kept its file open and outlived write()
     * (BG-134).
     */
    private List<Path> mergeWave(List<List<Path>> wave) throws IOException {
        List<TrackedTask<Path>> tasks = new ArrayList<>(wave.size());
        List<Path> outputs = new ArrayList<>(wave.size());
        for (List<Path> group : wave) {
            final Path merged = workDir.resolve(tag + ".orun" + (runCounter++));
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
        // Intermediate merges re-buffer the stream so the grouped run format
        // can re-group the (now globally consecutive) equal records of the
        // merged run.
        try (MergeIterator mi = new MergeIterator(group);
             DataOutputStream out = new DataOutputStream(
                     new BufferedOutputStream(Files.newOutputStream(merged), 1 << 17))) {
            Object[] chunk = new Object[batch];
            int k = 0;
            long written = 0;
            while (mi.hasNext()) {
                chunk[k++] = mi.next();
                written++;
                if (k == chunk.length) {
                    format.writeRun(out, chunk, k);
                    k = 0;
                    com.ebremer.beakgraph.huge.HugeBuildPipeline.checkCancelled("merging sorter '" + tag + "'");
                }
            }
            if (k > 0) {
                format.writeRun(out, chunk, k);
            }
            runCounts.put(merged, written);
        }
    }

    @Override
    public void close() {
        if (pendingSpill != null) {
            // The run being written is CLOSED before it is deleted (BG-134).
            pendingSpill.abandon();
            pendingSpill = null;
        }
        buffer = null;
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
        final RunFormat.RunStream<T> stream = format.newStream();
        final Long expected;
        long read = 0;
        T head;

        RunReader(Path path) throws IOException {
            this.path = path;
            this.expected = runCounts.get(path);
            this.in = new DataInputStream(new BufferedInputStream(Files.newInputStream(path), 1 << 17));
        }

        boolean advance() throws IOException {
            try {
                head = stream.read(in);
                read++;
                return true;
            } catch (EOFException eof) {
                head = null;
                closeAndDelete();
                if (expected != null && read != expected) {
                    throw new IOException("Spill run " + path + " of sorter '" + tag + "' truncated: read "
                            + read + " of " + expected + " records");
                }
                return false;
            }
        }

        void closeAndDelete() {
            try { in.close(); } catch (IOException ignored) {}
            try { Files.deleteIfExists(path); } catch (IOException ignored) {}
        }
    }

    private final class MergeIterator implements SortedCursor<T> {
        private final PriorityQueue<RunReader> heap;
        private final List<RunReader> readers = new ArrayList<>();

        MergeIterator(List<Path> runPaths) throws IOException {
            this.heap = new PriorityQueue<>(Math.max(2, runPaths.size()),
                    (a, b) -> comparator.compare(a.head, b.head));
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
        public boolean hasNext() {
            return !heap.isEmpty();
        }

        @Override
        public T next() {
            RunReader r = heap.poll();
            if (r == null) {
                throw new NoSuchElementException();
            }
            T result = r.head;
            try {
                if (r.advance()) {
                    heap.add(r);
                }
            } catch (IOException e) {
                close();
                throw new UncheckedIOException("Merge failed for sorter '" + tag + "'", e);
            }
            return result;
        }

        @Override
        public void close() {
            for (RunReader r : readers) {
                r.closeAndDelete();
            }
            heap.clear();
        }
    }
}
