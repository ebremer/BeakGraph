package com.ebremer.beakgraph.hdf5.writers.hugeUltra;

import com.ebremer.beakgraph.huge.RecordSorter;
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
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.function.ToLongFunction;

/**
 * The -method 4 object-record external sorter (used for the term columns,
 * whose records carry RDF nodes and therefore cannot be bit-packed). Same
 * contract as the sequential {@code ExternalSorter}, plus:
 *
 * <ul>
 * <li><b>background spilling</b> - a full buffer is sorted and written by a
 *     worker while ingestion continues into a fresh buffer (one spill in
 *     flight; see {@link AbstractSpillingSorter});</li>
 * <li><b>concurrent intermediate merges</b> - independent run groups collapse
 *     in parallel;</li>
 * <li><b>pluggable run format</b> - the term sorter's format groups
 *     consecutive equal terms as (term, count, rows...), writing each term's
 *     text once per run instead of once per occurrence (Zipfian repeats make
 *     this the single biggest I/O reduction of the build).</li>
 * </ul>
 */
final class ParallelSpillSorter<T> extends AbstractSpillingSorter implements RecordSorter<T> {

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

    private final Comparator<? super T> comparator;
    private final RunFormat<T> format;
    private final int batch;
    // Optional byte budget: a run also spills when the sizer's estimates of
    // the buffered records reach maxBytes - the record cap alone let a batch
    // of multi-KB literals grow to gigabytes (BG-125). With background
    // spilling two batches can be live per sorter; the callers' default
    // budget accounts for that.
    private final ToLongFunction<? super T> sizer;
    private final long maxBytes;

    private Object[] buffer;
    private int fill = 0;
    private long bufferedBytes = 0;
    private long size = 0;
    private int runsSpilled = 0;

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
        super(workDir, tag, ".orun", fanIn, exec, maxConcurrentMerges);
        this.comparator = comparator;
        this.format = format;
        this.batch = batch;
        this.sizer = sizer;
        this.maxBytes = maxBytes;
        this.buffer = new Object[batch];
    }

    @Override
    public void add(T record) throws IOException {
        requireNotConsumed();
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
        final Path run = registerRun(n);
        spillInBackground(() -> writeSortedRun(run, arr, n));
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

    @Override
    @SuppressWarnings("unchecked")
    public SortedCursor<T> sorted() throws IOException {
        markConsumed();
        awaitSpill();
        if (!hasRuns()) {
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
            final Path run = registerRun(fill);
            writeSortedRun(run, buffer, fill);
        }
        buffer = null;
        return new MergeIterator(mergeDownToFanIn());
    }

    @Override
    protected void mergeGroup(List<Path> group, Path merged) throws IOException {
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
            recordCount(merged, written);
        }
    }

    @Override
    public void close() {
        buffer = null;
        super.close();
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
            this.expected = expectedCount(path);
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
