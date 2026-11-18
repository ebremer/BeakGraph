package com.ebremer.beakgraph.hdf5.writers.hugeUltra;

import com.ebremer.beakgraph.huge.HugeIO;
import com.ebremer.beakgraph.huge.RangeSorter;
import com.ebremer.beakgraph.huge.RecordSorter;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
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
 *     this the single biggest I/O reduction of the build);</li>
 * <li><b>ranges</b> ({@link RangeSorter}) - the sorted records can be read as
 *     ranges of the order, each merged from the runs by a thread of its own.
 *     A run is written with an index for that: every so many records, where
 *     in the file a record begins and that record. A range is entered in
 *     each run at the entry before its lower bound, and ends in each at the
 *     first record that is not below its upper bound.</li>
 * </ul>
 */
final class ParallelSpillSorter<T> extends AbstractSpillingSorter implements RangeSorter<T> {

    /** Serializes stretches of a SORTED run and streams a run back record by record. */
    interface RunFormat<T> {
        /**
         * Writes {@code sorted[from..to)}. A reader that begins where one
         * call's bytes begin reads the records from there on: no state is
         * carried from one call into the next.
         */
        void writeRun(DataOutput out, Object[] sorted, int from, int to) throws IOException;

        /** A fresh (possibly stateful) per-run decoder. */
        RunStream<T> newStream();

        /**
         * True where two records read from one run are known to be equal in
         * the order without comparing them (the grouped term format's rows of
         * one term); false wherever that is not known.
         */
        default boolean equalAtSight(T a, T b) {
            return false;
        }

        /** Reads the next record; throws {@link EOFException} at run end. */
        interface RunStream<T> {
            T read(DataInput in) throws IOException;
        }
    }

    /** Entries the index of a run is begun for; it is thinned to half of them at twice as many. */
    static final int INDEX_ENTRIES = 1024;

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

    // The indexes of the runs that exist, and what their records are estimated
    // to hold of the heap. Runs are written by the spill in flight and by the
    // merges of a level at once, so both are kept under one lock; past the
    // budget every index gives up every other entry.
    private final Object indexLock = new Object();
    private final Map<Path, RunIndex> indexes = new HashMap<>();
    private long indexBytes = 0;
    private final long indexBudget;

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
        // A quarter of what one batch may hold, for the index records of all
        // the runs together; without a byte budget, 65,536 entries' worth.
        this.indexBudget = (sizer == null || maxBytes == Long.MAX_VALUE)
                ? 64L * (1 << 16)
                : Math.max(1L << 20, maxBytes / 4);
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
    private Comparator<Object> order() {
        return (Comparator<Object>) comparator;
    }

    private void writeSortedRun(Path run, Object[] arr, int n) {
        try {
            Arrays.parallelSort(arr, 0, n, order());
            RunIndex index = new RunIndex(n / INDEX_ENTRIES);
            long length;
            try (RunOutput out = new RunOutput(run)) {
                writeIndexed(out, arr, n, index);
                length = out.position();
            }
            publish(run, index, length);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to spill run for sorter '" + tag + "'", e);
        }
    }

    /**
     * Writes {@code arr[0..n)} to the run a stretch at a time, with an index
     * entry where each stretch begins. A stretch ends where the next record
     * differs from the one before it, so equal records - one group of the
     * grouped format - are never cut by an entry: the bytes are those of one
     * call for the whole array.
     */
    @SuppressWarnings("unchecked")
    private void writeIndexed(RunOutput out, Object[] arr, int n, RunIndex index) throws IOException {
        Comparator<Object> cmp = order();
        int from = 0;
        while (from < n) {
            index.add(out.position(), arr[from], heapOf((T) arr[from]));
            int to = (int) Math.min(n, (long) from + index.stride);
            while (to < n && cmp.compare(arr[to - 1], arr[to]) == 0) {
                to++;
            }
            format.writeRun(out.data, arr, from, to);
            from = to;
        }
    }

    private long heapOf(T record) {
        return (sizer == null) ? 64 : sizer.applyAsLong(record);
    }

    /** Makes a written run's index known, and thins every index while they hold more than the budget. */
    private void publish(Path run, RunIndex index, long length) {
        index.length = length;
        synchronized (indexLock) {
            indexes.put(run, index);
            indexBytes += index.bytes;
            while (indexBytes > indexBudget) {
                long total = 0;
                boolean thinned = false;
                for (RunIndex i : indexes.values()) {
                    if (i.size > 1) {
                        i.thin();
                        thinned = true;
                    }
                    total += i.bytes;
                }
                indexBytes = total;
                if (!thinned) {
                    break;
                }
            }
        }
    }

    /** The index of a run that is gone (merged into another) goes with it. */
    private void forget(Path run) {
        synchronized (indexLock) {
            RunIndex index = indexes.remove(run);
            if (index != null) {
                indexBytes -= index.bytes;
            }
        }
    }

    @Override
    public SortedCursor<T> sorted() throws IOException {
        markConsumed();
        awaitSpill();
        if (!hasRuns()) {
            final Object[] arr = buffer;
            final int n = fill;
            buffer = null;
            Arrays.parallelSort(arr, 0, n, order());
            return new ArrayCursor(arr, 0, n);
        }
        if (fill > 0) {
            final Path run = registerRun(fill);
            writeSortedRun(run, buffer, fill);
        }
        buffer = null;
        return new MergeIterator(mergeDownToFanIn());
    }

    @Override
    public Ranges<T> ranges() throws IOException {
        markConsumed();
        awaitSpill();
        if (!hasRuns()) {
            final Object[] arr = buffer;
            final int n = fill;
            buffer = null;
            Arrays.parallelSort(arr, 0, n, order());
            return new MemoryRanges(arr, n);
        }
        if (fill > 0) {
            final Path run = registerRun(fill);
            writeSortedRun(run, buffer, fill);
        }
        buffer = null;
        // The runs stay this sorter's: every range reads them, close() removes them.
        return new RunRanges(mergeDownKeepingRuns());
    }

    @Override
    protected void mergeGroup(List<Path> group, Path merged) throws IOException {
        // Intermediate merges re-buffer the stream so the grouped run format
        // can re-group the (now globally consecutive) equal records of the
        // merged run.
        RunIndex index = new RunIndex(batch / INDEX_ENTRIES);
        long length;
        try (MergeIterator mi = new MergeIterator(group);
             RunOutput out = new RunOutput(merged)) {
            Object[] chunk = new Object[batch];
            int k = 0;
            long written = 0;
            while (mi.hasNext()) {
                chunk[k++] = mi.next();
                written++;
                if (k == chunk.length) {
                    writeIndexed(out, chunk, k, index);
                    k = 0;
                    com.ebremer.beakgraph.huge.HugeBuildPipeline.checkCancelled("merging sorter '" + tag + "'");
                }
            }
            if (k > 0) {
                writeIndexed(out, chunk, k, index);
            }
            recordCount(merged, written);
            length = out.position();
        }
        publish(merged, index, length);
    }

    @Override
    public void close() {
        buffer = null;
        synchronized (indexLock) {
            indexes.clear();
            indexBytes = 0;
        }
        super.close();
    }

    // ------------------------------------------------------------------
    // runs: their index, and how they are written
    // ------------------------------------------------------------------

    /**
     * Where a run can be entered: for every so many of its records, the
     * offset at which one begins (the start of a {@link RunFormat#writeRun}
     * call) and that record. The entries' records ascend with their offsets,
     * and no record before an entry's offset is above the entry's record.
     */
    static final class RunIndex {
        long[] offsets = new long[16];
        Object[] keys = new Object[16];
        long[] heap = new long[16];
        int size = 0;
        /** Estimated heap of the entries' records. */
        long bytes = 0;
        /** The run's length in bytes, once it is written. */
        long length = 0;
        /** Records from one entry to the next, at least. */
        int stride;

        RunIndex(int stride) {
            this.stride = Math.max(1, stride);
        }

        void add(long offset, Object key, long keyHeap) {
            if (size == 2 * INDEX_ENTRIES) {
                thin();
            }
            if (size == offsets.length) {
                int grown = size * 2;
                offsets = Arrays.copyOf(offsets, grown);
                keys = Arrays.copyOf(keys, grown);
                heap = Arrays.copyOf(heap, grown);
            }
            offsets[size] = offset;
            keys[size] = key;
            heap[size] = keyHeap;
            size++;
            bytes += keyHeap;
        }

        /** Every other entry goes (the first stays): half the heap, stretches twice as long. */
        void thin() {
            int kept = 0;
            long total = 0;
            for (int i = 0; i < size; i += 2) {
                offsets[kept] = offsets[i];
                keys[kept] = keys[i];
                heap[kept] = heap[i];
                total += heap[i];
                kept++;
            }
            Arrays.fill(keys, kept, size, null);
            size = kept;
            bytes = total;
            stride = (stride > Integer.MAX_VALUE / 2) ? Integer.MAX_VALUE : stride * 2;
        }

        /**
         * The offset to enter the run at for the records that are not below
         * {@code from}: that of the entry before the first whose record is
         * not below it. Every record before that offset is below
         * {@code from}; the caller skips the ones after it that are, too.
         */
        long entryBefore(Object from, Comparator<Object> cmp) {
            int lo = 0;
            int hi = size;
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                if (cmp.compare(keys[mid], from) < 0) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            return (lo == 0) ? 0 : offsets[lo - 1];
        }
    }

    /** A run being written, and how many bytes of it there are so far. */
    private static final class RunOutput implements Closeable {
        private final HugeIO.Counted counter;
        final DataOutputStream data;

        RunOutput(Path run) throws IOException {
            this.counter = new HugeIO.Counted(new BufferedOutputStream(Files.newOutputStream(run), 1 << 17));
            this.data = new DataOutputStream(counter);
        }

        long position() {
            return counter.count();
        }

        @Override
        public void close() throws IOException {
            data.close();
        }
    }

    // ------------------------------------------------------------------
    // the whole sorted stream
    // ------------------------------------------------------------------

    /** The records of a sorted array from {@code from} to {@code to}: a sorter that never spilled. */
    private final class ArrayCursor implements SortedCursor<T> {
        private final Object[] arr;
        private final int to;
        private int i;

        ArrayCursor(Object[] arr, int from, int to) {
            this.arr = arr;
            this.i = from;
            this.to = to;
        }

        @Override
        public boolean hasNext() {
            return i < to;
        }

        @Override
        @SuppressWarnings("unchecked")
        public T next() {
            if (i >= to) {
                throw new NoSuchElementException();
            }
            return (T) arr[i++];
        }

        @Override
        public void close() {}
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
            forget(path);
        }
    }

    private final class MergeIterator implements SortedCursor<T> {
        private final PriorityQueue<RunReader> heap;
        private final List<RunReader> readers = new ArrayList<>();
        // The reader whose head is known to equal the record just returned: it
        // is still the smallest, so it stays out of the heap until that ends.
        private RunReader hot;

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
            return hot != null || !heap.isEmpty();
        }

        @Override
        public T next() {
            RunReader r = (hot != null) ? hot : heap.poll();
            if (r == null) {
                throw new NoSuchElementException();
            }
            hot = null;
            T result = r.head;
            try {
                if (r.advance()) {
                    if (format.equalAtSight(r.head, result)) {
                        hot = r;
                    } else {
                        heap.add(r);
                    }
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
            hot = null;
        }
    }

    // ------------------------------------------------------------------
    // the sorted stream as ranges
    // ------------------------------------------------------------------

    /** A sorter that never spilled: its ranges are stretches of the one sorted array. */
    private final class MemoryRanges implements Ranges<T> {
        private final Object[] arr;
        private final int n;

        MemoryRanges(Object[] arr, int n) {
            this.arr = arr;
            this.n = n;
        }

        @Override
        @SuppressWarnings("unchecked")
        public List<Sample<T>> samples() {
            int step = Math.max(1, n / INDEX_ENTRIES);
            List<Sample<T>> samples = new ArrayList<>(n / step + 1);
            for (int i = 0; i < n; i += step) {
                T record = (T) arr[i];
                samples.add(new Sample<>(record, Math.min(step, n - i) * heapOf(record)));
            }
            return samples;
        }

        /** The first position whose record is not below {@code key}. */
        private int lowerBound(T key) {
            Comparator<Object> cmp = order();
            int lo = 0;
            int hi = n;
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                if (cmp.compare(arr[mid], key) < 0) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            return lo;
        }

        @Override
        public SortedCursor<T> open(T from, T to) {
            int lo = (from == null) ? 0 : lowerBound(from);
            int hi = (to == null) ? n : lowerBound(to);
            return new ArrayCursor(arr, lo, Math.max(lo, hi));
        }

        @Override
        public int maxOpen() {
            return Integer.MAX_VALUE;
        }
    }

    /** The final runs, each with its index as it stood when the last of them was written. */
    private final class RunRanges implements Ranges<T> {
        private final List<Path> runs;
        private final List<RunIndex> index;

        RunRanges(List<Path> runs) throws IOException {
            this.runs = runs;
            this.index = new ArrayList<>(runs.size());
            synchronized (indexLock) {
                for (Path run : runs) {
                    RunIndex i = indexes.get(run);
                    if (i == null) {
                        throw new IOException("Spill run " + run + " of sorter '" + tag + "' has no index");
                    }
                    index.add(i);
                }
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public List<Sample<T>> samples() {
            List<Sample<T>> samples = new ArrayList<>();
            for (RunIndex i : index) {
                for (int e = 0; e < i.size; e++) {
                    long end = (e + 1 < i.size) ? i.offsets[e + 1] : i.length;
                    samples.add(new Sample<>((T) i.keys[e], Math.max(1, end - i.offsets[e])));
                }
            }
            return samples;
        }

        @Override
        public SortedCursor<T> open(T from, T to) throws IOException {
            return new RangeCursor(runs, index, from, to);
        }

        /**
         * As many ranges as the merges of a level may hold files for: each
         * open range holds one file per run, a running merge fan-in + 1.
         */
        @Override
        public int maxOpen() {
            long files = (long) maxConcurrentMerges * (fanIn + 1);
            return (int) Math.max(1, Math.min(Integer.MAX_VALUE, files / (runs.size() + 1)));
        }
    }

    /** One run read from where a range begins in it. The run stays: other ranges read it too. */
    private final class RangeReader {
        final DataInputStream in;
        final RunFormat.RunStream<T> stream = format.newStream();
        T head;

        RangeReader(Path path, RunIndex index, T from, int bufferBytes) throws IOException {
            InputStream raw = Files.newInputStream(path);
            try {
                if (from != null) {
                    raw.skipNBytes(index.entryBefore(from, order()));
                }
            } catch (IOException | RuntimeException e) {
                try { raw.close(); } catch (IOException ignored) {}
                throw e;
            }
            this.in = new DataInputStream(new BufferedInputStream(raw, bufferBytes));
            try {
                // The records between the index entry and the range.
                while (advance() && from != null && comparator.compare(head, from) < 0) {
                    // below the range
                }
            } catch (IOException | RuntimeException e) {
                close();
                throw e;
            }
        }

        boolean advance() throws IOException {
            try {
                head = stream.read(in);
                return true;
            } catch (EOFException eof) {
                head = null;
                return false;
            }
        }

        void close() {
            try { in.close(); } catch (IOException ignored) {}
        }
    }

    /** The merge of the runs' records within one range. */
    private final class RangeCursor implements SortedCursor<T> {
        private final PriorityQueue<RangeReader> heap;
        private final List<RangeReader> readers = new ArrayList<>();
        private final T to;
        private RangeReader hot;   // as MergeIterator's

        RangeCursor(List<Path> runPaths, List<RunIndex> index, T from, T to) throws IOException {
            this.to = to;
            this.heap = new PriorityQueue<>(Math.max(2, runPaths.size()),
                    (a, b) -> comparator.compare(a.head, b.head));
            // A range holds a buffer per run: 4 MiB between them, 64 KiB at most each.
            int bufferBytes = Math.max(1 << 13, Math.min(1 << 16, (4 << 20) / Math.max(1, runPaths.size())));
            try {
                for (int i = 0; i < runPaths.size(); i++) {
                    RangeReader r = new RangeReader(runPaths.get(i), index.get(i), from, bufferBytes);
                    readers.add(r);
                    keep(r);
                }
            } catch (IOException | RuntimeException e) {
                close();
                throw e;
            }
        }

        /** Back into the heap while the reader's head is within the range; closed once it is not. */
        private void keep(RangeReader r) {
            if (r.head != null && (to == null || comparator.compare(r.head, to) < 0)) {
                heap.add(r);
            } else {
                r.head = null;
                r.close();
            }
        }

        @Override
        public boolean hasNext() {
            return hot != null || !heap.isEmpty();
        }

        @Override
        public T next() {
            RangeReader r = (hot != null) ? hot : heap.poll();
            if (r == null) {
                throw new NoSuchElementException();
            }
            hot = null;
            T result = r.head;
            try {
                if (r.advance() && format.equalAtSight(r.head, result)) {
                    hot = r;   // equal to a record of the range: within it
                } else {
                    keep(r);
                }
            } catch (IOException e) {
                close();
                throw new UncheckedIOException("Merge failed for sorter '" + tag + "'", e);
            }
            return result;
        }

        @Override
        public void close() {
            for (RangeReader r : readers) {
                r.close();
            }
            heap.clear();
            hot = null;
        }
    }
}
