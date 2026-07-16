package com.ebremer.beakgraph.huge;

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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * External merge sort over records of type {@code T}: records are buffered in
 * RAM, sorted and spilled as runs to temp files when the buffer fills, and the
 * runs are k-way merged (multi-level when there are more runs than the fan-in)
 * into one sorted stream. This is what lets the huge writer sort node and quad
 * populations far larger than the heap - the RAM writer's
 * {@code Arrays.parallelSort} over all quads is the size ceiling this removes.
 *
 * <p>Single-consumer: {@link #sorted()} may be called once. Duplicate records
 * are preserved (callers dedup while streaming, exactly like the RAM writers
 * dedup after sorting).
 *
 * @author Erich Bremer
 */
public final class ExternalSorter<T> implements RecordSorter<T> {

    private static final Logger logger = LoggerFactory.getLogger(ExternalSorter.class);

    /** Serializes records for spill runs. {@code read} throws {@link EOFException} at run end. */
    public interface Codec<T> {
        void write(DataOutput out, T record) throws IOException;

        T read(DataInput in) throws IOException;
    }

    private final Path workDir;
    private final String tag;
    private final Codec<T> codec;
    private final Comparator<? super T> comparator;
    private final int maxRecordsInMemory;
    private final int mergeFanIn;
    private ArrayList<T> buffer = new ArrayList<>();
    private final List<Path> runs = new ArrayList<>();
    private long size = 0;
    private int runCounter = 0;
    private boolean consumed = false;

    public ExternalSorter(Path workDir, String tag, Codec<T> codec, Comparator<? super T> comparator,
                          int maxRecordsInMemory, int mergeFanIn) {
        if (maxRecordsInMemory < 1 || mergeFanIn < 2) {
            throw new IllegalArgumentException("maxRecordsInMemory >= 1 and mergeFanIn >= 2 required");
        }
        this.workDir = workDir;
        this.tag = tag;
        this.codec = codec;
        this.comparator = comparator;
        this.maxRecordsInMemory = maxRecordsInMemory;
        this.mergeFanIn = mergeFanIn;
    }

    @Override
    public void add(T record) throws IOException {
        if (consumed) {
            throw new IllegalStateException("Sorter '" + tag + "' already consumed");
        }
        buffer.add(record);
        size++;
        if (buffer.size() >= maxRecordsInMemory) {
            spillRun();
        }
    }

    /** Total records added. */
    @Override
    public long size() {
        return size;
    }

    @SuppressWarnings("unchecked")
    private T[] sortedBufferArray() {
        Object[] arr = buffer.toArray();
        // parallelSort: the comparator (NodeComparator for term records) is the
        // dominant cost of a run; use the cores exactly like the RAM writer does.
        Arrays.parallelSort(arr, (Comparator<Object>) comparator);
        return (T[]) arr;
    }

    private void spillRun() throws IOException {
        if (buffer.isEmpty()) return;
        T[] arr = sortedBufferArray();
        Path run = workDir.resolve(tag + ".run" + (runCounter++));
        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(run), 1 << 16))) {
            for (T rec : arr) {
                codec.write(out, rec);
            }
        }
        runs.add(run);
        logger.debug("Sorter '{}': spilled run {} ({} records)", tag, run.getFileName(), arr.length);
        buffer = new ArrayList<>();
    }

    /**
     * Finishes ingestion and returns the fully sorted stream. All-in-RAM inputs
     * (no spilled run) sort and iterate without touching disk.
     */
    @Override
    public RecordSorter.SortedCursor<T> sorted() throws IOException {
        if (consumed) {
            throw new IllegalStateException("Sorter '" + tag + "' already consumed");
        }
        consumed = true;
        if (runs.isEmpty()) {
            T[] arr = sortedBufferArray();
            buffer = new ArrayList<>();
            Iterator<T> it = Arrays.asList(arr).iterator();
            return new RecordSorter.SortedCursor<T>() {
                @Override public boolean hasNext() { return it.hasNext(); }
                @Override public T next() { return it.next(); }
                @Override public void close() {}
            };
        }
        spillRun();
        // Multi-level merge: collapse groups of fan-in runs until one merge pass
        // can stream them all.
        while (runs.size() > mergeFanIn) {
            List<Path> group = new ArrayList<>(runs.subList(0, mergeFanIn));
            runs.subList(0, mergeFanIn).clear();
            Path merged = workDir.resolve(tag + ".run" + (runCounter++));
            logger.debug("Sorter '{}': intermediate merge of {} runs", tag, group.size());
            try (MergeIterator mergeIt = new MergeIterator(group);
                 DataOutputStream out = new DataOutputStream(
                         new BufferedOutputStream(Files.newOutputStream(merged), 1 << 16))) {
                while (mergeIt.hasNext()) {
                    codec.write(out, mergeIt.next());
                }
            }
            runs.add(merged);
        }
        List<Path> finalRuns = new ArrayList<>(runs);
        runs.clear();
        return new MergeIterator(finalRuns);
    }

    @Override
    public void close() {
        buffer = new ArrayList<>();
        for (Path run : runs) {
            try {
                Files.deleteIfExists(run);
            } catch (IOException e) {
                logger.warn("Failed to delete spill run {}", run, e);
            }
        }
        runs.clear();
    }

    private final class RunReader {
        final Path path;
        final DataInputStream in;
        T head;

        RunReader(Path path) throws IOException {
            this.path = path;
            this.in = new DataInputStream(new BufferedInputStream(Files.newInputStream(path), 1 << 16));
        }

        /** Loads the next record into {@code head}; false (and cleanup) at run end. */
        boolean advance() throws IOException {
            try {
                head = codec.read(in);
                return true;
            } catch (EOFException eof) {
                head = null;
                closeAndDelete();
                return false;
            }
        }

        void closeAndDelete() {
            try { in.close(); } catch (IOException ignored) {}
            try { Files.deleteIfExists(path); } catch (IOException ignored) {}
        }
    }

    private final class MergeIterator implements RecordSorter.SortedCursor<T> {
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
