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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
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

    private Object[] buffer;
    private int fill = 0;
    private long size = 0;
    private final List<Path> runs = new ArrayList<>();
    private int runCounter = 0;
    private Future<?> pendingSpill;
    private boolean consumed = false;

    ParallelSpillSorter(Path workDir, String tag, Comparator<? super T> comparator,
                        RunFormat<T> format, int batch, int fanIn, ExecutorService exec) {
        this.workDir = workDir;
        this.tag = tag;
        this.comparator = comparator;
        this.format = format;
        this.batch = batch;
        this.fanIn = Math.max(2, fanIn);
        this.exec = exec;
        this.buffer = new Object[batch];
    }

    @Override
    public void add(T record) throws IOException {
        if (consumed) {
            throw new IllegalStateException("Sorter '" + tag + "' already consumed");
        }
        buffer[fill++] = record;
        size++;
        if (fill == batch) {
            spillAsync();
        }
    }

    @Override
    public long size() {
        return size;
    }

    private void spillAsync() throws IOException {
        awaitSpill();
        final Object[] arr = buffer;
        final int n = fill;
        buffer = new Object[batch];
        fill = 0;
        final Path run = workDir.resolve(tag + ".orun" + (runCounter++));
        runs.add(run);
        pendingSpill = exec.submit(() -> {
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
            writeSortedRun(run, buffer, fill);
        }
        buffer = null;
        while (runs.size() > fanIn) {
            logger.info("Sorter '{}': merging {} runs (fan-in {}, groups in parallel)", tag, runs.size(), fanIn);
            List<Path> next = new ArrayList<>();
            List<Future<Path>> merging = new ArrayList<>();
            for (int i = 0; i < runs.size(); i += fanIn) {
                final List<Path> group = new ArrayList<>(runs.subList(i, Math.min(runs.size(), i + fanIn)));
                if (group.size() == 1) {
                    next.add(group.get(0));
                    continue;
                }
                final Path merged = workDir.resolve(tag + ".orun" + (runCounter++));
                merging.add(exec.submit(() -> {
                    // Intermediate merges re-buffer the stream so the grouped
                    // run format can re-group the (now globally consecutive)
                    // equal records of the merged run.
                    try (MergeIterator mi = new MergeIterator(group);
                         DataOutputStream out = new DataOutputStream(
                                 new BufferedOutputStream(Files.newOutputStream(merged), 1 << 17))) {
                        Object[] chunk = new Object[batch];
                        int k = 0;
                        while (mi.hasNext()) {
                            chunk[k++] = mi.next();
                            if (k == chunk.length) {
                                format.writeRun(out, chunk, k);
                                k = 0;
                            }
                        }
                        if (k > 0) {
                            format.writeRun(out, chunk, k);
                        }
                    }
                    return merged;
                }));
            }
            for (Future<Path> f : merging) {
                try {
                    next.add(f.get());
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while merging sorter '" + tag + "'", ex);
                } catch (ExecutionException ex) {
                    Throwable c = ex.getCause();
                    if (c instanceof IOException io) throw io;
                    if (c instanceof UncheckedIOException uio) throw uio.getCause();
                    if (c instanceof RuntimeException re) throw re;
                    throw new IOException("Merge failed for sorter '" + tag + "'", c);
                }
            }
            runs.clear();
            runs.addAll(next);
        }
        List<Path> finalRuns = new ArrayList<>(runs);
        runs.clear();
        return new MergeIterator(finalRuns);
    }

    @Override
    public void close() {
        buffer = null;
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
        final RunFormat.RunStream<T> stream = format.newStream();
        T head;

        RunReader(Path path) throws IOException {
            this.path = path;
            this.in = new DataInputStream(new BufferedInputStream(Files.newInputStream(path), 1 << 17));
        }

        boolean advance() throws IOException {
            try {
                head = stream.read(in);
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
