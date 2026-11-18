package com.ebremer.beakgraph.huge;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * The parts of one sorted stream, written a range of the order at a time -
 * each by the thread that merged its range - and read as one: the parts in
 * their order hold the stream in its order.
 */
final class RecordSegments<T> {

    private final List<RecordFile<T>> parts;

    /** A chunk being decoded ahead; the reader of its file is closed with the file's last chunk. */
    private record Pending<R>(PoolTask<List<R>> task, RecordFile<R>.ChunkReader reader, boolean last) {}

    RecordSegments(List<RecordFile<T>> parts) {
        this.parts = List.copyOf(parts);
    }

    /** The parts, in the order of the stream. */
    List<RecordFile<T>> parts() {
        return parts;
    }

    /** Records in all the parts together. */
    long count() {
        long n = 0;
        for (RecordFile<T> part : parts) {
            n += part.count();
        }
        return n;
    }

    /** A fresh stream over every record, one part after another; close it when done. */
    RecordSorter.SortedCursor<T> read() {
        return new RecordSorter.SortedCursor<>() {
            private int next = 0;
            private RecordSorter.SortedCursor<T> part = null;

            private boolean advance() {
                while (part == null || !part.hasNext()) {
                    if (part != null) {
                        part.close();
                        part = null;
                    }
                    if (next == parts.size()) {
                        return false;
                    }
                    try {
                        part = parts.get(next++).read();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
                return true;
            }

            @Override
            public boolean hasNext() {
                return advance();
            }

            @Override
            public T next() {
                if (!advance()) {
                    throw new NoSuchElementException();
                }
                return part.next();
            }

            @Override
            public void close() {
                if (part != null) {
                    part.close();
                    part = null;
                }
                next = parts.size();
            }
        };
    }

    /**
     * As {@link #read}, with the records decoded ahead of their reader: the
     * chunks of the parts are read and decoded by tasks on {@code pool}, up
     * to {@code window} of them at a time, and {@code prepare} (if any) is
     * applied to every record on the thread that decoded it. The reader gets
     * the records in the order of the stream; a chunk no worker has begun
     * when the reader comes to it is decoded by the reader. The heap holds
     * the records of {@code window} chunks at most
     * ({@link RecordFile#CHUNK_RECORDS}, {@link RecordFile#CHUNK_BYTES}).
     */
    RecordSorter.SortedCursor<T> readAhead(java.util.concurrent.Executor pool, int window,
                                           java.util.function.Consumer<? super T> prepare) {
        final int ahead = Math.max(1, window);
        return new RecordSorter.SortedCursor<>() {
            private final java.util.ArrayDeque<Pending<T>> inFlight = new java.util.ArrayDeque<>();
            private final List<RecordFile<T>.ChunkReader> readers = new java.util.ArrayList<>();
            private int part = 0;
            private int chunk = 0;
            private RecordFile<T>.ChunkReader reader = null;
            private java.util.Iterator<T> current = java.util.Collections.emptyIterator();
            private boolean closed = false;

            /** Hands the next chunk of the stream to the pool; false when there is none left. */
            private boolean submit() throws IOException {
                while (part < parts.size()) {
                    RecordFile<T> file = parts.get(part);
                    int chunks = file.chunks();
                    if (chunk < chunks) {
                        if (reader == null) {
                            reader = file.chunkReader();
                            readers.add(reader);
                        }
                        final RecordFile<T>.ChunkReader from = reader;
                        final int which = chunk++;
                        inFlight.add(new Pending<>(PoolTask.submit(pool, () -> {
                            List<T> records = from.read(which);
                            if (prepare != null) {
                                for (T record : records) {
                                    prepare.accept(record);
                                }
                            }
                            return records;
                        }), from, chunk == chunks));
                        return true;
                    }
                    part++;
                    chunk = 0;
                    reader = null;
                }
                return false;
            }

            private boolean advance() {
                try {
                    while (!current.hasNext()) {
                        if (closed) {
                            return false;
                        }
                        while (inFlight.size() < ahead && submit()) {
                            // the window is kept full
                        }
                        Pending<T> oldest = inFlight.poll();
                        if (oldest == null) {
                            return false;
                        }
                        List<T> records = oldest.task().join("reading the sorted terms");
                        if (oldest.last()) {
                            readers.remove(oldest.reader());
                            oldest.reader().close();
                        }
                        current = records.iterator();
                    }
                    return true;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public boolean hasNext() {
                return advance();
            }

            @Override
            public T next() {
                if (!advance()) {
                    throw new NoSuchElementException();
                }
                return current.next();
            }

            /** Chunks not begun never run, running ones are waited for; then the files are closed. */
            @Override
            public void close() {
                closed = true;
                for (Pending<T> pending : inFlight) {
                    pending.task().abandon();
                }
                inFlight.clear();
                for (RecordFile<T>.ChunkReader open : readers) {
                    open.close();
                }
                readers.clear();
                current = java.util.Collections.emptyIterator();
            }
        };
    }

    void delete() {
        for (RecordFile<T> part : parts) {
            part.delete();
        }
    }
}
