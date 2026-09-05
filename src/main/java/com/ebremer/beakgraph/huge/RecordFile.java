package com.ebremer.beakgraph.huge;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NoSuchElementException;

/**
 * A flat temp file of codec-encoded records that is written once and can be
 * streamed any number of times. The huge writer materializes each sorted
 * column / dictionary stream this way because several later phases re-read it
 * (dedup count, dictionary encode, id join) - an {@link ExternalSorter} stream
 * is single-shot.
 *
 * @author Erich Bremer
 */
final class RecordFile<T> implements AutoCloseable {

    private final Path path;
    private final ExternalSorter.Codec<T> codec;
    private DataOutputStream out;
    private long count = 0;
    private boolean finished = false;

    RecordFile(Path path, ExternalSorter.Codec<T> codec) {
        this.path = path;
        this.codec = codec;
    }

    void append(T record) throws IOException {
        if (finished) {
            throw new IllegalStateException("RecordFile already finished: " + path);
        }
        if (out == null) {
            out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path), 1 << 16));
        }
        codec.write(out, record);
        count++;
    }

    void finish() throws IOException {
        if (finished) return;
        finished = true;
        if (out != null) {
            out.close();
            out = null;
        } else {
            // Zero records: create the empty file so readers see a valid stream.
            Files.deleteIfExists(path);
            Files.createFile(path);
        }
    }

    long count() {
        return count;
    }

    /** A fresh sequential stream over all records; close it when done. */
    RecordSorter.SortedCursor<T> read() throws IOException {
        if (!finished) {
            throw new IllegalStateException("RecordFile not finished: " + path);
        }
        DataInputStream in = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(path), 1 << 16));
        final long expected = count;
        return new RecordSorter.SortedCursor<T>() {
            private long read = 0;
            private T head = advance();
            private boolean closed = false;

            private T advance() {
                try {
                    T record = codec.read(in);
                    read++;
                    return record;
                } catch (EOFException eof) {
                    closeStream();
                    // Every codec reads multi-byte records, and DataInput throws
                    // the same EOFException for a record cut in the middle as
                    // for a clean end: only the count tells them apart (BG-129).
                    if (read != expected) {
                        throw new UncheckedIOException(new IOException("Record file " + path + " truncated: read "
                                + read + " of " + expected + " records"));
                    }
                    return null;
                } catch (IOException e) {
                    closeStream();
                    throw new UncheckedIOException("Failed reading " + path, e);
                }
            }

            private void closeStream() {
                if (!closed) {
                    closed = true;
                    try { in.close(); } catch (IOException ignored) {}
                }
            }

            @Override
            public boolean hasNext() {
                return head != null;
            }

            @Override
            public T next() {
                if (head == null) throw new NoSuchElementException();
                T result = head;
                head = advance();
                return result;
            }

            @Override
            public void close() {
                closeStream();
            }
        };
    }

    void delete() {
        try {
            if (out != null) {
                out.close();
                out = null;
            }
            Files.deleteIfExists(path);
        } catch (IOException ignored) {}
    }

    /** Closes the write stream (if any) and removes the file; idempotent. Lets the pipeline track() it (BG-124). */
    @Override
    public void close() {
        delete();
    }
}
