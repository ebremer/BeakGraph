package com.ebremer.beakgraph.huge;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * The rows of one column that fall in one range of the term order, each with
 * the code of its term: the term's place among the range's own entities or
 * literals, which is all a range can know of an id while the ranges before it
 * are still being merged. The ids follow once every range's counts are in
 * ({@code HugeBuildPipeline}), without a second look at any term.
 *
 * <p>Layout: groups of {@code (code, count, row * count)} as unsigned varints,
 * a term's rows together and its code written once for them - or once for
 * every {@link #GROUP_ROWS} of them, so that a term of a billion rows is not
 * held to be counted. Codes ascend from group to group.
 */
final class RangeIds implements AutoCloseable {

    /** Rows one group holds at most. */
    static final int GROUP_ROWS = 4096;

    private final Path path;
    private Writer writer = new Writer();
    private long count = 0;
    private boolean finished = false;

    RangeIds(Path path) {
        this.path = path;
    }

    /** Adds a row of the term with {@code code}; the codes of successive calls never descend. */
    void add(long code, long row) throws IOException {
        if (finished) {
            throw new IllegalStateException("Range ids already finished: " + path);
        }
        writer.add(code, row);
        count++;
    }

    /** Rows added. */
    long count() {
        return count;
    }

    void finish() throws IOException {
        if (finished) {
            return;
        }
        finished = true;
        writer.close();
        writer = null;
    }

    /** The groups in the order they were added; close it when done. */
    Reader read() throws IOException {
        if (!finished) {
            throw new IllegalStateException("Range ids not finished: " + path);
        }
        return new Reader();
    }

    void delete() {
        try {
            if (writer != null) {
                writer.abandon();
                writer = null;
            }
            finished = true;
            Files.deleteIfExists(path);
        } catch (IOException ignored) {}
    }

    /** Closes the write stream (if any) and removes the file; idempotent, so the pipeline can track it. */
    @Override
    public void close() {
        delete();
    }

    private final class Writer {
        private OutputStream out;
        private final byte[] buf = new byte[1 << 16];
        private int held = 0;
        private long code = -1;
        private final long[] rows = new long[GROUP_ROWS];
        private int n = 0;

        void add(long code, long row) throws IOException {
            if (code != this.code || n == GROUP_ROWS) {
                group();
                this.code = code;
            }
            rows[n++] = row;
        }

        private void group() throws IOException {
            if (n == 0) {
                return;
            }
            varLong(code);
            varLong(n);
            for (int i = 0; i < n; i++) {
                varLong(rows[i]);
            }
            n = 0;
        }

        private void varLong(long value) throws IOException {
            if (value < 0) {
                throw new IllegalArgumentException("varint value must be non-negative: " + value);
            }
            if (held + 10 > buf.length) {
                flush();
            }
            while ((value & ~0x7FL) != 0) {
                buf[held++] = (byte) ((value & 0x7F) | 0x80);
                value >>>= 7;
            }
            buf[held++] = (byte) value;
        }

        private void flush() throws IOException {
            if (out == null) {
                out = Files.newOutputStream(path);
            }
            out.write(buf, 0, held);
            held = 0;
        }

        void close() throws IOException {
            group();
            flush();   // an empty file where there was no row: readers find a valid stream
            out.close();
            out = null;
        }

        void abandon() throws IOException {
            if (out != null) {
                out.close();
                out = null;
            }
        }
    }

    /** One group at a time: {@link #next()}, then {@link #code}, {@link #size} and {@link #rows}. */
    final class Reader implements AutoCloseable {
        private final InputStream in = Files.newInputStream(path);
        private final byte[] buf = new byte[1 << 16];
        private int pos = 0;
        private int limit = 0;
        private long read = 0;
        long code;
        int size;
        final long[] rows = new long[GROUP_ROWS];

        Reader() throws IOException {}

        /** Loads the next group; false at the end of the file. */
        boolean next() throws IOException {
            if (pos == limit && !fill()) {
                // A group cut short fails in varLong; a file cut at a group's
                // end is told from a whole one by the count alone (BG-129).
                if (read != count) {
                    throw new IOException("Range ids " + path + " truncated: read " + read + " of " + count + " rows");
                }
                return false;
            }
            code = varLong();
            long n = varLong();
            if (n < 1 || n > GROUP_ROWS) {
                throw new IOException("Corrupt range ids " + path + ": group of " + n + " rows");
            }
            size = (int) n;
            for (int i = 0; i < size; i++) {
                rows[i] = varLong();
            }
            read += size;
            return true;
        }

        private boolean fill() throws IOException {
            int n = in.read(buf, 0, buf.length);
            pos = 0;
            limit = Math.max(0, n);
            return n > 0;
        }

        private long varLong() throws IOException {
            long result = 0;
            int shift = 0;
            while (true) {
                if (pos == limit && !fill()) {
                    throw new EOFException("Range ids " + path + " end inside a group");
                }
                byte b = buf[pos++];
                result |= (long) (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    return result;
                }
                shift += 7;
                if (shift >= 64) {
                    throw new IOException("Corrupt range ids " + path + ": malformed varint");
                }
            }
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }
}
