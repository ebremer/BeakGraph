package com.ebremer.beakgraph.huge;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A flat temp file of codec-encoded records that is written once and can be
 * streamed any number of times. The huge writer keeps each part of a sorted
 * dictionary stream this way because later phases re-read it (dictionary
 * encode, triple-term component join) - an {@link ExternalSorter} stream is
 * single-shot.
 *
 * <p>The file is also readable a chunk at a time, by any number of threads at
 * once ({@link #chunkReader}): where each run of up to {@link #CHUNK_RECORDS}
 * records (or {@link #CHUNK_BYTES} bytes) begins is noted as it is written,
 * so a chunk is one read of a known stretch of the file and the records are
 * decoded from memory.
 *
 * @author Erich Bremer
 */
final class RecordFile<T> implements AutoCloseable {

    /** A chunk ends at this many records, */
    static final int CHUNK_RECORDS = 4096;
    /** or at the first record that begins this many bytes after the chunk's start. */
    static final int CHUNK_BYTES = 1 << 18;

    private final Path path;
    private final ExternalSorter.Codec<T> codec;
    private HugeIO.Counted counter;
    private DataOutputStream out;
    private long count = 0;
    private boolean finished = false;
    // Where each chunk begins in the file, and the ordinal of its first record.
    private long[] chunkOffset = new long[8];
    private long[] chunkFirst = new long[8];
    private int chunks = 0;
    /** The file's length once finished. */
    private long length = 0;

    RecordFile(Path path, ExternalSorter.Codec<T> codec) {
        this.path = path;
        this.codec = codec;
    }

    void append(T record) throws IOException {
        if (finished) {
            throw new IllegalStateException("RecordFile already finished: " + path);
        }
        if (out == null) {
            counter = new HugeIO.Counted(new BufferedOutputStream(Files.newOutputStream(path), 1 << 16));
            out = new DataOutputStream(counter);
        }
        long position = counter.count();
        if (chunks == 0 || count - chunkFirst[chunks - 1] == CHUNK_RECORDS
                || position - chunkOffset[chunks - 1] >= CHUNK_BYTES) {
            if (chunks == chunkOffset.length) {
                chunkOffset = Arrays.copyOf(chunkOffset, chunks * 2);
                chunkFirst = Arrays.copyOf(chunkFirst, chunks * 2);
            }
            chunkOffset[chunks] = position;
            chunkFirst[chunks] = count;
            chunks++;
        }
        codec.write(out, record);
        count++;
    }

    void finish() throws IOException {
        if (finished) return;
        finished = true;
        if (out != null) {
            length = counter.count();
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

    /** Chunks the finished file holds: its records in order, each chunk a stretch of them. */
    int chunks() {
        if (!finished) {
            throw new IllegalStateException("RecordFile not finished: " + path);
        }
        return chunks;
    }

    /** Opens the file for reading by chunk; close it when the last chunk has been read. */
    ChunkReader chunkReader() throws IOException {
        if (!finished) {
            throw new IllegalStateException("RecordFile not finished: " + path);
        }
        return new ChunkReader();
    }

    /** The file read a chunk at a time. {@link #read} may be called by several threads at once. */
    final class ChunkReader implements AutoCloseable {
        private final FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);

        private ChunkReader() throws IOException {}

        /** The records of chunk {@code chunk}, in order. */
        List<T> read(int chunk) throws IOException {
            boolean last = chunk + 1 == chunks;
            long from = chunkOffset[chunk];
            long bytes = (last ? length : chunkOffset[chunk + 1]) - from;
            int records = (int) ((last ? count : chunkFirst[chunk + 1]) - chunkFirst[chunk]);
            if (bytes > Integer.MAX_VALUE - 8) {
                throw new IOException("Record file " + path + ": chunk " + chunk + " of " + bytes + " bytes is too large to read at once");
            }
            byte[] data = new byte[(int) bytes];
            ByteBuffer buffer = ByteBuffer.wrap(data);
            long position = from;
            while (buffer.hasRemaining()) {
                int n = channel.read(buffer, position);
                if (n < 0) {
                    throw new IOException("Record file " + path + " truncated: chunk " + chunk + " ends at "
                            + position + " of " + (from + bytes));
                }
                position += n;
            }
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
            List<T> out = new ArrayList<>(records);
            try {
                for (int i = 0; i < records; i++) {
                    out.add(codec.read(in));
                }
            } catch (EOFException eof) {
                throw new IOException("Record file " + path + " corrupt: chunk " + chunk + " holds "
                        + out.size() + " of " + records + " records", eof);
            }
            if (in.available() != 0) {
                throw new IOException("Record file " + path + " corrupt: chunk " + chunk + " holds "
                        + in.available() + " bytes past its " + records + " records");
            }
            return out;
        }

        @Override
        public void close() {
            try { channel.close(); } catch (IOException ignored) {}
        }
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
