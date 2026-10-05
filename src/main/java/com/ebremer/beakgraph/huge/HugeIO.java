package com.ebremer.beakgraph.huge;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Small shared I/O helpers for the huge writer: temp-file-to-dataset streaming
 * and unsigned varint coding for the spill record formats.
 *
 * @author Erich Bremer
 */
public final class HugeIO {

    /** Copy buffer for temp-file-to-HDF5 streaming; bounds writer RAM per transfer. */
    static final int TRANSFER_BUFFER_BYTES = 8 * 1024 * 1024;

    private HugeIO() {}

    /** Streams an entire temp file into an already-created dataset. */
    static void copyFileIntoDataset(Path file, StreamingHdf5Dataset ds) throws IOException {
        try (InputStream in = new BufferedInputStream(Files.newInputStream(file), 1 << 20)) {
            byte[] buf = new byte[TRANSFER_BUFFER_BYTES];
            int n;
            while ((n = in.read(buf)) > 0) {
                ds.write(buf, 0, n);
            }
        }
    }

    /**
     * Unsigned LEB128-style varint (7 bits per byte, high bit = continuation).
     * Encoded into a scratch array and written with one call: the buffered
     * stream behind {@code out} takes its lock per write, and a byte-at-a-time
     * varint paid it up to ten times per record (BG-250).
     */
    public static void writeVarLong(DataOutput out, long value) throws IOException {
        if (value < 0) {
            throw new IllegalArgumentException("varint value must be non-negative: " + value);
        }
        byte[] buf = new byte[10];
        int n = 0;
        while ((value & ~0x7FL) != 0) {
            buf[n++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        buf[n++] = (byte) value;
        out.write(buf, 0, n);
    }

    public static long readVarLong(DataInput in) throws IOException {
        long result = 0;
        int shift = 0;
        while (true) {
            byte b = in.readByte();
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
            if (shift >= 64) {
                throw new IOException("Malformed varint (too long)");
            }
        }
    }

    /**
     * Reads exactly {@code len} bytes or throws {@link EOFException}; DataInput's
     * readFully equivalent, factored for symmetry with the writers here.
     */
    static byte[] readBytes(DataInput in, int len) throws IOException {
        byte[] data = new byte[len];
        in.readFully(data);
        return data;
    }
}

