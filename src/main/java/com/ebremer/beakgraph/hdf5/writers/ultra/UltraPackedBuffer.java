package com.ebremer.beakgraph.hdf5.writers.ultra;

import io.jhdf.api.WritableDataset;
import io.jhdf.api.WritableGroup;

/**
 * Positionally-writable twin of the append-only
 * {@link com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer} write side,
 * for BYTE-ALIGNED widths only. Every S/SB/BB buffer and every columnar id
 * list in the store rounds its width up to a multiple of 8 bits, so entry
 * {@code i} occupies exactly bytes {@code [i*w/8, (i+1)*w/8)} - no two entries
 * share a byte, which is what makes concurrent {@link #set} calls on DISTINCT
 * indexes safe with no synchronization at all. That positional independence is
 * the enabler for the ultra writer's chunk-parallel index emission and column
 * population.
 *
 * <p>The serialized bytes are identical to what the sequential buffer's
 * MSB-first accumulator produces for the same width (big-endian bytes, no
 * trailing partial byte at byte-aligned widths), and {@link #add} writes the
 * same dataset shape and {@code width}/{@code numEntries} attributes, so
 * readers and structural parity checks cannot tell the writers apart.
 */
final class UltraPackedBuffer {

    private final String name;
    private final int bitWidth;
    private final int bytesPerEntry;
    private final long numEntries;
    private final byte[] data;

    UltraPackedBuffer(String name, long numEntries, int bitWidth) {
        if (bitWidth < 8 || bitWidth > 64 || (bitWidth % 8) != 0) {
            throw new IllegalArgumentException(
                    "UltraPackedBuffer requires a byte-aligned width in [8,64], got " + bitWidth);
        }
        if (numEntries < 0) {
            throw new IllegalArgumentException("numEntries must be >= 0, got " + numEntries);
        }
        this.name = name;
        this.bitWidth = bitWidth;
        this.bytesPerEntry = bitWidth / 8;
        this.numEntries = numEntries;
        long bytes = numEntries * (long) bytesPerEntry;
        if (bytes > Integer.MAX_VALUE - 16) {
            // Same ceiling the sequential writer has (its bytes accumulate in a
            // ByteArrayOutputStream); sources beyond it need the -huge writer.
            throw new IllegalArgumentException(
                    "Buffer '" + name + "' needs " + bytes + " bytes; too large for the in-memory ultra writer");
        }
        this.data = new byte[(int) bytes];
    }

    /**
     * Writes entry {@code index}. Safe to call concurrently for distinct
     * indexes (entries never share a byte); racing on the SAME index is a bug.
     */
    void set(long index, long value) {
        if (index < 0 || index >= numEntries) {
            throw new IndexOutOfBoundsException("Index " + index + " out of bounds [0, " + numEntries + ")");
        }
        // Same fit check as BitPackedUnSignedLongBuffer.writeLong: only width 64
        // carries a negative value's full two's-complement pattern.
        if (bitWidth != 64 && (value < 0 || value > ((1L << bitWidth) - 1))) {
            throw new IllegalArgumentException("Value " + value + " does not fit in " + bitWidth + " bits");
        }
        int off = (int) (index * bytesPerEntry);
        for (int b = bytesPerEntry - 1; b >= 0; b--) {
            data[off + b] = (byte) value;
            value >>>= 8;
        }
    }

    long getNumEntries() {
        return numEntries;
    }

    /** Test/verification access: reads entry {@code index} back. */
    long get(long index) {
        if (index < 0 || index >= numEntries) {
            throw new IndexOutOfBoundsException("Index " + index + " out of bounds [0, " + numEntries + ")");
        }
        int off = (int) (index * bytesPerEntry);
        long v = 0;
        for (int b = 0; b < bytesPerEntry; b++) {
            v = (v << 8) | (data[off + b] & 0xFFL);
        }
        return v;
    }

    byte[] bytes() {
        return data;
    }

    void add(WritableGroup group) {
        if (data.length > 0) {
            WritableDataset ds = group.putDataset(name, data);
            ds.putAttribute("width", bitWidth);
            ds.putAttribute("numEntries", numEntries);
        }
    }
}
