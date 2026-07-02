package com.ebremer.beakgraph.io;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * {@link RandomAccessBytes} over a {@link ByteBuffer} - the classic path for
 * jHDF-mapped datasets (which are capped at 2 GiB by ByteBuffer's int
 * indexing; larger datasets use {@link MemorySegmentBytes}).
 *
 * <p>The source buffer is duplicated at construction (position/limit/order of
 * the original are never touched afterwards) and forced BIG-ENDIAN; only
 * absolute reads are issued, so instances are safe for concurrent readers.
 *
 * @author Erich Bremer
 */
public final class ByteBufferBytes implements RandomAccessBytes {

    private final ByteBuffer buffer;
    private final long size;

    public ByteBufferBytes(ByteBuffer source) {
        // duplicate: shares content, isolates position/limit/order from the caller
        this.buffer = source.duplicate().order(ByteOrder.BIG_ENDIAN);
        this.size = this.buffer.limit();
    }

    private static int idx(long offset) {
        // The wrapped buffer is < 2 GiB by construction; a larger offset is a
        // caller bug and surfaces as the same IndexOutOfBounds the reads throw.
        if (offset < 0 || offset > Integer.MAX_VALUE) {
            throw new IndexOutOfBoundsException("offset " + offset);
        }
        return (int) offset;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public byte get(long offset) {
        return buffer.get(idx(offset));
    }

    @Override
    public long getLong(long offset) {
        return buffer.getLong(idx(offset));
    }

    @Override
    public float getFloat(long offset) {
        return buffer.getFloat(idx(offset));
    }

    @Override
    public double getDouble(long offset) {
        return buffer.getDouble(idx(offset));
    }

    @Override
    public void get(long offset, byte[] dst, int dstOffset, int length) {
        buffer.get(idx(offset), dst, dstOffset, length);
    }
}
