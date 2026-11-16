package com.ebremer.beakgraph.io;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * {@link RandomAccessBytes} over an array kept in several mapped pieces of
 * equal length, one after another: a Zarr array past the size of one chunk
 * (SPECIFICATIONS.md §3a, item 3), whose chunks are separate files - or
 * separate entries of one archive - and so cannot be one mapping. Every read
 * goes straight to the piece that holds it, as {@link MemorySegmentBytes}
 * reads its one segment: nothing is copied on the way, and a value that lies
 * across two pieces - at most one read in a piece's length - is put together
 * from its bytes.
 *
 * <p>As with one mapping ({@link DatasetBytes}), a piece is read through its
 * segment or through a ByteBuffer view of it: the GraalVM native binary reads
 * faster through the view, a JVM no slower through the segment.
 *
 * <p>The pieces live as long as their owner's arena (a store's, closed with
 * the store); a read after that throws {@link IllegalStateException}. All
 * reads are absolute and BIG-ENDIAN, and instances are safe for concurrent
 * readers.
 *
 * @author Erich Bremer
 */
public final class SegmentedBytes implements RandomAccessBytes {

    private static final ValueLayout.OfLong LONG_BE =
            ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
    private static final ValueLayout.OfFloat FLOAT_BE =
            ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
    private static final ValueLayout.OfDouble DOUBLE_BE =
            ValueLayout.JAVA_DOUBLE_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
    private final MemorySegment[] pieces;
    /** A big-endian view of each piece, where the pieces are read through views; else null. */
    private final ByteBuffer[] views;
    /** The bytes of the array in each piece but the last, which holds the rest. */
    private final long length;
    /**
     * {@code floor((2^64 - 1) / length)}, unsigned: the piece of an offset by the high half of a
     * multiplication ({@link #piece}), where a division is several times slower.
     */
    private final long reciprocal;
    private final long size;

    /**
     * @param pieces the array's bytes in order; each at least {@code length}
     *               bytes, the last at least what is left of the array (a
     *               Zarr chunk is stored whole, past the array's end too)
     * @param length the bytes of the array each piece but the last holds
     * @param size   the array's length in bytes
     */
    public SegmentedBytes(MemorySegment[] pieces, long length, long size) {
        this(pieces, length, size, false);
    }

    /**
     * @param throughViews whether each piece is read through a ByteBuffer
     *                     view of it, and not its segment; a view holds a
     *                     piece of under 2 GiB only
     */
    public SegmentedBytes(MemorySegment[] pieces, long length, long size, boolean throughViews) {
        if (throughViews && length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("a piece of " + length + " bytes has no ByteBuffer view");
        }
        if (length < 1 || size < 1) {
            throw new IllegalArgumentException("length " + length + ", size " + size);
        }
        long count = (size + length - 1) / length;
        if (pieces.length != count) {
            throw new IllegalArgumentException(pieces.length + " pieces of " + length + " bytes for an array of " + size);
        }
        this.pieces = new MemorySegment[pieces.length];
        for (int i = 0; i < pieces.length; i++) {
            long held = Math.min(length, size - i * length);
            if (pieces[i].byteSize() < held) {
                throw new IllegalArgumentException("piece " + i + " is " + pieces[i].byteSize() + " bytes, short of the "
                        + held + " it holds of the array");
            }
            // Exactly the array's bytes: a read past the array's end is then out of the piece's bounds too.
            this.pieces[i] = pieces[i].asSlice(0, held);
        }
        this.length = length;
        this.reciprocal = Long.divideUnsigned(-1L, length);
        this.size = size;
        if (throughViews) {
            this.views = new ByteBuffer[pieces.length];
            for (int i = 0; i < views.length; i++) {
                views[i] = this.pieces[i].asByteBuffer().order(ByteOrder.BIG_ENDIAN);
            }
        } else {
            this.views = null;
        }
    }

    /** A place in a view: one an {@code int} holds, or the read is outside the array. */
    private static int idx(long at) {
        if (at < 0 || at > Integer.MAX_VALUE) {
            throw new IndexOutOfBoundsException("offset " + at + " in a piece");
        }
        return (int) at;
    }

    @Override
    public long size() {
        return size;
    }

    /**
     * The piece that holds {@code offset}: {@code offset / length}, by a
     * multiplication and a correction. With {@code m = floor((2^64 - 1) /
     * length)}, the high 64 bits of {@code offset * m} are the quotient or
     * one less - {@code m} falls short of {@code 2^64 / length} by at most
     * {@code 1 / length} of itself, which over an offset below 2^64 comes to
     * less than one - so one step up settles it. An offset outside the array
     * (a negative one is a huge one, unsigned) gives an index outside the
     * pieces, or a place outside the piece it names: every read of it fails
     * with {@link IndexOutOfBoundsException}.
     */
    private int piece(long offset) {
        int i = (int) Math.unsignedMultiplyHigh(offset, reciprocal);
        return offset - i * length >= length ? i + 1 : i;
    }

    @Override
    public byte get(long offset) {
        int i = piece(offset);
        long at = offset - i * length;
        ByteBuffer[] v = views;
        return v != null ? v[i].get(idx(at)) : pieces[i].get(ValueLayout.JAVA_BYTE, at);
    }

    @Override
    public long getLong(long offset) {
        int i = piece(offset);
        long at = offset - i * length;
        if (at <= length - Long.BYTES) {
            ByteBuffer[] v = views;
            return v != null ? v[i].getLong(idx(at)) : pieces[i].get(LONG_BE, at);
        }
        long v = 0;
        for (int k = 0; k < Long.BYTES; k++) {
            v = (v << 8) | (get(offset + k) & 0xFFL);
        }
        return v;
    }

    @Override
    public float getFloat(long offset) {
        int i = piece(offset);
        long at = offset - i * length;
        if (at <= length - Float.BYTES) {
            ByteBuffer[] v = views;
            return v != null ? v[i].getFloat(idx(at)) : pieces[i].get(FLOAT_BE, at);
        }
        int v = 0;
        for (int k = 0; k < Float.BYTES; k++) {
            v = (v << 8) | (get(offset + k) & 0xFF);
        }
        return Float.intBitsToFloat(v);
    }

    @Override
    public double getDouble(long offset) {
        int i = piece(offset);
        long at = offset - i * length;
        if (at <= length - Double.BYTES) {
            ByteBuffer[] v = views;
            return v != null ? v[i].getDouble(idx(at)) : pieces[i].get(DOUBLE_BE, at);
        }
        return Double.longBitsToDouble(getLong(offset));
    }

    @Override
    public void get(long offset, byte[] dst, int dstOffset, int count) {
        Objects.checkFromIndexSize(dstOffset, count, dst.length);
        // count > size - offset is the overflow-safe form of offset + count > size
        if (offset < 0 || count > size - offset) {
            throw new IndexOutOfBoundsException("offset " + offset + ", length " + count + ", size " + size);
        }
        while (count > 0) {
            int i = piece(offset);
            long at = offset - i * length;
            int n = (int) Math.min(count, length - at);
            if (views != null) {
                views[i].get(idx(at), dst, dstOffset, n);
            } else {
                MemorySegment.copy(pieces[i], ValueLayout.JAVA_BYTE, at, dst, dstOffset, n);
            }
            offset += n;
            dstOffset += n;
            count -= n;
        }
    }
}
