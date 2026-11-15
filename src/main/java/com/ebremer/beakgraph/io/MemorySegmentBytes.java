package com.ebremer.beakgraph.io;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * {@link RandomAccessBytes} over an FFM {@link MemorySegment} - on a JVM the
 * path for every dataset of a store opened from a local file, past
 * ByteBuffer's 2 GiB ceiling or not; in the GraalVM native binary, for the
 * datasets past it (see {@link DatasetBytes}). Since JDK 22 a file region of
 * any size can be mapped straight to a segment.
 *
 * <p>A segment handed to the constructor lives as its owner decides: a
 * store's datasets are slices of the one mapping Falcon makes of the file,
 * unmapped when the store is closed (see {@link DatasetBytes}). The
 * {@code map} methods here use an automatic {@link Arena} instead: the
 * region unmaps when the segment becomes unreachable, and survives the close
 * of the channel it was made from. Windows caveat: as with any mapping, the
 * file stays in-use until the unmap actually happens.
 *
 * <p>All reads are absolute and the layouts are unaligned BIG-ENDIAN (the
 * bit-packed data honors no alignment). Both kinds of arena are shared, so
 * instances are safe for concurrent readers.
 *
 * @author Erich Bremer
 */
public final class MemorySegmentBytes implements RandomAccessBytes {

    private static final ValueLayout.OfLong LONG_BE =
            ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
    private static final ValueLayout.OfFloat FLOAT_BE =
            ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
    private static final ValueLayout.OfDouble DOUBLE_BE =
            ValueLayout.JAVA_DOUBLE_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);

    private final MemorySegment segment;

    public MemorySegmentBytes(MemorySegment segment) {
        this.segment = segment;
    }

    /**
     * Maps {@code size} bytes of {@code file} starting at {@code offset},
     * read-only, through a channel opened here for the purpose. Prefer
     * {@link #map(FileChannel, long, long)} whenever a channel on the file is
     * already open: this form resolves whatever the path names NOW.
     */
    public static MemorySegmentBytes map(Path file, long offset, long size) throws IOException {
        try (FileChannel fc = FileChannel.open(file, StandardOpenOption.READ)) {
            return map(fc, offset, size);
        }
    }

    /** Maps {@code size} bytes at {@code offset} of an open channel, read-only; the mapping outlives the channel. */
    public static MemorySegmentBytes map(FileChannel channel, long offset, long size) throws IOException {
        MemorySegment seg = channel.map(FileChannel.MapMode.READ_ONLY, offset, size, Arena.ofAuto());
        return new MemorySegmentBytes(seg);
    }

    @Override
    public long size() {
        return segment.byteSize();
    }

    @Override
    public byte get(long offset) {
        return segment.get(ValueLayout.JAVA_BYTE, offset);
    }

    @Override
    public long getLong(long offset) {
        return segment.get(LONG_BE, offset);
    }

    @Override
    public float getFloat(long offset) {
        return segment.get(FLOAT_BE, offset);
    }

    @Override
    public double getDouble(long offset) {
        return segment.get(DOUBLE_BE, offset);
    }

    @Override
    public void get(long offset, byte[] dst, int dstOffset, int length) {
        MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, offset, dst, dstOffset, length);
    }
}
