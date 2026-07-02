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
 * {@link RandomAccessBytes} over an FFM {@link MemorySegment} - the path for
 * datasets past ByteBuffer's 2 GiB ceiling (and, at parity performance, an
 * alternative below it). Since JDK 22 a file region of any size can be mapped
 * straight to a segment; absolute segment reads JIT to the same machine code
 * as direct-ByteBuffer access.
 *
 * <p>Mappings use an automatic {@link Arena}: the region unmaps when the
 * segment becomes unreachable, so no close() plumbing is needed through the
 * reader stack (the file channel is closed immediately after mapping - the
 * mapping survives it). Windows caveat: as with any mapping, the file stays
 * in-use until the unmap actually happens.
 *
 * <p>All reads are absolute and the layouts are unaligned BIG-ENDIAN (the
 * bit-packed data honors no alignment). Auto arenas are shared, so instances
 * are safe for concurrent readers.
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

    /** Maps {@code size} bytes of {@code file} starting at {@code offset}, read-only. */
    public static MemorySegmentBytes map(Path file, long offset, long size) throws IOException {
        try (FileChannel fc = FileChannel.open(file, StandardOpenOption.READ)) {
            MemorySegment seg = fc.map(FileChannel.MapMode.READ_ONLY, offset, size, Arena.ofAuto());
            return new MemorySegmentBytes(seg);
        }
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
