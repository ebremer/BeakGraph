package com.ebremer.beakgraph.io;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

/**
 * {@link RandomAccessBytes} over a window of a {@link FileChannel}, using
 * absolute positional reads - the path for channel-backed HDF5 storage that
 * cannot be memory-mapped (an HTTP range-request channel, a zip/S3
 * filesystem, ...). Nothing is materialized up front: each read touches only
 * the bytes it asks for, so a remote dataset is fetched lazily, range by
 * range, by whatever block caching the underlying channel provides.
 *
 * <p>Multi-byte reads are assembled BIG-ENDIAN from the raw bytes, matching
 * the on-disk format. Instances are safe for concurrent readers as long as
 * the channel's positional reads are thread-safe (jHDF's channel wrapper
 * serializes them internally); the 8-byte scratch buffer is per-thread.
 *
 * <p>{@link IOException}s surface as {@link UncheckedIOException} - this
 * interface's contract has no checked I/O failures.
 *
 * @author Erich Bremer
 */
public final class ChannelBytes implements RandomAccessBytes {

    private static final ThreadLocal<ByteBuffer> SCRATCH =
            ThreadLocal.withInitial(() -> ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN));

    private final FileChannel channel;
    private final long base;
    private final long size;

    /**
     * @param channel source of positional reads; NOT owned (closing is the
     *                storage's job, this view just reads through it)
     * @param base    absolute channel offset of this window's byte 0
     * @param size    window length in bytes
     */
    public ChannelBytes(FileChannel channel, long base, long size) {
        if (base < 0 || size < 0) {
            throw new IllegalArgumentException("base " + base + ", size " + size);
        }
        this.channel = channel;
        this.base = base;
        this.size = size;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public byte get(long offset) {
        return scratch(offset, 1).get(0);
    }

    @Override
    public long getLong(long offset) {
        return scratch(offset, Long.BYTES).getLong(0);
    }

    @Override
    public float getFloat(long offset) {
        return scratch(offset, Float.BYTES).getFloat(0);
    }

    @Override
    public double getDouble(long offset) {
        return scratch(offset, Double.BYTES).getDouble(0);
    }

    @Override
    public void get(long offset, byte[] dst, int dstOffset, int length) {
        check(offset, length);
        readFully(ByteBuffer.wrap(dst, dstOffset, length), offset);
    }

    private ByteBuffer scratch(long offset, int length) {
        check(offset, length);
        ByteBuffer bb = SCRATCH.get().clear().limit(length);
        readFully(bb, offset);
        return bb;
    }

    private void check(long offset, int length) {
        // length > size - offset is the overflow-safe form of offset + length > size
        if (offset < 0 || length > size - offset) {
            throw new IndexOutOfBoundsException(
                    "offset " + offset + ", length " + length + ", size " + size);
        }
    }

    private void readFully(ByteBuffer bb, long offset) {
        long channelPosition = base + offset;
        int start = bb.position();
        try {
            while (bb.hasRemaining()) {
                int n = channel.read(bb, channelPosition + (bb.position() - start));
                if (n <= 0) {
                    throw new EOFException("Unexpected end of channel at offset " + offset
                            + " (channel position " + (channelPosition + bb.position() - start) + ")");
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Channel read failed at offset " + offset, e);
        }
    }
}
