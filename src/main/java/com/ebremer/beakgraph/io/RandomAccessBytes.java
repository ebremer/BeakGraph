package com.ebremer.beakgraph.io;

/**
 * Absolute-offset random access over an immutable byte region - the seam
 * between BeakGraph's readers and whatever holds the bytes.
 *
 * <p>All offsets are {@code long} (no 2 GiB ceiling) and all multi-byte reads
 * are BIG-ENDIAN, matching the on-disk format (DataOutputStream primitives and
 * MSB-first bit packing). The API is deliberately position-free: every read is
 * absolute, so one instance may be shared by concurrent query threads - the
 * same contract the readers previously relied on with absolute ByteBuffer
 * reads.
 *
 * <p>Implementations: {@link ByteBufferBytes} (wraps the jHDF-mapped buffer,
 * the default for datasets under 2 GiB), {@link MemorySegmentBytes} (an FFM
 * mapped segment for datasets beyond ByteBuffer's reach) and
 * {@link ChannelBytes} (positional reads through a remote-backed channel).
 * The interface is sealed so the hottest call site in the read path -
 * {@code BitPackedUnSignedLongBuffer}'s word fetch, which every id lookup,
 * bitmap probe and binary-search step lands on - can dispatch with a klass
 * compare and a direct call instead of a megamorphic interface call once a
 * JVM has opened all three kinds of dataset (BG-260). A future chunked
 * implementation (e.g. Zarr with a decompressed-chunk cache) joins the
 * {@code permits} list and that buffer's {@code readLong}/{@code readByte}
 * chain; the readers themselves stay untouched.
 *
 * <p>Out-of-range offsets throw {@link IndexOutOfBoundsException}.
 *
 * @author Erich Bremer
 */
public sealed interface RandomAccessBytes permits ByteBufferBytes, MemorySegmentBytes, ChannelBytes {

    /** Total number of readable bytes. */
    long size();

    /** The byte at {@code offset}. */
    byte get(long offset);

    /** The big-endian 64-bit value at {@code offset} (unaligned allowed). */
    long getLong(long offset);

    /** The big-endian 32-bit float at {@code offset} (unaligned allowed). */
    float getFloat(long offset);

    /** The big-endian 64-bit double at {@code offset} (unaligned allowed). */
    double getDouble(long offset);

    /** Copies {@code length} bytes starting at {@code offset} into {@code dst}. */
    void get(long offset, byte[] dst, int dstOffset, int length);

    /**
     * Whether reads reach a remote store (an HTTP range channel) rather than
     * local memory or a local file. Readers use it to skip eager accelerators
     * that touch a whole section - over HTTP that is a full download (BG-240).
     */
    default boolean isRemote() {
        return false;
    }
}
