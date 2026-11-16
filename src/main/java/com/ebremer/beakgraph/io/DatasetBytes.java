package com.ebremer.beakgraph.io;

import com.ebremer.falcon.hdf5.Dataset;
import com.ebremer.falcon.hdf5.RangeReader;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Builds the {@link RandomAccessBytes} view of a contiguous dataset of a
 * store opened through Falcon (https://github.com/ebremer/falcon).
 *
 * <p>A store opened from a local file is memory-mapped once, whole, by
 * Falcon, and each dataset is a view of that one mapping
 * ({@code Dataset.storageSegment()}): nothing is copied or read ahead, and a
 * dataset of any size is there at once. The view is of the very file whose
 * metadata (address, size, the numEntries / width attributes) was read: there
 * is no second open of the path, so a store rebuilt in place between open and
 * first query cannot pair old metadata with new bytes (BG-448).
 *
 * <p>Which way a mapped dataset is read depends on what runs the code,
 * because the two ways measure differently there:
 * <ul>
 *   <li><b>On a JVM</b>, through its segment itself
 *       ({@link MemorySegmentBytes}), whatever its size. Queries on a
 *       current-format store measured the same through a ByteBuffer view of
 *       the segment, but the linear bitmap scans of a store older than the
 *       rank/select directories ran about twice as fast through the
 *       segment.</li>
 *   <li><b>In the GraalVM native binary</b>, through a ByteBuffer view of the
 *       segment ({@link ByteBufferBytes}), up to the 2 GiB a ByteBuffer can
 *       index. Through the segment the binary read a format v6 store 27%
 *       slower in {@code -verify -deep} and 28% slower in {@code -export NQ}
 *       (38.8 s against 30.5 s, 7.25 s against 5.66 s; 4.0M quads,
 *       2026-11-14). A dataset past 2 GiB is read through its segment there
 *       too.</li>
 * </ul>
 * An array mapped in several pieces (a Zarr array of several chunks,
 * {@link SegmentedBytes}) follows the same rule piece by piece: in the
 * native binary a store of such arrays took 14.0 s to {@code -verify -deep}
 * through the segments and 11.6 s through views, where the same store of
 * one-chunk arrays takes 11.3 s (2026-11-15).
 *
 * <p>The mapping lasts exactly as long as the store is open: Falcon unmaps
 * the file when {@code HDF5Reader.close()} closes it, at once and on every
 * OS, so the store file is free to be replaced or deleted from then on (on
 * Windows too - there is no wait for a GC, BG-353). The other side of that:
 * a view read after the close throws {@link IllegalStateException}, so a
 * reader must not be closed while a query is still reading through it.
 *
 * <p>Channel-backed stores (opened over a {@code SeekableByteChannel}, e.g.
 * HTTP range requests) cannot be memory-mapped. They are served by
 * {@link ChannelBytes}, which reads only the touched ranges, at the dataset's
 * offset in the file ({@code Dataset.storageOffset()}), through the same
 * reader the store's metadata was read through.
 *
 * <p>The {@code beakgraph.ffm.threshold} system property (bytes) chooses
 * otherwise, for testing and operations: a mapped dataset of at most that
 * many bytes is read through a ByteBuffer view of its segment, a strictly
 * larger one through the segment. Its default is 0 on a JVM (every dataset
 * through its segment) and 2147483647 in the native binary (every dataset a
 * ByteBuffer can index through a view). A ByteBuffer cannot index past
 * 2 GiB, so datasets beyond that use the segment whatever the property says.
 *
 * @author Erich Bremer
 */
public final class DatasetBytes {

    /**
     * The property a GraalVM native image carries ("runtime" while it runs, "buildtime" while it is built) and
     * a JVM does not: GraalVM's {@code ImageInfo.PROPERTY_IMAGE_CODE_KEY}, read by name so that nothing of
     * GraalVM's has to be on the class path.
     */
    private static final String NATIVE_IMAGE_PROPERTY = "org.graalvm.nativeimage.imagecode";

    /** True in a GraalVM native image, built or running. */
    static boolean inNativeImage() {
        return System.getProperty(NATIVE_IMAGE_PROPERTY) != null;
    }

    /**
     * The threshold where {@code beakgraph.ffm.threshold} sets none: 0 on a JVM, so that every dataset is read
     * through its segment, and all a ByteBuffer can index in a native image, so that every dataset it can hold
     * is read through a view (see the class comment for what each measured).
     */
    static long defaultFfmThreshold(boolean nativeImage) {
        return nativeImage ? Integer.MAX_VALUE : 0L;
    }

    /**
     * Up to this many bytes a mapped dataset is read through a ByteBuffer view of its MemorySegment instead of
     * the segment itself.
     */
    private static volatile long ffmThreshold = Long.getLong("beakgraph.ffm.threshold", defaultFfmThreshold(inNativeImage()));

    private DatasetBytes() {}

    /** Overrides the FFM threshold (tests); returns the previous value. */
    static long setFfmThreshold(long thresholdBytes) {
        long previous = ffmThreshold;
        ffmThreshold = thresholdBytes;
        return previous;
    }

    /**
     * The view of a mapped array's bytes, whatever container they were mapped
     * from: an HDF5 dataset's slice of its file's mapping, or a Zarr array's
     * mapped chunk.
     *
     * @param segment the array's bytes, exactly
     */
    public static RandomAccessBytes of(MemorySegment segment) {
        long size = segment.byteSize();
        if (size == 0) {
            return new ByteBufferBytes(ByteBuffer.allocate(0));
        }
        // The segment, unless the threshold asks for a ByteBuffer view - which cannot index past 2 GiB.
        return (size > ffmThreshold || size > Integer.MAX_VALUE)
                ? new MemorySegmentBytes(segment)
                : new ByteBufferBytes(segment.asByteBuffer());
    }

    /**
     * The view of a mapped array that lies in several pieces of equal
     * length, one after another (a Zarr array of several chunks): each piece
     * read through its segment or through a ByteBuffer view of it, by the
     * same threshold as one mapping, held to a piece.
     *
     * @param pieces the array's bytes in order (see {@link SegmentedBytes})
     * @param length the bytes of the array each piece but the last holds
     * @param size   the array's length in bytes
     */
    public static RandomAccessBytes of(MemorySegment[] pieces, long length, long size) {
        return new SegmentedBytes(pieces, length, size, length <= ffmThreshold && length <= Integer.MAX_VALUE);
    }

    /**
     * @param dataset a contiguous dataset of an open store
     * @param channel what the store is read through when it is not mapped
     *                from a local file (see {@link ChannelBytes}), else null
     */
    public static RandomAccessBytes of(Dataset dataset, RangeReader channel) {
        long size = Math.multiplyExact(dataset.dataspace().elementCount(), (long) dataset.datatype().size());
        if (size == 0) {
            return new ByteBufferBytes(ByteBuffer.allocate(0));
        }
        Optional<MemorySegment> mapped = dataset.storageSegment();
        if (mapped.isPresent()) {
            return of(mapped.get());
        }
        OptionalLong offset = dataset.storageOffset();
        if (offset.isEmpty()) {
            throw new IllegalStateException("Dataset '" + dataset.path() + "' (" + size
                    + " bytes) has no bytes in the file: it was never written, or its data is kept in"
                    + " external files (SPECIFICATIONS.md §3, item 2)");
        }
        if (channel == null) {
            throw new IllegalStateException("Dataset '" + dataset.path() + "' cannot be memory-mapped,"
                    + " and its store was not opened through a channel to read it by");
        }
        return new ChannelBytes(channel, offset.getAsLong(), size);
    }
}
