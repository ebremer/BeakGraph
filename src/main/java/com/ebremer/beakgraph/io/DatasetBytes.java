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
 * first query cannot pair old metadata with new bytes (BG-448). A dataset is
 * read through its segment itself ({@link MemorySegmentBytes}), whatever its
 * size. A ByteBuffer view of the same segment ({@link ByteBufferBytes}) is
 * kept as an option only: queries on a current-format store measured the
 * same through either, but the linear bitmap scans of a store older than the
 * rank/select directories ran about twice as fast through the segment.
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
 * <p>The {@code beakgraph.ffm.threshold} system property (bytes, default 0)
 * brings the ByteBuffer view back for testing and operations: a mapped
 * dataset of at most that many bytes is read through a ByteBuffer view of
 * its segment, a strictly larger one through the segment. A ByteBuffer
 * cannot index past 2 GiB, so datasets beyond that use the segment whatever
 * the property says.
 *
 * @author Erich Bremer
 */
public final class DatasetBytes {

    /**
     * Up to this many bytes a mapped dataset is read through a ByteBuffer view of its MemorySegment instead of
     * the segment itself. 0, the default: every dataset is read through its segment.
     */
    private static volatile long ffmThreshold = Long.getLong("beakgraph.ffm.threshold", 0L);

    private DatasetBytes() {}

    /** Overrides the FFM threshold (tests); returns the previous value. */
    static long setFfmThreshold(long thresholdBytes) {
        long previous = ffmThreshold;
        ffmThreshold = thresholdBytes;
        return previous;
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
            MemorySegment segment = mapped.get();
            // The segment, unless the threshold asks for a ByteBuffer view - which cannot index past 2 GiB.
            return (size > ffmThreshold || size > Integer.MAX_VALUE)
                    ? new MemorySegmentBytes(segment)
                    : new ByteBufferBytes(segment.asByteBuffer());
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
