package com.ebremer.beakgraph.io;

import io.jhdf.api.dataset.ContiguousDataset;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

/**
 * Builds the {@link RandomAccessBytes} view of a jHDF contiguous dataset.
 *
 * <p>Datasets that fit a ByteBuffer keep today's exact path - jHDF's own
 * mapped buffer, whose lifetime jHDF manages via {@code HdfFile.close()}.
 * Datasets past the 2 GiB ByteBuffer ceiling (which {@code getBuffer()}
 * cannot serve at all) are FFM-mapped directly from the underlying file at
 * {@code dataAddress + userBlockSize} - the same file offset jHDF itself
 * would map - with an automatic arena managing the unmap.
 *
 * <p>The threshold is overridable for testing and operations via the
 * {@code beakgraph.ffm.threshold} system property (bytes; datasets strictly
 * larger go through FFM). Setting it to 0 forces every dataset onto the FFM
 * path.
 *
 * @author Erich Bremer
 */
public final class DatasetBytes {

    /** Above this many bytes a dataset is FFM-mapped instead of using getBuffer(). */
    private static volatile long ffmThreshold =
            Long.getLong("beakgraph.ffm.threshold", Integer.MAX_VALUE);

    private DatasetBytes() {}

    /** Overrides the FFM threshold (tests); returns the previous value. */
    static long setFfmThreshold(long thresholdBytes) {
        long previous = ffmThreshold;
        ffmThreshold = thresholdBytes;
        return previous;
    }

    public static RandomAccessBytes of(ContiguousDataset dataset) {
        long size = dataset.getSizeInBytes();
        if (size == 0) {
            return new ByteBufferBytes(ByteBuffer.allocate(0));
        }
        long address = dataset.getDataAddress();
        if (size > ffmThreshold && address >= 0) {
            try {
                long fileOffset = address + dataset.getHdfFile().getUserBlockSize();
                return MemorySegmentBytes.map(dataset.getFileAsPath(), fileOffset, size);
            } catch (IOException e) {
                throw new UncheckedIOException(
                    "Failed to FFM-map dataset '" + dataset.getPath() + "' ("
                  + size + " bytes) from " + dataset.getFileAsPath(), e);
            }
        }
        // <= 2 GiB: jHDF's own mapped buffer, exactly as before.
        return new ByteBufferBytes(dataset.getBuffer());
    }
}
