package com.ebremer.beakgraph.io;

import io.jhdf.api.dataset.ContiguousDataset;
import io.jhdf.nio.FileChannelFromSeekableByteChannel;
import io.jhdf.storage.HdfFileChannel;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

/**
 * Builds the {@link RandomAccessBytes} view of a jHDF contiguous dataset.
 *
 * <p>Datasets that fit a ByteBuffer keep today's exact path - jHDF's own
 * mapped buffer, whose lifetime jHDF manages via {@code HdfFile.close()}.
 * Datasets past the 2 GiB ByteBuffer ceiling (which {@code getBuffer()}
 * cannot serve at all) are FFM-mapped at {@code dataAddress + userBlockSize}
 * - the same file offset jHDF itself would map - through jHDF's OWN open
 * channel, with an automatic arena managing the unmap. Mapping through that
 * channel pins the same file the dataset's metadata (address, size, the
 * numEntries / width attributes) came from; re-opening the path mapped
 * whatever the path named at that moment, and index datasets are mapped
 * lazily at first use, so a store rebuilt in place between open and first
 * query decoded old metadata against new bytes (BG-448).
 *
 * <p>Channel-backed files (an {@code HdfFile} opened over a
 * {@code SeekableByteChannel}, e.g. HTTP range requests) take neither path:
 * they cannot be memory-mapped, the FFM path's local file path does not
 * correspond to the actual bytes, and {@code getBuffer()} would materialize
 * the whole dataset up front. They are served by {@link ChannelBytes}, which
 * reads only the touched ranges through the channel.
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
        HdfFileChannel hfc = dataset.getHdfFile().getHdfBackingStorage() instanceof HdfFileChannel c ? c : null;
        if (address >= 0 && hfc != null && hfc.getFileChannel() instanceof FileChannelFromSeekableByteChannel) {
            return new ChannelBytes(hfc.getFileChannel(),
                    address + dataset.getHdfFile().getUserBlockSize(), size);
        }
        if (size > ffmThreshold && address >= 0) {
            try {
                long fileOffset = address + dataset.getHdfFile().getUserBlockSize();
                if (hfc != null) {
                    // The file jHDF has open - the one the metadata describes.
                    return MemorySegmentBytes.map(hfc.getFileChannel(), fileOffset, size);
                }
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
