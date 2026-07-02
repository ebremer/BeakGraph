package com.ebremer.beakgraph.huge;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Disk-backed, write-only twin of
 * {@link com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer}: identical
 * MSB-first bit packing and identical dataset shape ({@code width} /
 * {@code numEntries} attributes), but the packed bytes accumulate in a temp
 * file instead of a {@code ByteArrayOutputStream}, so the buffer size is
 * bounded by disk, not heap. Values must fit their declared width - the same
 * silent-truncation guard as the RAM twin.
 *
 * @author Erich Bremer
 */
final class SpillBitPackedBuffer implements AutoCloseable {

    private final Path file;
    private final OutputStream out;
    private final int bitWidth;
    private long writeAccumulator = 0L;
    private int writeAccumulatorCount = 0;
    private long numEntries = 0;
    private long bytesWritten = 0;
    private boolean completed = false;

    SpillBitPackedBuffer(Path file, int bitWidth) throws IOException {
        // Same width envelope as the RAM buffer: the pack accumulator carries a
        // value plus a <=7-bit sub-byte offset in one long, so 58..63 are unsafe;
        // 64 is byte-aligned and fine.
        boolean supported = (bitWidth >= 1 && bitWidth <= 57) || bitWidth == 64;
        if (!supported) {
            throw new IllegalArgumentException(
                "Unsupported bit width: " + bitWidth + ". Supported: 1..57, or 64 (byte-aligned).");
        }
        this.file = file;
        this.bitWidth = bitWidth;
        this.out = new BufferedOutputStream(Files.newOutputStream(file), 1 << 16);
    }

    void writeInteger(int value) {
        if (bitWidth != 32 && bitWidth != 64 && (value < 0 || value > ((1L << bitWidth) - 1))) {
            throw new IllegalArgumentException("Value " + value + " does not fit in " + bitWidth + " bits");
        }
        putValue(value & ((bitWidth == 64) ? -1L : (1L << bitWidth) - 1));
        numEntries++;
    }

    void writeLong(long value) {
        if (bitWidth != 64 && (value < 0 || value > ((1L << bitWidth) - 1))) {
            throw new IllegalArgumentException("Value " + value + " does not fit in " + bitWidth + " bits");
        }
        putValue(value & ((bitWidth == 64) ? -1L : (1L << bitWidth) - 1));
        numEntries++;
    }

    private void putValue(long valToPack) {
        if (completed) {
            throw new IllegalStateException("Buffer already completed: " + file);
        }
        writeAccumulator = (writeAccumulator << bitWidth) | valToPack;
        writeAccumulatorCount += bitWidth;
        try {
            while (writeAccumulatorCount >= 8) {
                int shift = writeAccumulatorCount - 8;
                out.write((byte) (writeAccumulator >>> shift));
                bytesWritten++;
                writeAccumulator &= (1L << shift) - 1;
                writeAccumulatorCount -= 8;
            }
        } catch (IOException ex) {
            throw new UncheckedIOException("Failed writing spill buffer " + file, ex);
        }
    }

    /** Flushes the trailing partial byte and closes the temp file for writing. */
    void complete() throws IOException {
        if (completed) return;
        completed = true;
        if (writeAccumulatorCount > 0) {
            out.write((byte) (writeAccumulator << (8 - writeAccumulatorCount)));
            bytesWritten++;
            writeAccumulator = 0;
            writeAccumulatorCount = 0;
        }
        out.close();
    }

    long getNumEntries() {
        return numEntries;
    }

    int getBitWidth() {
        return bitWidth;
    }

    /**
     * Streams the packed bytes into {@code group} as a dataset named
     * {@code name} with the {@code width}/{@code numEntries} attributes, then
     * deletes the temp file. Mirrors the RAM buffer's {@code add()}: an empty
     * buffer writes no dataset at all.
     */
    void transferTo(StreamingHdf5Group group, String name) throws IOException {
        complete();
        if (bytesWritten == 0) {
            Files.deleteIfExists(file);
            return;
        }
        try (StreamingHdf5Dataset ds = group.createByteDataset(name, bytesWritten)) {
            HugeIO.copyFileIntoDataset(file, ds);
            ds.putAttribute("width", bitWidth);
            ds.putAttribute("numEntries", numEntries);
        }
        Files.deleteIfExists(file);
    }

    @Override
    public void close() throws IOException {
        complete();
        Files.deleteIfExists(file);
    }
}
