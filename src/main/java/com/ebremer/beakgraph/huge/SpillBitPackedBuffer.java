package com.ebremer.beakgraph.huge;

import com.ebremer.beakgraph.Params;

import com.ebremer.beakgraph.hdf5.BlockedEncoder;
import com.ebremer.beakgraph.hdf5.DictionarySinks;
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
final class SpillBitPackedBuffer implements AutoCloseable, DictionarySinks.LongSink {

    private final Path file;
    private final OutputStream out;
    private final int bitWidth;
    private long writeAccumulator = 0L;
    private int writeAccumulatorCount = 0;
    private long numEntries = 0;
    private long bytesWritten = 0;
    private boolean completed = false;
    // Packed bytes are staged and written a chunk at a time: the buffered
    // stream's write(int) takes its lock per byte (BG-250).
    private final byte[] stage = new byte[1 << 16];
    private int staged = 0;

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

    public void writeInteger(int value) {
        if (bitWidth != 32 && bitWidth != 64 && (value < 0 || value > ((1L << bitWidth) - 1))) {
            throw new IllegalArgumentException("Value " + value + " does not fit in " + bitWidth + " bits");
        }
        putValue(value & ((bitWidth == 64) ? -1L : (1L << bitWidth) - 1));
        numEntries++;
    }

    public void writeLong(long value) {
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
                stageByte((byte) (writeAccumulator >>> shift));
                writeAccumulator &= (1L << shift) - 1;
                writeAccumulatorCount -= 8;
            }
        } catch (IOException ex) {
            throw new UncheckedIOException("Failed writing spill buffer " + file, ex);
        }
    }

    private void stageByte(byte b) throws IOException {
        stage[staged++] = b;
        bytesWritten++;
        if (staged == stage.length) {
            flushStage();
        }
    }

    private void flushStage() throws IOException {
        if (staged > 0) {
            out.write(stage, 0, staged);
            staged = 0;
        }
    }

    /** Flushes the trailing partial byte and closes the temp file for writing. */
    void complete() throws IOException {
        if (completed) return;
        completed = true;
        if (writeAccumulatorCount > 0) {
            stageByte((byte) (writeAccumulator << (8 - writeAccumulatorCount)));
            writeAccumulator = 0;
            writeAccumulatorCount = 0;
        }
        flushStage();
        out.close();
    }

    public long getNumEntries() {
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
            ds.putAttribute(Params.WIDTH, bitWidth);
            ds.putAttribute(Params.NUM_ENTRIES, numEntries);
        }
        Files.deleteIfExists(file);
    }

    /**
     * As {@link #transferTo}, in the block-structured form (SPECIFICATIONS.md §5.1a)
     * the index id sequences take: dataset {@code name} and its directory
     * {@code name + "_blocks"}, the bytes the in-memory writers write for the same
     * values. The temp file is read twice - once to size the blocks, once to write
     * them - a block's worth of values at a time, so the heap holds the directory
     * and a transfer buffer, whatever the sequence's length.
     */
    void transferBlockedTo(StreamingHdf5Group group, String name) throws IOException {
        transferBlockedTo(group, name, null, 0);
    }

    /**
     * As {@link #transferBlockedTo(StreamingHdf5Group, String)}, with one more
     * 64-bit attribute {@code extraName} on the dataset after the standard ones
     * (when not null): {@code sequenceElements}' {@code numSlots}, written in
     * the order the in-memory writer writes it.
     */
    void transferBlockedTo(StreamingHdf5Group group, String name, String extraName, long extraValue) throws IOException {
        complete();
        if (numEntries == 0) {
            Files.deleteIfExists(file);
            return;
        }
        BlockedEncoder.Layout layout;
        try (Unpacker values = new Unpacker()) {
            layout = BlockedEncoder.layout(numEntries, values);
        }
        try (StreamingHdf5Dataset ds = group.createByteDataset(name, layout.dataBytes); Unpacker values = new Unpacker()) {
            byte[] pending = new byte[HugeIO.TRANSFER_BUFFER_BYTES];
            int[] held = {0};
            BlockedEncoder.encode(layout, values, (buf, off, len) -> {
                while (len > 0) {
                    int n = Math.min(len, pending.length - held[0]);
                    System.arraycopy(buf, off, pending, held[0], n);
                    held[0] += n;
                    off += n;
                    len -= n;
                    if (held[0] == pending.length) {
                        ds.write(pending, 0, held[0]);
                        held[0] = 0;
                    }
                }
            });
            ds.write(pending, 0, held[0]);
            ds.putAttribute(Params.WIDTH, layout.maxWidth);
            ds.putAttribute(Params.NUM_ENTRIES, numEntries);
            ds.putAttribute(Params.BLOCK_ENTRIES, layout.blockEntries);
            if (extraName != null) {
                ds.putAttribute(extraName, extraValue);
            }
        }
        try (StreamingHdf5Dataset directory = group.createByteDataset(name + Params.BLOCKS_SUFFIX, layout.directory.length)) {
            directory.write(layout.directory);
        }
        Files.deleteIfExists(file);
    }

    /**
     * The values, in order, from the completed temp file, which stays until a transfer deletes it: the
     * disk engines' element index walks {@code sequenceStarts} beside the elements this way.
     */
    Unpacker values() throws IOException {
        complete();
        return new Unpacker();
    }

    /** The values of the completed temp file, in order: the inverse of {@link #putValue}. */
    final class Unpacker implements BlockedEncoder.Values, AutoCloseable {
        private final java.io.InputStream in;
        private long acc;
        private int accCount;

        Unpacker() throws IOException {
            this.in = new java.io.BufferedInputStream(Files.newInputStream(file), 1 << 20);
        }

        @Override
        public long next() throws IOException {
            while (accCount < bitWidth) {
                int b = in.read();
                if (b < 0) {
                    throw new java.io.EOFException("Spill buffer " + file + " ends before its " + numEntries + " entries");
                }
                acc = (acc << 8) | b;
                accCount += 8;
            }
            int shift = accCount - bitWidth;
            long value = acc >>> shift;
            acc &= (1L << shift) - 1;
            accCount -= bitWidth;
            return value;
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }

    @Override
    public void close() throws IOException {
        complete();
        Files.deleteIfExists(file);
    }
}
