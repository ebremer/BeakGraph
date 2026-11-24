package com.ebremer.beakgraph.hdf5;

import com.ebremer.beakgraph.Params;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.LongUnaryOperator;

/**
 * Writes a sequence of unsigned integers in its block-structured form
 * (SPECIFICATIONS.md §5.1a), the form of the index id sequences since format
 * v6. The sequence is cut into blocks of the same number of entries; each
 * block stores its entries less the smallest of them (its base), packed
 * MSB-first in as many bits as the largest difference needs, and padded to a
 * whole number of 64-bit words. A directory holds each block's base and width.
 * (A block whose differences need more than 57 bits stores its entries whole,
 * in 64 bits each.)
 *
 * <p>What that buys a reader ({@link BitPackedUnSignedLongBuffer#readBlocked}):
 * the directory is a few bytes per thousand entries and is kept on the heap,
 * so an entry is still one read (directory, then its bits), a search in a long
 * sorted run is narrowed to one block without touching the file, a block
 * decodes into a {@code long[]} from whole words, and a run of nearby ids
 * costs the bits of their spread, not of the largest id in the store.
 *
 * <p>Every engine writes through this class, so the same sequence is the same
 * bytes whichever engine built the store. The encoding is two passes over the
 * values: {@link #layout} sizes the blocks (a streamed dataset is created with
 * its length), {@link #encode} writes them.
 *
 * @author Erich Bremer
 */
public final class BlockedEncoder {

    /** A directory entry: the block's base, 8 bytes big-endian, then its width, one byte. */
    public static final int DIRECTORY_ENTRY_BYTES = 9;
    /** Zero bytes after the last block: with them any entry is fetched by one 8-byte read. */
    public static final int TAIL_BYTES = 8;
    /** The smallest block: entries per block are this, times 4 until the directory fits {@link #MAX_BLOCKS}. */
    public static final int MIN_BLOCK_ENTRIES = 1024;
    /** A reader keeps 17 bytes of directory per block on the heap: at most 68 MiB for a sequence. */
    public static final long MAX_BLOCKS = 1L << 22;
    /** The widest block that is packed; a block that needs more stores its entries whole, in 64 bits. */
    public static final int MAX_PACKED_WIDTH = 57;

    /** The values of a sequence, in order. */
    @FunctionalInterface
    public interface Values {
        long next() throws IOException;
    }

    /** Where the encoded bytes go, in order. */
    @FunctionalInterface
    public interface Sink {
        void write(byte[] buf, int off, int len) throws IOException;
    }

    /** The shape of an encoded sequence: what {@link #layout} learns in the first pass. */
    public static final class Layout {
        public final long numEntries;
        public final int blockEntries;
        /** {@link #DIRECTORY_ENTRY_BYTES} per block. */
        public final byte[] directory;
        /** The bytes {@link #encode} writes: every block and the tail. */
        public final long dataBytes;
        /** The width of the widest block. */
        public final int maxWidth;

        Layout(long numEntries, int blockEntries, byte[] directory, long dataBytes, int maxWidth) {
            this.numEntries = numEntries;
            this.blockEntries = blockEntries;
            this.directory = directory;
            this.dataBytes = dataBytes;
            this.maxWidth = maxWidth;
        }
    }

    private BlockedEncoder() {}

    /** Entries per block for a sequence of {@code numEntries}: a function of the count alone, so every engine agrees. */
    public static int blockEntriesFor(long numEntries) {
        long k = MIN_BLOCK_ENTRIES;
        while ((numEntries + k - 1) / k > MAX_BLOCKS) {
            k <<= 2;
        }
        return Math.toIntExact(k);
    }

    /** The bytes a block of {@code count} entries of {@code width} bits takes: whole 64-bit words. */
    public static long blockBytes(long count, int width) {
        return ((count * width + 63) >>> 6) << 3;
    }

    /** The first pass: every block's base and width. {@code values} yields the {@code numEntries} values once. */
    public static Layout layout(long numEntries, Values values) throws IOException {
        if (numEntries <= 0) {
            throw new IllegalArgumentException("a block-structured sequence holds at least one entry: " + numEntries);
        }
        int k = blockEntriesFor(numEntries);
        int blocks = Math.toIntExact((numEntries + k - 1) / k);
        byte[] directory = new byte[Math.multiplyExact(blocks, DIRECTORY_ENTRY_BYTES)];
        long bytes = 0;
        int maxWidth = 0;
        long remaining = numEntries;
        for (int b = 0; b < blocks; b++) {
            int count = (int) Math.min(k, remaining);
            long min = values.next();
            long max = min;
            for (int i = 1; i < count; i++) {
                long v = values.next();
                if (Long.compareUnsigned(v, min) < 0) {
                    min = v;
                }
                if (Long.compareUnsigned(v, max) > 0) {
                    max = v;
                }
            }
            long spread = max - min;
            int width = (spread == 0) ? 0 : 64 - Long.numberOfLeadingZeros(spread);
            if (width > MAX_PACKED_WIDTH) {
                width = 64;     // entries stored whole; the base stays the block's smallest entry, for searches
            }
            int at = b * DIRECTORY_ENTRY_BYTES;
            for (int i = 0; i < 8; i++) {
                directory[at + i] = (byte) (min >>> (56 - 8 * i));
            }
            directory[at + 8] = (byte) width;
            bytes += blockBytes(count, width);
            maxWidth = Math.max(maxWidth, width);
            remaining -= count;
        }
        return new Layout(numEntries, k, directory, bytes + TAIL_BYTES, maxWidth);
    }

    /** The second pass: the blocks, then the tail. {@code values} yields the same values again. */
    public static void encode(Layout layout, Values values, Sink sink) throws IOException {
        int k = layout.blockEntries;
        long[] words = new long[k];                       // a block is at most k words (width 64)
        byte[] bytes = new byte[Math.min(k, 1 << 13) * 8];
        long remaining = layout.numEntries;
        int blocks = layout.directory.length / DIRECTORY_ENTRY_BYTES;
        for (int b = 0; b < blocks; b++) {
            int count = (int) Math.min(k, remaining);
            int at = b * DIRECTORY_ENTRY_BYTES;
            long base = 0;
            for (int i = 0; i < 8; i++) {
                base = (base << 8) | (layout.directory[at + i] & 0xFFL);
            }
            int width = layout.directory[at + 8] & 0xFF;
            int used = (int) (blockBytes(count, width) >>> 3);
            java.util.Arrays.fill(words, 0, used, 0L);
            long mask = (width == 64) ? -1L : (1L << width) - 1;
            for (int i = 0; i < count; i++) {
                long v = values.next();
                if (width == 0) {
                    if (v != base) {
                        throw new IllegalStateException("the sequence changed between the two passes (entry " + (layout.numEntries - remaining + i) + ")");
                    }
                    continue;
                }
                long d = (width == 64) ? v : v - base;
                if ((d & ~mask) != 0 || Long.compareUnsigned(v, base) < 0) {
                    throw new IllegalStateException("the sequence changed between the two passes (entry " + (layout.numEntries - remaining + i) + ")");
                }
                long bit = (long) i * width;
                int word = (int) (bit >>> 6);
                int offset = (int) (bit & 63);
                if (offset + width <= 64) {
                    words[word] |= d << (64 - offset - width);
                } else {
                    int over = offset + width - 64;
                    words[word] |= d >>> over;
                    words[word + 1] |= d << (64 - over);
                }
            }
            for (int w = 0; w < used; ) {
                int n = Math.min(used - w, bytes.length >>> 3);
                for (int j = 0; j < n; j++) {
                    long x = words[w + j];
                    int o = j << 3;
                    bytes[o] = (byte) (x >>> 56);
                    bytes[o + 1] = (byte) (x >>> 48);
                    bytes[o + 2] = (byte) (x >>> 40);
                    bytes[o + 3] = (byte) (x >>> 32);
                    bytes[o + 4] = (byte) (x >>> 24);
                    bytes[o + 5] = (byte) (x >>> 16);
                    bytes[o + 6] = (byte) (x >>> 8);
                    bytes[o + 7] = (byte) x;
                }
                sink.write(bytes, 0, n << 3);
                w += n;
            }
            remaining -= count;
        }
        sink.write(new byte[TAIL_BYTES], 0, TAIL_BYTES);
    }

    /**
     * Adds a sequence held in memory to {@code group}: the dataset {@code name}
     * (the blocks) with its {@code numEntries}, {@code width} (the widest
     * block's) and {@code blockEntries} attributes, and the dataset
     * {@code name + "_blocks"} (the directory). An empty sequence adds nothing,
     * as an empty bit-packed buffer adds none.
     *
     * @param at the value at an index, asked for every index in order, twice
     * @return the dataset {@code name}, for further attributes; null when nothing was added
     */
    public static WritableDataset add(WritableGroup group, String name, long numEntries, LongUnaryOperator at) {
        if (numEntries == 0) {
            return null;
        }
        try {
            Layout layout = layout(numEntries, counting(at));
            if (layout.dataBytes > Integer.MAX_VALUE - 16) {
                // The ceiling every in-memory dataset has; sources beyond it need a disk-based engine.
                throw new IllegalArgumentException("Dataset '" + name + "' needs " + layout.dataBytes
                        + " bytes; too large for an in-memory writer");
            }
            ByteArrayOutputStream data = new ByteArrayOutputStream((int) layout.dataBytes);
            encode(layout, counting(at), data::write);
            WritableDataset ds = group.putDataset(name, data.toByteArray());
            ds.putAttribute(Params.WIDTH, layout.maxWidth);
            ds.putAttribute(Params.NUM_ENTRIES, numEntries);
            ds.putAttribute(Params.BLOCK_ENTRIES, layout.blockEntries);
            group.putDataset(name + Params.BLOCKS_SUFFIX, layout.directory);
            return ds;
        } catch (IOException e) {
            throw new UncheckedIOException(e);      // nothing here does I/O: the values and the sink are in memory
        }
    }

    private static Values counting(LongUnaryOperator at) {
        long[] next = {0};
        return () -> at.applyAsLong(next[0]++);
    }
}
