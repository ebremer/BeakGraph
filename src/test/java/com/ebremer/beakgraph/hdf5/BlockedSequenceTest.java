package com.ebremer.beakgraph.hdf5;

import com.ebremer.beakgraph.io.ByteBufferBytes;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.SplittableRandom;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

/**
 * The block-structured form of a sequence (SPECIFICATIONS.md §5.1a): what
 * {@link BlockedEncoder} writes reads back entry for entry, one at a time and
 * in bulk, and a search in a sorted run answers exactly as a search of the
 * same values in a plain array - whatever the widths, wherever the run begins
 * and ends against the block boundaries, and whether or not the value is
 * there. A buffer whose directory and bytes disagree is refused when opened.
 */
class BlockedSequenceTest {

    private record Encoded(byte[] data, BlockedEncoder.Layout layout) {
        BitPackedUnSignedLongBuffer view() {
            return BitPackedUnSignedLongBuffer.readBlocked(new ByteBufferBytes(ByteBuffer.wrap(data)),
                    new ByteBufferBytes(ByteBuffer.wrap(layout.directory)), layout.numEntries, layout.blockEntries, layout.maxWidth);
        }
    }

    private static Encoded encode(long[] values) throws Exception {
        int[] at = {0};
        BlockedEncoder.Layout layout = BlockedEncoder.layout(values.length, () -> values[at[0]++]);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        at[0] = 0;
        BlockedEncoder.encode(layout, () -> values[at[0]++], out::write);
        assertEquals(layout.dataBytes, out.size(), "the layout names the bytes the encoder writes");
        assertEquals((values.length + layout.blockEntries - 1) / layout.blockEntries * BlockedEncoder.DIRECTORY_ENTRY_BYTES,
                layout.directory.length);
        return new Encoded(out.toByteArray(), layout);
    }

    private static void assertReadsBack(long[] values) throws Exception {
        BitPackedUnSignedLongBuffer view = encode(values).view();
        assertTrue(view.isBlocked());
        assertEquals(values.length, view.getNumEntries());
        for (int i = 0; i < values.length; i++) {
            if (values[i] != view.get(i)) {
                assertEquals(values[i], view.get(i), "entry " + i);
            }
        }
        assertArrayEquals(values, view.stream().toArray(), "as a stream");
        // In bulk, from offsets and over lengths that fall across block boundaries.
        SplittableRandom random = new SplittableRandom(7);
        for (int t = 0; t < 200; t++) {
            int from = random.nextInt(values.length);
            int count = random.nextInt(Math.min(values.length - from, 5000) + 1);
            long[] dst = new long[count + 3];
            Arrays.fill(dst, -7);
            view.get(from, dst, 2, count);
            assertEquals(-7, dst[0]);
            assertEquals(-7, dst[1]);
            assertEquals(-7, dst[count + 2]);
            assertArrayEquals(Arrays.copyOfRange(values, from, from + count), Arrays.copyOfRange(dst, 2, 2 + count),
                    "bulk [" + from + ", " + (from + count) + ")");
        }
    }

    @Test
    void everyWidthReadsBack() throws Exception {
        SplittableRandom random = new SplittableRandom(1);
        for (int width : new int[]{1, 2, 7, 8, 13, 16, 24, 31, 32, 33, 40, 56, 57}) {
            long[] values = new long[5000];
            long base = random.nextLong(1L << 40);
            for (int i = 0; i < values.length; i++) {
                values[i] = base + random.nextLong(1L << width);
            }
            values[17] = base;                             // the spread is exactly the width
            values[18] = base + (1L << width) - 1;
            Encoded e = encode(values);
            assertEquals(width, e.layout.maxWidth, "a block is as wide as its spread: " + width);
            assertReadsBack(values);
        }
    }

    @Test
    void edgesOfTheEncoding() throws Exception {
        assertReadsBack(new long[]{42});                                    // one entry: width 0, no block bytes
        long[] constant = new long[3000];
        Arrays.fill(constant, 9_000_000_000L);
        Encoded c = encode(constant);
        assertEquals(0, c.layout.maxWidth);
        assertEquals(BlockedEncoder.TAIL_BYTES, c.data.length, "blocks of one value take no bytes");
        assertReadsBack(constant);
        // Exactly one block, exactly two, and one entry over.
        for (int n : new int[]{1023, 1024, 1025, 2048, 2049}) {
            long[] values = new long[n];
            for (int i = 0; i < n; i++) {
                values[i] = (i * 2654435761L) & 0xFFFFF;
            }
            assertReadsBack(values);
        }
        // A spread past 57 bits: the block stores its entries whole, and unsigned.
        long[] wide = new long[2500];
        SplittableRandom random = new SplittableRandom(3);
        for (int i = 0; i < wide.length; i++) {
            wide[i] = random.nextLong();
        }
        wide[5] = 0;
        wide[6] = -1L;
        Encoded w = encode(wide);
        assertEquals(64, w.layout.maxWidth);
        assertReadsBack(wide);
        // Mixed: a narrow block beside a wide one.
        long[] mixed = new long[4096];
        for (int i = 0; i < mixed.length; i++) {
            mixed[i] = (i < 1024) ? 5 : (i < 2048) ? i : (i < 3072) ? (1L << 50) + i * 31L : wide[i - 3072];
        }
        assertReadsBack(mixed);
    }

    /** The answers of the three searches on a plain array: what any implementation must return. */
    private static long binarySearch(long[] v, long start, long end, long value) {
        long low = start;
        long high = end;
        while (low <= high) {
            long mid = (low + high) >>> 1;
            int cmp = Long.compareUnsigned(v[(int) mid], value);
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return -(low + 1);
    }

    private static long lowerBound(long[] v, long start, long end, long value) {
        for (long i = start; i <= end; i++) {
            if (Long.compareUnsigned(v[(int) i], value) >= 0) {
                return i;
            }
        }
        return -1;
    }

    private static long upperBound(long[] v, long start, long end, long value) {
        for (long i = end; i >= start; i--) {
            if (Long.compareUnsigned(v[(int) i], value) <= 0) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Runs as the indexes hold them: ascending, distinct, one after another, each
     * restarting low - so a block that straddles two runs has a base that belongs
     * to neither search. Runs of every length class: within a block, across one
     * boundary, across many blocks.
     */
    @Test
    void searchesInSortedRunsAnswerAsAPlainArrayDoes() throws Exception {
        SplittableRandom random = new SplittableRandom(11);
        int n = 60_000;
        long[] values = new long[n];
        int[] runStart = new int[n];
        int[] runEnd = new int[n];
        int runs = 0;
        for (int at = 0; at < n; ) {
            int kind = random.nextInt(10);
            int length = (kind < 6) ? 1 + random.nextInt(40) : (kind < 9) ? 500 + random.nextInt(2500) : 4000 + random.nextInt(9000);
            length = Math.min(length, n - at);
            long v = random.nextInt(50);
            long step = 1 + random.nextInt(kind < 6 ? 1000 : 6);
            for (int i = 0; i < length; i++) {
                v += 1 + random.nextLong(step);
                values[at + i] = v;
            }
            runStart[runs] = at;
            runEnd[runs] = at + length - 1;
            runs++;
            at += length;
        }
        BitPackedUnSignedLongBuffer view = encode(values).view();
        for (int r = 0; r < runs; r++) {
            int s = runStart[r];
            int e = runEnd[r];
            // Every value of a short run, a sample of a long one, their neighbours (absent or not), and both extremes.
            int stride = Math.max(1, (e - s + 1) / 60);
            for (int i = s; i <= e; i += stride) {
                for (long probe : new long[]{values[i] - 1, values[i], values[i] + 1}) {
                    check(view, values, s, e, probe);
                }
            }
            check(view, values, s, e, 0);
            check(view, values, s, e, values[s]);
            check(view, values, s, e, values[e]);
            check(view, values, s, e, values[e] + 1);
            check(view, values, s, e, Long.MAX_VALUE);
            // A search of part of a run, as an iterator resumes one.
            if (e - s > 3000) {
                int from = s + 1 + random.nextInt(e - s - 1);
                check(view, values, from, e, values[from + random.nextInt(e - from + 1)]);
                check(view, values, from, e, values[from] - 1);
                check(view, values, s, from, values[from] + 1);
            }
        }
    }

    private static void check(BitPackedUnSignedLongBuffer view, long[] values, long s, long e, long probe) {
        String where = "run [" + s + ", " + e + "], value " + probe;
        assertEquals(binarySearch(values, s, e, probe), view.binarySearch(s, e, probe), "binarySearch in " + where);
        assertEquals(lowerBound(values, s, e, probe), view.lowerBound(s, e, probe), "lowerBound in " + where);
        assertEquals(upperBound(values, s, e, probe), view.upperBound(s, e, probe), "upperBound in " + where);
    }

    @Test
    void aDirectoryThatDisagreesWithTheBytesIsRefused() throws Exception {
        long[] values = new long[3000];
        for (int i = 0; i < values.length; i++) {
            values[i] = i * 3L;
        }
        Encoded e = encode(values);
        ByteBufferBytes data = new ByteBufferBytes(ByteBuffer.wrap(e.data));
        ByteBufferBytes directory = new ByteBufferBytes(ByteBuffer.wrap(e.layout.directory));
        int k = e.layout.blockEntries;
        int w = e.layout.maxWidth;
        // More entries than the directory covers, a directory cut short, data cut short.
        IllegalStateException tooMany = assertThrows(IllegalStateException.class,
                () -> BitPackedUnSignedLongBuffer.readBlocked(data, directory, values.length + 2000L, k, w));
        assertTrue(tooMany.getMessage().contains("bytes are stored"), tooMany.getMessage());
        assertThrows(IllegalStateException.class, () -> BitPackedUnSignedLongBuffer.readBlocked(data,
                new ByteBufferBytes(ByteBuffer.wrap(Arrays.copyOf(e.layout.directory, e.layout.directory.length - 9))), values.length, k, w));
        IllegalStateException cut = assertThrows(IllegalStateException.class, () -> BitPackedUnSignedLongBuffer.readBlocked(
                new ByteBufferBytes(ByteBuffer.wrap(Arrays.copyOf(e.data, e.data.length - 16))), directory, values.length, k, w));
        assertTrue(cut.getMessage().contains("bytes are stored"), cut.getMessage());
        // A width no block may have, one above what the dataset declares, and a block size that is no power of two.
        byte[] badWidth = e.layout.directory.clone();
        badWidth[8] = 60;
        assertThrows(IllegalStateException.class, () -> BitPackedUnSignedLongBuffer.readBlocked(data,
                new ByteBufferBytes(ByteBuffer.wrap(badWidth)), values.length, k, 64));
        assertThrows(IllegalStateException.class, () -> BitPackedUnSignedLongBuffer.readBlocked(data, directory, values.length, k, w - 1));
        assertThrows(IllegalStateException.class, () -> BitPackedUnSignedLongBuffer.readBlocked(data, directory, values.length, 1000, w));
        assertThrows(IllegalStateException.class, () -> BitPackedUnSignedLongBuffer.readBlocked(data, directory, 0, k, w));
        // A bitmap's calls are not a sequence's.
        BitPackedUnSignedLongBuffer view = e.view();
        assertThrows(UnsupportedOperationException.class, () -> view.select1(1));
        assertThrows(IndexOutOfBoundsException.class, () -> view.get(values.length));
    }

    @Test
    void blocksGrowWithTheSequenceSoTheDirectoryStaysSmall() {
        assertEquals(1024, BlockedEncoder.blockEntriesFor(1));
        assertEquals(1024, BlockedEncoder.blockEntriesFor(1024L * BlockedEncoder.MAX_BLOCKS));
        assertEquals(4096, BlockedEncoder.blockEntriesFor(1024L * BlockedEncoder.MAX_BLOCKS + 1));
        assertEquals(4096, BlockedEncoder.blockEntriesFor(10_000_000_000L));
        assertEquals(16384, BlockedEncoder.blockEntriesFor(60_000_000_000L));
    }
}
