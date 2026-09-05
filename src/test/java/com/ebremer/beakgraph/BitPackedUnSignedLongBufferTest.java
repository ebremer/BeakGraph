package com.ebremer.beakgraph;

import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the bit-packed buffer, using the in-memory (null-buffer) write path.
 * Covers the supported width range, the rejection of the unsafe 58..63 widths (which would
 * silently drop high bits in the pack/unpack accumulators), and the 1-based contract of select1.
 */
class BitPackedUnSignedLongBufferTest {

    @Test
    void roundTripsAcrossSupportedWidths() {
        int[] widths = {1, 2, 7, 8, 13, 31, 32, 40, 56, 57, 64};
        for (int w : widths) {
            BitPackedUnSignedLongBuffer buf = new BitPackedUnSignedLongBuffer(null, null, 0, w);
            long mask = (w == 64) ? -1L : (1L << w) - 1;
            int n = 100;
            long[] vals = new long[n];
            for (int i = 0; i < n; i++) {
                long v = (0x9E3779B97F4A7C15L * (i + 1)) & mask; // spreads bits, including high ones
                vals[i] = v;
                buf.writeLong(v);
            }
            buf.prepareForReading();
            for (int i = 0; i < n; i++) {
                assertEquals(vals[i], buf.get(i), "get() width=" + w + " index=" + i);
            }
            assertArrayEquals(vals, buf.stream().toArray(), "stream() width=" + w);
        }
    }

    @Test
    void streamFailsLoudlyOnTruncatedBuffer() {
        // A buffer shorter than its declared entry count is corrupt. The stream
        // used to emit silent garbage (negative-shift artifacts) where the
        // sequential reader threw; both must now fail loudly.
        java.nio.ByteBuffer twoBytes = java.nio.ByteBuffer.allocate(2);
        BitPackedUnSignedLongBuffer truncated = new BitPackedUnSignedLongBuffer(null, twoBytes, 10, 16);
        assertThrows(java.nio.BufferUnderflowException.class, () -> truncated.stream().toArray());
    }

    @Test
    void rejectsValuesThatWouldBeTruncated() {
        // A value wider than the buffer's bit width used to be silently masked,
        // corrupting the dictionary far from the cause. It must be rejected.
        BitPackedUnSignedLongBuffer narrow = new BitPackedUnSignedLongBuffer(null, null, 0, 8);
        assertDoesNotThrow(() -> narrow.writeLong(255));
        assertThrows(IllegalArgumentException.class, () -> narrow.writeLong(256));
        assertThrows(IllegalArgumentException.class, () -> narrow.writeLong(-1));
        assertThrows(IllegalArgumentException.class, () -> narrow.writeInteger(256));

        // Widths 32 and 64 legitimately carry full two's-complement patterns
        // (the int/long literal buffers): negatives must stay writable there.
        BitPackedUnSignedLongBuffer w32 = new BitPackedUnSignedLongBuffer(null, null, 0, 32);
        assertDoesNotThrow(() -> w32.writeInteger(-5));
        BitPackedUnSignedLongBuffer w64 = new BitPackedUnSignedLongBuffer(null, null, 0, 64);
        assertDoesNotThrow(() -> w64.writeLong(Long.MIN_VALUE));
    }

    @Test
    void rejectsUnsupportedBitWidths() {
        // 58..63 would overflow the 64-bit accumulator (value + <=7-bit sub-byte offset) and silently
        // corrupt data, so they are rejected rather than written.
        for (int w = 58; w <= 63; w++) {
            final int width = w;
            assertThrows(IllegalArgumentException.class,
                () -> new BitPackedUnSignedLongBuffer(null, null, 0, width),
                "width " + w + " must be rejected");
        }
        for (int w : new int[]{0, -1, 65, 1000}) {
            final int width = w;
            assertThrows(IllegalArgumentException.class,
                () -> new BitPackedUnSignedLongBuffer(null, null, 0, width));
        }
        // The boundary supported widths must still construct.
        assertDoesNotThrow(() -> new BitPackedUnSignedLongBuffer(null, null, 0, 57));
        assertDoesNotThrow(() -> new BitPackedUnSignedLongBuffer(null, null, 0, 64));
    }

    @Test
    void select1RejectsRankBelowOne() {
        BitPackedUnSignedLongBuffer bm = new BitPackedUnSignedLongBuffer(null, null, 0, 1);
        int[] bits = {0, 1, 0, 1, 1}; // set bits at indices 1, 3, 4
        for (int b : bits) bm.writeInteger(b);
        bm.prepareForReading();
        assertEquals(-1, bm.select1(0),  "rank 0 is not a valid 1-based select");
        assertEquals(-1, bm.select1(-7), "a negative rank is invalid");
        assertEquals(1, bm.select1(1));
        assertEquals(3, bm.select1(2));
        assertEquals(4, bm.select1(3));
        assertEquals(-1, bm.select1(4),  "only 3 set bits exist");
    }

    // --- BG-92: the primitives the iterators navigate with ----------------

    /** A 1-bit bitmap from a boolean oracle, through the in-memory write path. */
    private static BitPackedUnSignedLongBuffer bitmap(boolean[] bits) {
        BitPackedUnSignedLongBuffer bm = new BitPackedUnSignedLongBuffer(null, null, 0, 1);
        for (boolean b : bits) bm.writeInteger(b ? 1 : 0);
        bm.prepareForReading();
        return bm;
    }

    private static boolean[][] patterns(int n, java.util.Random rnd) {
        boolean[] zeros = new boolean[n];
        boolean[] ones = new boolean[n];
        java.util.Arrays.fill(ones, true);
        boolean[] last = new boolean[n];
        last[n - 1] = true;
        boolean[] tail = new boolean[n];              // ones only in the last partial word
        for (int i = (n - 1) & ~63; i < n; i++) tail[i] = true;
        boolean[] random = new boolean[n];
        for (int i = 0; i < n; i++) random[i] = rnd.nextInt(5) == 0;
        boolean[] sparse = new boolean[n];
        for (int i = 0; i < n; i += 61) sparse[i] = true;
        return new boolean[][]{zeros, ones, last, tail, random, sparse};
    }

    /** The documented contract of nextSetBit, spelled out against the oracle. */
    private static long expectedNextSetBit(boolean[] bits, int from, long maxWords) {
        int n = bits.length;
        if (from >= n) return -1;
        long lastWord = (n - 1) >>> 6;
        long w = from >>> 6;
        long budgetLast = (maxWords >= Long.MAX_VALUE - w) ? Long.MAX_VALUE : w + maxWords - 1;
        for (int i = from; i < n; i++) {
            if (bits[i]) {
                return ((i >>> 6) <= budgetLast) ? i : BitPackedUnSignedLongBuffer.SCAN_EXHAUSTED;
            }
        }
        return (lastWord <= budgetLast) ? -1 : BitPackedUnSignedLongBuffer.SCAN_EXHAUSTED;
    }

    /** MSB-first 64-bit view starting at bit i, zero-padded past the end. */
    private static long expectedWord64(boolean[] bits, int i) {
        long w = 0;
        for (int k = 0; k < 64; k++) {
            w <<= 1;
            if (i + k < bits.length && bits[i + k]) w |= 1;
        }
        return w;
    }

    @Test
    void nextSetBitAndGetWord64MatchAnOracleAtEveryOffset() {
        java.util.Random rnd = new java.util.Random(92);
        for (int n = 1; n <= 200; n++) {
            for (boolean[] bits : patterns(n, rnd)) {
                BitPackedUnSignedLongBuffer bm = bitmap(bits);
                for (int from = 0; from <= n; from++) {
                    assertEquals(expectedNextSetBit(bits, from, Long.MAX_VALUE), bm.nextSetBit(from, Long.MAX_VALUE),
                            "n=" + n + " from=" + from + " unbounded");
                    for (long k = 1; k <= 3; k++) {
                        assertEquals(expectedNextSetBit(bits, from, k), bm.nextSetBit(from, k),
                                "n=" + n + " from=" + from + " maxWords=" + k);
                    }
                }
                for (int i = 0; i < n; i++) {
                    assertEquals(expectedWord64(bits, i), bm.getWord64(i), "n=" + n + " getWord64(" + i + ")");
                }
            }
        }
    }

    @Test
    void nextSetBitOnlyExistsForBitmaps() {
        BitPackedUnSignedLongBuffer wide = new BitPackedUnSignedLongBuffer(null, null, 0, 8);
        wide.writeInteger(1);
        wide.prepareForReading();
        assertThrows(UnsupportedOperationException.class, () -> wide.nextSetBit(0, 1));
        assertThrows(UnsupportedOperationException.class, wide::bitReader);
    }

    @Test
    void bitReaderSurvivesBackwardSeeksAndRandomJumps() {
        int n = 1000;
        java.util.Random rnd = new java.util.Random(93);
        boolean[] bits = new boolean[n];
        for (int i = 0; i < n; i++) bits[i] = rnd.nextBoolean();
        BitPackedUnSignedLongBuffer.BitReader reader = bitmap(bits).bitReader();
        for (int i = 0; i < n; i++) assertEquals(bits[i], reader.bit(i), "forward " + i);
        // backward across word boundaries
        for (int i = 130; i >= 60; i--) assertEquals(bits[i], reader.bit(i), "backward " + i);
        // straddling a boundary repeatedly must refresh the cached word each time
        for (int r = 0; r < 20; r++) {
            assertEquals(bits[63], reader.bit(63));
            assertEquals(bits[64], reader.bit(64));
        }
        for (int r = 0; r < 5_000; r++) {
            int i = rnd.nextInt(n);
            assertEquals(bits[i], reader.bit(i), "random " + i);
        }
    }

    private static BitPackedUnSignedLongBuffer packed(int width, long[] values) {
        BitPackedUnSignedLongBuffer buf = new BitPackedUnSignedLongBuffer(null, null, 0, width);
        for (long v : values) buf.writeLong(v);
        buf.prepareForReading();
        return buf;
    }

    private static long oracleLowerBound(long[] a, int start, int end, long v) {
        for (int i = start; i <= end; i++) if (Long.compareUnsigned(a[i], v) >= 0) return i;
        return -1;
    }

    private static long oracleUpperBound(long[] a, int start, int end, long v) {
        for (int i = end; i >= start; i--) if (Long.compareUnsigned(a[i], v) <= 0) return i;
        return -1;
    }

    private static long oracleBinarySearch(long[] a, int start, int end, long v) {
        int low = start;
        for (int i = start; i <= end; i++) {
            int c = Long.compareUnsigned(a[i], v);
            if (c == 0) return i;
            if (c < 0) low = i + 1;
        }
        return -(low + 1);
    }

    @Test
    void boundsAndBinarySearchAreUnsignedAndEncodeInsertionPoints() {
        for (int width : new int[]{8, 57, 64}) {
            long mask = (width == 64) ? -1L : (1L << width) - 1;
            java.util.TreeSet<Long> set = new java.util.TreeSet<>(Long::compareUnsigned);
            java.util.Random rnd = new java.util.Random(width);
            for (int i = 0; i < 40; i++) set.add(rnd.nextLong() & mask);
            set.add(0L);
            set.add(mask);                     // the largest unsigned value the width holds
            if (width == 64) {
                set.add(Long.MIN_VALUE);       // top bit set: signed order would misplace these
                set.add(-1L);
                set.add(Long.MAX_VALUE);
            }
            long[] sorted = set.stream().mapToLong(Long::longValue).toArray();
            long[] withDuplicates = new long[sorted.length + 3];
            System.arraycopy(sorted, 0, withDuplicates, 0, sorted.length);
            // a duplicate run at the end keeps the array sorted and exercises ties
            java.util.Arrays.fill(withDuplicates, sorted.length, withDuplicates.length, sorted[sorted.length - 1]);
            long[] a = withDuplicates;
            BitPackedUnSignedLongBuffer buf = packed(width, a);

            java.util.List<Long> probes = new java.util.ArrayList<>();
            for (long v : a) {
                probes.add(v);
                probes.add(v + 1);         // gaps and, for the max value, the wrap-around
                probes.add(v - 1);
            }
            probes.add(mask);
            probes.add(0L);
            if (width == 64) { probes.add(Long.MIN_VALUE + 1); probes.add(-2L); }
            int[][] ranges = {{0, a.length - 1}, {0, 0}, {a.length - 1, a.length - 1}, {3, a.length - 4}, {5, 4}};
            for (int[] r : ranges) {
                for (long v : probes) {
                    if (width != 64) v &= mask;
                    String at = "width=" + width + " range=[" + r[0] + "," + r[1] + "] value=" + Long.toUnsignedString(v);
                    assertEquals(oracleLowerBound(a, r[0], r[1], v), buf.lowerBound(r[0], r[1], v), "lowerBound " + at);
                    assertEquals(oracleUpperBound(a, r[0], r[1], v), buf.upperBound(r[0], r[1], v), "upperBound " + at);
                    long found = buf.binarySearch(r[0], r[1], v);
                    long oracle = oracleBinarySearch(a, r[0], r[1], v);
                    if (found >= 0) {
                        assertEquals(v, a[(int) found], "binarySearch hit " + at);
                        assertTrue(oracle >= 0, "binarySearch found a value the oracle says is absent " + at);
                    } else {
                        assertEquals(oracle, found, "binarySearch insertion point " + at);
                    }
                }
            }
        }
    }
}
