package com.ebremer.beakgraph;

import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
}
