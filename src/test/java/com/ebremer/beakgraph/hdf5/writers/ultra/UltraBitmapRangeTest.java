package com.ebremer.beakgraph.hdf5.writers.ultra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * BG-114: the index padding is a word-wise range fill now; it must set
 * exactly the bits the per-bit loop set, across word boundaries and at the
 * edges, and compose with concurrent single-bit ORs on the edge words.
 */
class UltraBitmapRangeTest {

    private static void assertSameWords(UltraBitmap expected, UltraBitmap actual, String what) {
        assertEquals(expected.numWords(), actual.numWords());
        for (int w = 0; w < expected.numWords(); w++) {
            assertEquals(expected.word(w), actual.word(w), what + ": word " + w);
        }
    }

    @Test
    void rangeFillEqualsPerBitSet() {
        Random rnd = new Random(114);
        for (int round = 0; round < 500; round++) {
            long bits = 1 + rnd.nextInt(400);
            long from = rnd.nextLong(bits + 1);
            long to = from + rnd.nextLong(bits + 1 - from);
            UltraBitmap perBit = new UltraBitmap("a", bits);
            UltraBitmap ranged = new UltraBitmap("b", bits);
            for (long i = from; i < to; i++) {
                perBit.set(i);
            }
            ranged.setRange(from, to);
            assertSameWords(perBit, ranged, "[" + from + ", " + to + ") of " + bits);
        }
    }

    @Test
    void edgesAndNeighboursCompose() {
        UltraBitmap b = new UltraBitmap("edges", 200);
        b.set(0);
        b.set(199);
        b.setRange(63, 129);          // straddles two word boundaries
        b.setRange(5, 5);             // empty
        b.set(64);                    // already set by the range: idempotent
        UltraBitmap expected = new UltraBitmap("expected", 200);
        expected.set(0);
        expected.set(199);
        for (long i = 63; i < 129; i++) {
            expected.set(i);
        }
        assertSameWords(expected, b, "edges");
        b.setRange(0, 200);
        for (int w = 0; w < 3; w++) {
            assertEquals(-1L, b.word(w), "full words");
        }
        assertEquals(-1L << (64 - 8), b.word(3), "the last partial word: bits 192..199");
        assertThrows(IndexOutOfBoundsException.class, () -> b.setRange(0, 201));
        assertThrows(IndexOutOfBoundsException.class, () -> b.setRange(10, 9));
    }
}
