package com.ebremer.beakgraph.io;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * {@link SegmentedBytes}: an array in several mapped pieces reads as the one
 * array it is - every byte, every value that lies across two pieces, and
 * nothing past its end, though the last piece is stored longer than the array.
 */
class SegmentedBytesTest {

    /** {@code data} cut into pieces of {@code length} bytes, each stored whole: the last one padded with 0x55. */
    private static SegmentedBytes cut(Arena arena, byte[] data, int length, boolean views) {
        int count = (data.length + length - 1) / length;
        MemorySegment[] pieces = new MemorySegment[count];
        for (int i = 0; i < count; i++) {
            byte[] piece = new byte[length];
            Arrays.fill(piece, (byte) 0x55);
            System.arraycopy(data, i * length, piece, 0, Math.min(length, data.length - i * length));
            pieces[i] = arena.allocate(length);
            MemorySegment.copy(piece, 0, pieces[i], ValueLayout.JAVA_BYTE, 0, length);
        }
        return new SegmentedBytes(pieces, length, data.length, views);
    }

    /** Through the pieces' segments, and through ByteBuffer views of them (as the native binary reads). */
    private static final boolean[] BOTH = {false, true};

    @Test
    void everyReadIsThatOfTheWholeArray() {
        byte[] data = new byte[10_007];
        new Random(42).nextBytes(data);
        ByteBuffer whole = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
        try (Arena arena = Arena.ofConfined()) {
            // Pieces shorter than a long, of exactly one, odd, a power of two, and longer than the array.
            for (int length : new int[] {1, 3, 7, 8, 9, 64, 1000, 4096, 10_006, 10_007, 20_000}) {
              for (boolean views : BOTH) {
                SegmentedBytes bytes = cut(arena, data, length, views);
                String what = "pieces of " + length + (views ? " through views" : "");
                assertEquals(data.length, bytes.size(), what);
                assertFalse(bytes.isRemote(), what);
                for (int at = 0; at < data.length; at++) {
                    assertEquals(data[at], bytes.get(at), what + " at " + at);
                }
                for (int at = 0; at <= data.length - 8; at++) {
                    assertEquals(whole.getLong(at), bytes.getLong(at), what + " at " + at);
                    assertEquals(Double.doubleToRawLongBits(whole.getDouble(at)),
                            Double.doubleToRawLongBits(bytes.getDouble(at)), what + " at " + at);
                }
                for (int at = 0; at <= data.length - 4; at++) {
                    assertEquals(Float.floatToRawIntBits(whole.getFloat(at)), Float.floatToRawIntBits(bytes.getFloat(at)),
                            what + " at " + at);
                }
                Random random = new Random(length);
                for (int round = 0; round < 200; round++) {
                    int at = random.nextInt(data.length);
                    int n = random.nextInt(Math.min(3 * length + 9, data.length - at) + 1);
                    byte[] read = new byte[n + 6];
                    bytes.get(at, read, 3, n);
                    assertArrayEquals(Arrays.copyOfRange(data, at, at + n), Arrays.copyOfRange(read, 3, 3 + n), what);
                    assertEquals(0, read[0] | read[1] | read[2] | read[n + 3] | read[n + 4] | read[n + 5], what);
                }
                byte[] all = new byte[data.length];
                bytes.get(0, all, 0, all.length);
                assertArrayEquals(data, all, what);
              }
            }
        }
    }

    @Test
    void nothingIsReadPastTheArray() {
        byte[] data = new byte[1001];
        new Random(7).nextBytes(data);
        try (Arena arena = Arena.ofConfined()) {
            for (int length : new int[] {1, 8, 100, 1000, 1001, 5000}) {
              for (boolean views : BOTH) {
                // The last piece is stored whole: there are bytes after the array's end, and they are not the array's.
                SegmentedBytes bytes = cut(arena, data, length, views);
                String what = "pieces of " + length + (views ? " through views" : "");
                for (long at : new long[] {-1, Long.MIN_VALUE, data.length, data.length + 1, (long) length * 9999, Long.MAX_VALUE,
                        1L << 40, 1L << 62}) {
                    assertThrows(IndexOutOfBoundsException.class, () -> bytes.get(at), what + " at " + at);
                    assertThrows(IndexOutOfBoundsException.class, () -> bytes.getLong(at), what + " at " + at);
                    assertThrows(IndexOutOfBoundsException.class, () -> bytes.get(at, new byte[1], 0, 1), what + " at " + at);
                }
                for (int back = 1; back < 8; back++) {
                    long at = data.length - back;
                    assertThrows(IndexOutOfBoundsException.class, () -> bytes.getLong(at), what + ": a long " + back + " from the end");
                    assertThrows(IndexOutOfBoundsException.class, () -> bytes.getDouble(at), what);
                }
                for (int back = 1; back < 4; back++) {
                    long at = data.length - back;
                    assertThrows(IndexOutOfBoundsException.class, () -> bytes.getFloat(at), what);
                }
                assertThrows(IndexOutOfBoundsException.class, () -> bytes.get(data.length - 3, new byte[10], 0, 4), what);
                assertThrows(IndexOutOfBoundsException.class, () -> bytes.get(0, new byte[4], 2, 3), what);
                bytes.get(data.length, new byte[0], 0, 0); // nothing, at the end: allowed, as of any array
              }
            }
        }
    }

    @Test
    void aPieceIsFoundByItsOffsetHoweverFarIn() {
        // The same megabyte stands for every piece: an array of gigabytes without the memory, whose byte at an
        // offset is the piece's byte at offset mod length - so a piece found one off reads the wrong byte only at
        // the piece's edges, which is where each offset below lies.
        for (int length : new int[] {1_000_003, 1 << 20, 999_983}) {
          for (boolean views : BOTH) {
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment piece = arena.allocate(length);
                piece.set(ValueLayout.JAVA_BYTE, 0, (byte) 0x11);
                piece.set(ValueLayout.JAVA_BYTE, length - 1L, (byte) 0x77);
                int count = 9000; // over 8 GiB: offsets past an int, and past 2^32
                MemorySegment[] pieces = new MemorySegment[count];
                Arrays.fill(pieces, piece);
                long size = (long) count * length;
                SegmentedBytes bytes = new SegmentedBytes(pieces, length, size, views);
                for (int i = 0; i < count; i++) {
                    long start = (long) i * length;
                    assertEquals(0x11, bytes.get(start), "first byte of piece " + i + " of " + length);
                    assertEquals(0x77, bytes.get(start + length - 1), "last byte of piece " + i + " of " + length);
                    if (i > 0) {
                        // A long across the edge: the last byte of one piece, the first of the next, and six zeros.
                        assertEquals(0x7711_0000_0000_0000L, bytes.getLong(start - 1), "across the edge into piece " + i);
                    }
                }
                assertThrows(IndexOutOfBoundsException.class, () -> bytes.get(size));
            }
          }
        }
    }

    @Test
    void whatIsNotAnArrayInPiecesIsRefused() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment ten = arena.allocate(10);
            assertThrows(IllegalArgumentException.class, () -> new SegmentedBytes(new MemorySegment[] {ten}, 10, 11), "one piece short");
            assertThrows(IllegalArgumentException.class, () -> new SegmentedBytes(new MemorySegment[] {ten, ten, ten}, 10, 20), "one too many");
            assertThrows(IllegalArgumentException.class, () -> new SegmentedBytes(new MemorySegment[] {ten, arena.allocate(4)}, 10, 15),
                    "a last piece shorter than what is left of the array");
            assertThrows(IllegalArgumentException.class, () -> new SegmentedBytes(new MemorySegment[] {ten}, 0, 10));
            assertThrows(IllegalArgumentException.class, () -> new SegmentedBytes(new MemorySegment[0], 10, 0));
            assertEquals(15, new SegmentedBytes(new MemorySegment[] {ten, arena.allocate(5)}, 10, 15).size());
            assertThrows(IllegalArgumentException.class, () -> new SegmentedBytes(new MemorySegment[] {ten}, 1L << 31, 10, true),
                    "a piece no ByteBuffer holds");
        }
    }
}
