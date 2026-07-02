package com.ebremer.beakgraph.io;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * The two RandomAccessBytes implementations must be observably identical:
 * every read (byte, big-endian long/float/double, bulk) at every offset -
 * including unaligned and boundary offsets - returns the same value, and
 * out-of-range offsets fail the same way. The FFM implementation is exercised
 * over a real file mapping, at offset zero and at a non-zero base offset (the
 * dataset-address case).
 */
class RandomAccessBytesParityTest {

    // NEVER + manual delete: auto-arena mappings unmap at GC, which on Windows
    // can outlive JUnit's cleanup and fail the run over a locked temp file.
    @TempDir(cleanup = CleanupMode.NEVER)
    static Path dir;

    @AfterAll
    static void tryCleanup() {
        try (var files = Files.walk(dir)) {
            files.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                p.toFile().deleteOnExit();
                try { Files.deleteIfExists(p); } catch (Exception ignored) {}
            });
        } catch (Exception ignored) {}
    }

    @Test
    void implementationsAgreeAtEveryOffset() throws Exception {
        Random rnd = new Random(99);
        byte[] data = new byte[64 * 1024 + 13]; // deliberately not power-of-two
        rnd.nextBytes(data);
        Path f = dir.resolve("parity.bin");
        Files.write(f, data);

        RandomAccessBytes bb = new ByteBufferBytes(ByteBuffer.wrap(data));
        RandomAccessBytes seg = MemorySegmentBytes.map(f, 0, data.length);

        assertEquals(data.length, bb.size());
        assertEquals(data.length, seg.size());

        // dense sweep near both ends plus random interior offsets
        for (int i = 0; i < 512; i++) {
            checkAt(bb, seg, i, data.length);
            checkAt(bb, seg, data.length - 1 - i, data.length);
        }
        for (int i = 0; i < 5_000; i++) {
            checkAt(bb, seg, rnd.nextLong(data.length), data.length);
        }

        // bulk reads, including a full-array read
        byte[] a = new byte[1024], b = new byte[1024];
        for (int i = 0; i < 200; i++) {
            long off = rnd.nextLong(data.length - a.length);
            bb.get(off, a, 0, a.length);
            seg.get(off, b, 0, b.length);
            assertArrayEquals(a, b, "bulk @" + off);
        }
        byte[] whole = new byte[data.length];
        seg.get(0, whole, 0, whole.length);
        assertArrayEquals(data, whole, "full bulk read through the segment");

        // identical failure mode out of range
        assertThrows(IndexOutOfBoundsException.class, () -> bb.get(data.length));
        assertThrows(IndexOutOfBoundsException.class, () -> seg.get(data.length));
        assertThrows(IndexOutOfBoundsException.class, () -> bb.getLong(data.length - 4));
        assertThrows(IndexOutOfBoundsException.class, () -> seg.getLong(data.length - 4));
        assertThrows(IndexOutOfBoundsException.class, () -> seg.get(-1));
    }

    @Test
    void nonZeroBaseOffsetMapsTheRightWindow() throws Exception {
        Random rnd = new Random(7);
        byte[] data = new byte[4096];
        rnd.nextBytes(data);
        Path f = dir.resolve("offset.bin");
        Files.write(f, data);

        int base = 1234; // like a dataset's file address
        RandomAccessBytes seg = MemorySegmentBytes.map(f, base, data.length - base);
        assertEquals(data.length - base, seg.size());
        for (int i = 0; i < seg.size(); i++) {
            assertEquals(data[base + i], seg.get(i), "byte @" + i);
        }
    }

    private static void checkAt(RandomAccessBytes bb, RandomAccessBytes seg, long off, int len) {
        if (off < 0 || off >= len) return;
        assertEquals(bb.get(off), seg.get(off), "byte @" + off);
        if (off + Long.BYTES <= len) {
            assertEquals(bb.getLong(off), seg.getLong(off), "long @" + off);
            assertEquals(bb.getDouble(off), seg.getDouble(off), "double @" + off);
        }
        if (off + Float.BYTES <= len) {
            assertEquals(bb.getFloat(off), seg.getFloat(off), "float @" + off);
        }
    }
}
