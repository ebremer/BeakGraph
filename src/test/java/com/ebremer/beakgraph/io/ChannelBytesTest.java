package com.ebremer.beakgraph.io;

import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link ChannelBytes} must be observably identical to the other
 * {@link RandomAccessBytes} implementations (see RandomAccessBytesParityTest
 * for the ByteBuffer/FFM pair): every read at every offset - unaligned and
 * boundary included - returns the same value, out-of-range offsets throw
 * IndexOutOfBoundsException, and the window (base + size) is enforced even
 * when the channel has more bytes. Per-thread scratch buffers make
 * concurrent primitive reads safe; that is exercised too.
 */
class ChannelBytesTest {

    @TempDir
    static Path dir;

    @Test
    void agreesWithByteBufferBytesAtEveryOffset() throws Exception {
        Random rnd = new Random(42);
        byte[] data = new byte[64 * 1024 + 13]; // deliberately not power-of-two
        rnd.nextBytes(data);
        Path f = dir.resolve("channel.bin");
        Files.write(f, data);

        try (FileChannel fc = FileChannel.open(f, StandardOpenOption.READ)) {
            RandomAccessBytes bb = new ByteBufferBytes(ByteBuffer.wrap(data));
            RandomAccessBytes ch = new ChannelBytes(fc, 0, data.length);

            assertEquals(data.length, ch.size());

            for (int i = 0; i < 256; i++) {
                checkAt(bb, ch, i, data.length);
                checkAt(bb, ch, data.length - 1 - i, data.length);
            }
            for (int i = 0; i < 2_000; i++) {
                checkAt(bb, ch, rnd.nextLong(data.length), data.length);
            }

            byte[] a = new byte[1024], b = new byte[1024];
            for (int i = 0; i < 100; i++) {
                long off = rnd.nextLong(data.length - a.length);
                bb.get(off, a, 0, a.length);
                ch.get(off, b, 0, b.length);
                assertArrayEquals(a, b, "bulk @" + off);
            }
            byte[] whole = new byte[data.length];
            ch.get(0, whole, 0, whole.length);
            assertArrayEquals(data, whole, "full bulk read through the channel");

            assertThrows(IndexOutOfBoundsException.class, () -> ch.get(data.length));
            assertThrows(IndexOutOfBoundsException.class, () -> ch.getLong(data.length - 4));
            assertThrows(IndexOutOfBoundsException.class, () -> ch.get(-1));
            assertThrows(IndexOutOfBoundsException.class,
                    () -> ch.get(data.length - 8, new byte[16], 0, 16));
        }
    }

    @Test
    void nonZeroBaseReadsTheRightWindowAndNothingBeyondIt() throws Exception {
        Random rnd = new Random(7);
        byte[] data = new byte[4096];
        rnd.nextBytes(data);
        Path f = dir.resolve("window.bin");
        Files.write(f, data);

        int base = 1234;   // like a dataset's file address
        int size = 2000;   // window ends before the file does
        try (FileChannel fc = FileChannel.open(f, StandardOpenOption.READ)) {
            RandomAccessBytes ch = new ChannelBytes(fc, base, size);
            assertEquals(size, ch.size());
            for (int i = 0; i < size; i++) {
                assertEquals(data[base + i], ch.get(i), "byte @" + i);
            }
            // the file continues past the window; the view must not
            assertThrows(IndexOutOfBoundsException.class, () -> ch.get(size));
            assertThrows(IndexOutOfBoundsException.class, () -> ch.getLong(size - 7));
        }
    }

    @Test
    void windowPastChannelEndSurfacesAsUncheckedIOException() throws Exception {
        byte[] data = new byte[100];
        Path f = dir.resolve("short.bin");
        Files.write(f, data);
        try (FileChannel fc = FileChannel.open(f, StandardOpenOption.READ)) {
            // A lying window: in range for the view, but the channel has no bytes there.
            RandomAccessBytes ch = new ChannelBytes(fc, 0, 200);
            assertThrows(UncheckedIOException.class, () -> ch.get(150));
        }
    }

    @Test
    void concurrentPrimitiveReadsAgree() throws Exception {
        Random rnd = new Random(1);
        byte[] data = new byte[32 * 1024];
        rnd.nextBytes(data);
        Path f = dir.resolve("concurrent.bin");
        Files.write(f, data);

        try (FileChannel fc = FileChannel.open(f, StandardOpenOption.READ)) {
            RandomAccessBytes expected = new ByteBufferBytes(ByteBuffer.wrap(data));
            RandomAccessBytes ch = new ChannelBytes(fc, 0, data.length);
            try (ExecutorService pool = Executors.newFixedThreadPool(8)) {
                List<Callable<Boolean>> tasks = new ArrayList<>();
                for (int t = 0; t < 8; t++) {
                    long seed = 1000L + t;
                    tasks.add(() -> {
                        Random r = new Random(seed);
                        for (int i = 0; i < 2_000; i++) {
                            long off = r.nextLong(data.length - Long.BYTES);
                            if (expected.getLong(off) != ch.getLong(off)
                                    || expected.get(off) != ch.get(off)) {
                                return false;
                            }
                        }
                        return true;
                    });
                }
                for (Future<Boolean> result : pool.invokeAll(tasks)) {
                    assertTrue(result.get(), "concurrent reads must match");
                }
            }
        }
    }

    private static void checkAt(RandomAccessBytes bb, RandomAccessBytes ch, long off, int len) {
        if (off < 0 || off >= len) {
            return;
        }
        assertEquals(bb.get(off), ch.get(off), "byte @" + off);
        if (off + Long.BYTES <= len) {
            assertEquals(bb.getLong(off), ch.getLong(off), "long @" + off);
            assertEquals(bb.getDouble(off), ch.getDouble(off), "double @" + off);
        }
        if (off + Float.BYTES <= len) {
            assertEquals(bb.getFloat(off), ch.getFloat(off), "float @" + off);
        }
    }
}
