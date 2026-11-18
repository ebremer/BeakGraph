package com.ebremer.beakgraph.huge;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.hdf5.WritableDataset;
import com.ebremer.beakgraph.hdf5.WritableGroup;
import com.ebremer.beakgraph.hdf5.writers.FCDWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

/**
 * The disk engines' front-coded string store is encoded a batch of strings at
 * a time, by the adding thread or - given a pool - by the pool's threads,
 * and written in order. Its three buffers must be, byte for byte, what the
 * in-memory {@link FCDWriter} makes of the same strings one at a time: the
 * string buffer, the offsets of the blocks, the flags of the compressed
 * fragments. The strings here cut batches in the middle of blocks (by
 * characters and by count), cross the compression threshold both ways, share
 * prefixes that end inside a surrogate pair, and include the empty string.
 */
class SpillFCDWriterBatchTest {

    @TempDir
    Path dir;

    /** A group that keeps what is put in it. */
    private static final class Kept implements WritableGroup {
        final Map<String, byte[]> datasets = new HashMap<>();
        final Map<String, Long> attributes = new HashMap<>();
        final Map<String, Kept> groups = new HashMap<>();

        @Override public WritableGroup putGroup(String name) {
            Kept child = new Kept();
            groups.put(name, child);
            return child;
        }

        @Override public WritableDataset putDataset(String name, byte[] data) {
            datasets.put(name, data.clone());
            return new WritableDataset() {
                @Override public void write(byte[] buf, int off, int len) { throw new UnsupportedOperationException(); }
                @Override public long length() { return data.length; }
                @Override public long written() { return data.length; }
                @Override public void putAttribute(String attr, int value) { attributes.put(name + "@" + attr, (long) value); }
                @Override public void putAttribute(String attr, long value) { attributes.put(name + "@" + attr, value); }
            };
        }

        @Override public WritableDataset createDataset(String name, long length) { throw new UnsupportedOperationException(); }
        @Override public void putAttribute(String name, int value) { attributes.put(name, (long) value); }
        @Override public void putAttribute(String name, long value) { attributes.put(name, value); }
    }

    private static List<String> strings() {
        Random rnd = new Random(42);
        List<String> out = new ArrayList<>();
        out.add("");
        out.add("");
        // Around the compression threshold, in bytes: ASCII, and two-byte characters.
        for (int n = Params.COMPRESSION_THRESHOLD - 3; n <= Params.COMPRESSION_THRESHOLD + 3; n++) {
            out.add("a".repeat(n));
            out.add("é".repeat(n / 2) + "z".repeat(n % 2));
        }
        // Prefixes that end inside a surrogate pair, and at one.
        out.add("bird 🐦 one");
        out.add("bird 🐧 two");
        out.add("bird 🐧 two and more");
        out.add("bird \uD83D");   // an unpaired high surrogate at the end
        out.add("bird 🐧");
        // Sorted-looking IRIs and polygons: long shared prefixes, fragments past the threshold.
        for (int i = 0; i < 20_000; i++) {
            out.add("http://bench.beakgraph.org/feature/" + String.format("%07d", i));
        }
        for (int i = 0; i < 6_000; i++) {
            StringBuilder wkt = new StringBuilder("POLYGON ((");
            int vertices = 8 + rnd.nextInt(40);
            for (int v = 0; v < vertices; v++) {
                wkt.append(1000 + i / 3).append(' ').append(rnd.nextInt(90_000)).append(v + 1 < vertices ? ", " : "))");
            }
            out.add(wkt.toString());
        }
        // One string longer than a batch, in the middle of a block; then more strings than a batch counts
        // (short ones), so that the batch after it is cut by count, in the middle of a block too.
        out.add("long ".repeat(SpillFCDWriter.BATCH_CHARS / 4));
        out.add("x");
        for (int i = 0; i < SpillFCDWriter.BATCH_STRINGS + 3_000; i++) {
            out.add(Integer.toString(i % 7));
        }
        out.add("tail " + "t".repeat(200));
        return out;
    }

    private static Kept inMemory(List<String> strings) throws IOException {
        Kept root = new Kept();
        try (FCDWriter ram = new FCDWriter(Path.of("strings"), Params.FCD_BLOCK_SIZE)) {
            for (String s : strings) ram.add(s);
            assertEquals(strings.size(), ram.getNumEntries());
            ram.add(root);
        }
        return root.groups.get("strings");
    }

    /** The three temp files of a finished spill writer, as (strings, offsets, compressed). */
    private byte[][] spilled(String tag, List<String> strings, Executor pool, int window) throws IOException {
        Path work = Files.createDirectories(dir.resolve(tag));
        try (SpillFCDWriter writer = new SpillFCDWriter(work, "strings", Params.FCD_BLOCK_SIZE, pool, window)) {
            long n = 0;
            for (String s : strings) {
                assertEquals(n++, writer.getNumEntries(), "the count is that of the strings added, at once");
                writer.add(s);
            }
            writer.finish();
            assertThrows(IllegalStateException.class, () -> writer.add("late"));
            return new byte[][]{
                Files.readAllBytes(work.resolve("strings.fcd.strings")),
                Files.readAllBytes(work.resolve("strings.fcd.offsets")),
                Files.readAllBytes(work.resolve("strings.fcd.compressed"))};
        } finally {
            Workspaces.deleteTree(work, LoggerFactory.getLogger(SpillFCDWriterBatchTest.class));
            assertTrue(Files.notExists(work), "close() leaves the temp files removable");
        }
    }

    /** A buffer the in-memory writer did not put (it had nothing in it) is an empty one. */
    private static byte[] of(Kept ram, String name) {
        byte[] data = ram.datasets.get(name);
        return (data == null) ? new byte[0] : data;
    }

    private static void assertSame(String how, Kept ram, byte[][] spill) {
        assertArrayEquals(of(ram, "stringbuffer"), spill[0], how + ": string buffer");
        assertArrayEquals(of(ram, "offsets"), spill[1], how + ": block offsets");
        assertArrayEquals(of(ram, "compressed"), spill[2], how + ": compressed flags");
    }

    @Test
    void theBuffersAreTheInMemoryWritersByteForByte() throws Exception {
        List<String> strings = strings();
        Kept ram = inMemory(strings);
        assertTrue(ram.datasets.get("stringbuffer").length > 1 << 20, "a string buffer of some megabytes");
        assertSame("the adding thread", ram, spilled("alone", strings, null, 0));
        ForkJoinPool pool = new ForkJoinPool(4);
        try {
            assertSame("a pool, one batch at a time", ram, spilled("pool-1", strings, pool, 1));
            assertSame("a pool, eight batches at a time", ram, spilled("pool-8", strings, pool, 8));
            // Added from a thread of the pool, as a build's dictionary stage adds.
            byte[][] fromWorker = pool.submit(() -> spilled("pool-worker", strings, pool, 3)).get();
            assertSame("a pool, added from one of its threads", ram, fromWorker);
        } finally {
            pool.shutdownNow();
        }
        assertSame("an executor that runs nothing", ram, spilled("idle", strings, task -> { }, 4));
    }

    @Test
    void fewStringsAndNone() throws Exception {
        for (int n : new int[]{0, 1, Params.FCD_BLOCK_SIZE - 1, Params.FCD_BLOCK_SIZE, Params.FCD_BLOCK_SIZE + 1, 3 * Params.FCD_BLOCK_SIZE}) {
            List<String> strings = new ArrayList<>();
            for (int i = 0; i < n; i++) strings.add("http://ex.org/a/long/enough/prefix/to/share/" + i);
            Kept ram = inMemory(strings);
            ForkJoinPool pool = new ForkJoinPool(2);
            try {
                assertSame(n + " strings", ram, spilled("few-" + n, strings, pool, 2));
                assertSame(n + " strings, alone", ram, spilled("few-alone-" + n, strings, null, 0));
            } finally {
                pool.shutdownNow();
            }
        }
    }

    @Test
    void aWriterClosedBeforeItIsFinishedLeavesNothing() throws Exception {
        Path work = Files.createDirectories(dir.resolve("abandoned"));
        ForkJoinPool pool = new ForkJoinPool(3);
        try {
            SpillFCDWriter writer = new SpillFCDWriter(work, "strings", Params.FCD_BLOCK_SIZE, pool, 4);
            for (String s : strings()) writer.add(s);
            writer.close();   // a failed build: batches still being encoded, strings still gathered
            writer.close();
        } finally {
            pool.shutdownNow();
        }
        Workspaces.deleteTree(work, LoggerFactory.getLogger(SpillFCDWriterBatchTest.class));
        assertTrue(Files.notExists(work));
    }
}
