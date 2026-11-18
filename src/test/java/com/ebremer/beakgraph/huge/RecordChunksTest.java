package com.ebremer.beakgraph.huge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A dictionary's sorted terms are kept in record files, one a range of the
 * term order, and decoded ahead of the thread that encodes them:
 * {@link RecordFile}'s chunks hold the file's records in order, whichever
 * way a chunk ended (records or bytes) and from however many threads they
 * are read; {@link RecordSegments#readAhead} gives the parts' records in the
 * order of the stream, each prepared once, on any executor - one that runs
 * nothing included, since the reader does a chunk nobody has begun
 * ({@link PoolTask}) - and leaves no file open behind it.
 */
class RecordChunksTest {

    @TempDir
    Path dir;

    private static final ExternalSorter.Codec<String> STRINGS = new ExternalSorter.Codec<>() {
        @Override public void write(DataOutput out, String record) throws IOException {
            byte[] bytes = record.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            HugeIO.writeVarLong(out, bytes.length);
            out.write(bytes);
        }
        @Override public String read(DataInput in) throws IOException {
            return new String(HugeIO.readBytes(in, (int) HugeIO.readVarLong(in)), java.nio.charset.StandardCharsets.UTF_8);
        }
    };

    private RecordFile<String> file(String name, List<String> records) throws IOException {
        RecordFile<String> rf = new RecordFile<>(dir.resolve(name), STRINGS);
        for (String s : records) rf.append(s);
        rf.finish();
        return rf;
    }

    private static List<String> records(String prefix, int n, int length) {
        List<String> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) out.add((prefix + i + "-").repeat(1 + length / (prefix.length() + 6)));
        return out;
    }

    private static <T> List<T> drain(RecordSorter.SortedCursor<T> cursor) {
        List<T> out = new ArrayList<>();
        try (cursor) {
            while (cursor.hasNext()) out.add(cursor.next());
        }
        return out;
    }

    private static List<String> chunksOf(RecordFile<String> rf) throws IOException {
        List<String> out = new ArrayList<>();
        try (RecordFile<String>.ChunkReader reader = rf.chunkReader()) {
            for (int c = 0; c < rf.chunks(); c++) {
                List<String> chunk = reader.read(c);
                assertFalse(chunk.isEmpty(), "no chunk is empty");
                assertTrue(chunk.size() <= RecordFile.CHUNK_RECORDS);
                out.addAll(chunk);
            }
        }
        return out;
    }

    @Test
    void chunksHoldTheRecordsInOrder() throws Exception {
        // Short records: a chunk ends at its count.
        List<String> shortOnes = records("r", 3 * RecordFile.CHUNK_RECORDS + 17, 8);
        RecordFile<String> a = file("short.bin", shortOnes);
        assertEquals(4, a.chunks());
        assertEquals(shortOnes, chunksOf(a));
        assertEquals(shortOnes, drain(a.read()));
        // Long records: a chunk ends at its bytes, well before its count.
        List<String> longOnes = records("long", 900, 3_000);
        RecordFile<String> b = file("long.bin", longOnes);
        assertTrue(b.chunks() > 8, "chunks of about " + RecordFile.CHUNK_BYTES + " bytes: " + b.chunks());
        assertEquals(longOnes, chunksOf(b));
        // One record past a chunk's bytes by itself is a chunk by itself.
        List<String> huge = List.of("x".repeat(3 * RecordFile.CHUNK_BYTES), "y", "z".repeat(RecordFile.CHUNK_BYTES + 1), "w");
        RecordFile<String> c = file("huge.bin", huge);
        assertEquals(huge, chunksOf(c));
        // No record: no chunk, and an empty file to read.
        RecordFile<String> d = file("none.bin", List.of());
        assertEquals(0, d.chunks());
        assertEquals(List.of(), drain(d.read()));
        for (RecordFile<String> rf : List.of(a, b, c, d)) rf.close();
        try (Stream<Path> left = Files.list(dir)) {
            assertEquals(List.of(), left.toList());
        }
    }

    @Test
    void chunksAreReadFromManyThreadsThroughOneReader() throws Exception {
        List<String> records = records("t", 40_000, 40);
        RecordFile<String> rf = file("threads.bin", records);
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try (RecordFile<String>.ChunkReader reader = rf.chunkReader()) {
            List<Future<List<String>>> parts = new ArrayList<>();
            for (int c = 0; c < rf.chunks(); c++) {
                final int chunk = c;
                parts.add(pool.submit(() -> reader.read(chunk)));
            }
            List<String> all = new ArrayList<>();
            for (Future<List<String>> part : parts) all.addAll(part.get());
            assertEquals(records, all);
        } finally {
            pool.shutdownNow();
            rf.close();
        }
    }

    @Test
    void aChunkCutShortFails() throws Exception {
        RecordFile<String> rf = file("cut.bin", records("c", 5_000, 20));
        try (FileChannel ch = FileChannel.open(dir.resolve("cut.bin"), StandardOpenOption.WRITE)) {
            ch.truncate(ch.size() - 3);
        }
        try (RecordFile<String>.ChunkReader reader = rf.chunkReader()) {
            assertEquals(RecordFile.CHUNK_RECORDS, reader.read(0).size(), "the chunks before the cut are whole");
            IOException ex = assertThrows(IOException.class, () -> reader.read(rf.chunks() - 1));
            assertTrue(ex.getMessage().contains("truncated"), ex.getMessage());
        }
        rf.close();
    }

    /** The parts' records in order through read() and through readAhead() on {@code pool}. */
    private void segments(Executor pool, int window) throws Exception {
        List<List<String>> parts = List.of(
                records("a", 9_000, 10), List.of(), records("b", 1, 5), records("c", 700, 2_000), List.of(), records("d", 5_000, 60));
        List<RecordFile<String>> files = new ArrayList<>();
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < parts.size(); i++) {
            files.add(file("part-" + window + "-" + i + ".bin", parts.get(i)));
            expected.addAll(parts.get(i));
        }
        RecordSegments<String> segments = new RecordSegments<>(files);
        assertEquals(expected.size(), segments.count());
        assertEquals(expected, drain(segments.read()));
        AtomicInteger prepared = new AtomicInteger();
        assertEquals(expected, drain(segments.readAhead(pool, window, s -> prepared.incrementAndGet())));
        assertEquals(expected.size(), prepared.get(), "every record is prepared once");
        assertEquals(expected, drain(segments.readAhead(pool, window, null)), "and none need be");
        // Closed part of the way through: nothing is left running, no file is left open.
        RecordSorter.SortedCursor<String> some = segments.readAhead(pool, window, null);
        for (int i = 0; i < 6_000; i++) assertEquals(expected.get(i), some.next());
        some.close();
        assertFalse(some.hasNext());
        segments.delete();
        for (int i = 0; i < parts.size(); i++) {
            assertTrue(Files.notExists(dir.resolve("part-" + window + "-" + i + ".bin")), "part " + i + " is closed, so it can be removed");
        }
    }

    @Test
    void segmentsReadAheadInTheOrderOfTheStream() throws Exception {
        ForkJoinPool pool = new ForkJoinPool(4);
        try {
            segments(pool, 1);
            segments(pool, 6);
            // From a thread of the pool itself, as a build's stage reads: the chunks go to that thread's own queue.
            pool.submit(() -> {
                segments(pool, 3);
                return null;
            }).get(60, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }
        ExecutorService one = Executors.newSingleThreadExecutor();
        try {
            segments(one, 4);
        } finally {
            one.shutdownNow();
        }
        // An executor that never runs anything: the reader decodes every chunk itself.
        segments(task -> { }, 5);
    }

    @Test
    void aFailureWhilePreparingIsTheReadersFailure() throws Exception {
        RecordFile<String> rf = file("fail.bin", records("f", 10_000, 10));
        RecordSegments<String> segments = new RecordSegments<>(List.of(rf));
        ForkJoinPool pool = new ForkJoinPool(3);
        try {
            RecordSorter.SortedCursor<String> cursor = segments.readAhead(pool, 4, s -> {
                if (s.startsWith("f7000-")) throw new IllegalStateException("boom at " + s.substring(0, 5));
            });
            IllegalStateException ex = assertThrows(IllegalStateException.class, () -> {
                while (cursor.hasNext()) cursor.next();
            });
            assertEquals("boom at f7000", ex.getMessage());
            cursor.close();
        } finally {
            pool.shutdownNow();
        }
        segments.delete();
        assertTrue(Files.notExists(dir.resolve("fail.bin")));
        // A part that is gone when it is asked for is an I/O failure of the read.
        RecordSegments<String> gone = new RecordSegments<>(List.of(file("gone.bin", records("g", 100, 10))));
        Files.delete(dir.resolve("gone.bin"));
        assertThrows(UncheckedIOException.class, () -> drain(gone.readAhead(Runnable::run, 2, null)));
    }

    @Test
    void aPoolTaskIsRunByWhoeverComesFirstAndOnce() throws Exception {
        AtomicInteger runs = new AtomicInteger();
        // Nobody runs it: join does.
        PoolTask<String> inline = PoolTask.submit(task -> { }, () -> "ran " + runs.incrementAndGet());
        assertFalse(inline.isDone());
        assertEquals("ran 1", inline.join("testing"));
        assertTrue(inline.isDone());
        inline.run();
        assertEquals("ran 1", inline.join("testing"), "a second run or join does not run the body again");
        // A worker has it: join waits for that run.
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            CountDownLatch begun = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            PoolTask<Integer> held = PoolTask.submit(worker, () -> {
                begun.countDown();
                release.await();
                return runs.incrementAndGet();
            });
            assertTrue(begun.await(10, TimeUnit.SECONDS));
            Thread releaser = new Thread(() -> {
                try { Thread.sleep(100); } catch (InterruptedException ignored) { }
                release.countDown();
            });
            releaser.start();
            assertEquals(2, held.join("testing"));
            releaser.join();
            // Failures come out as Futures.join gives them: an IOException as itself.
            PoolTask<Void> failing = PoolTask.submit(worker, () -> { throw new IOException("disk"); });
            assertEquals("disk", assertThrows(IOException.class, () -> failing.join("testing")).getMessage());
            PoolTask<Void> error = PoolTask.submit(worker, () -> { throw new OutOfMemoryError("heap"); });
            assertThrows(OutOfMemoryError.class, () -> error.join("testing"));
            // Abandoned before it began: it never runs. Abandoned while running: waited for.
            PoolTask<Integer> never = PoolTask.submit(task -> { }, runs::incrementAndGet);
            never.abandon();
            never.run();
            assertEquals(2, runs.get());
            CountDownLatch running = new CountDownLatch(1);
            AtomicInteger ended = new AtomicInteger();
            PoolTask<Void> slow = PoolTask.submit(worker, () -> {
                running.countDown();
                Thread.sleep(150);
                ended.incrementAndGet();
                return null;
            });
            assertTrue(running.await(10, TimeUnit.SECONDS));
            slow.abandon();
            assertEquals(1, ended.get(), "abandon() returned after the running body ended");
        } finally {
            worker.shutdownNow();
        }
    }
}
