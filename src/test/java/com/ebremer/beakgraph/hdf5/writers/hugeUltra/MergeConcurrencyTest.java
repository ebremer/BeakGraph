package com.ebremer.beakgraph.hdf5.writers.hugeUltra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.huge.RecordSorter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * BG-133: a merge level runs at most {@code maxConcurrentMerges} groups at a
 * time - each holds fan-in + 1 open files and (for term sorters) a batch of
 * live records - instead of submitting every group of the level at once.
 * A counting executor records the peak number of simultaneously running
 * sorter tasks; with tiny batches and fan-in 2 a few hundred records make
 * hundreds of groups over many levels.
 */
class MergeConcurrencyTest {

    @TempDir
    Path dir;

    private final ForkJoinPool pool = new ForkJoinPool(8);

    @AfterEach
    void shutdown() {
        pool.shutdownNow();
    }

    /** Delegates to the pool while counting how many submitted tasks run at once. */
    static final class CountingExecutor extends AbstractExecutorService {
        private final ExecutorService delegate;
        private final AtomicInteger running = new AtomicInteger();
        private final AtomicInteger peak = new AtomicInteger();
        private final AtomicInteger total = new AtomicInteger();

        CountingExecutor(ExecutorService delegate) {
            this.delegate = delegate;
        }

        @Override
        public void execute(Runnable command) {
            total.incrementAndGet();
            delegate.execute(() -> {
                int now = running.incrementAndGet();
                peak.accumulateAndGet(now, Math::max);
                try {
                    command.run();
                } finally {
                    running.decrementAndGet();
                }
            });
        }

        int peak() { return peak.get(); }
        int total() { return total.get(); }

        @Override public void shutdown() { }
        @Override public List<Runnable> shutdownNow() { return List.of(); }
        @Override public boolean isShutdown() { return false; }
        @Override public boolean isTerminated() { return false; }
        @Override public boolean awaitTermination(long timeout, TimeUnit unit) { return true; }
    }

    private static final ParallelSpillSorter.RunFormat<String> STRINGS = new ParallelSpillSorter.RunFormat<>() {
        @Override
        public void writeRun(DataOutput out, Object[] sorted, int n) throws IOException {
            for (int i = 0; i < n; i++) out.writeUTF((String) sorted[i]);
        }

        @Override
        public RunStream<String> newStream() {
            return DataInput::readUTF;   // EOFException ends the run
        }
    };

    private void termSorterHonoursTheCap(int cap) throws Exception {
        CountingExecutor exec = new CountingExecutor(pool);
        Random rnd = new Random(cap);
        List<String> input = new ArrayList<>();
        for (int i = 0; i < 400; i++) input.add("s" + rnd.nextInt(100_000));
        List<String> expected = new ArrayList<>(input);
        expected.sort(Comparator.naturalOrder());
        Path work = Files.createDirectories(dir.resolve("term-" + cap));
        List<String> actual = new ArrayList<>();
        try (ParallelSpillSorter<String> sorter = new ParallelSpillSorter<>(work, "t", Comparator.naturalOrder(),
                STRINGS, 4, 2, exec, null, Long.MAX_VALUE, cap)) {
            for (String s : input) sorter.add(s);
            try (RecordSorter.SortedCursor<String> c = sorter.sorted()) {
                while (c.hasNext()) actual.add(c.next());
            }
        }
        assertEquals(expected, actual);
        assertTrue(exec.total() > 100, "many spill and merge tasks ran: " + exec.total());
        assertTrue(exec.peak() <= cap, "at most " + cap + " tasks ran at once, saw " + exec.peak());
    }

    @Test
    void termSorterRunsAtMostTheConfiguredMergesAtOnce() throws Exception {
        termSorterHonoursTheCap(1);
        termSorterHonoursTheCap(3);
    }

    private void packedSorterHonoursTheCap(int cap) throws Exception {
        CountingExecutor exec = new CountingExecutor(pool);
        Random rnd = new Random(10 + cap);
        List<Long> input = new ArrayList<>();
        for (int i = 0; i < 500; i++) input.add(rnd.nextLong() >>> 1);
        List<Long> expected = new ArrayList<>(input);
        expected.sort(Comparator.naturalOrder());
        Path work = Files.createDirectories(dir.resolve("packed-" + cap));
        List<Long> actual = new ArrayList<>();
        try (PackedLongSorter sorter = new PackedLongSorter(work, "p", 63, 4, 2, pool, exec, cap)) {
            for (long v : input) sorter.add(0, v);
            try (PackedLongSorter.KeyCursor c = sorter.sorted()) {
                while (c.advance()) actual.add(c.lo());
            }
        }
        assertEquals(expected, actual);
        assertTrue(exec.peak() <= cap, "at most " + cap + " tasks ran at once, saw " + exec.peak());
    }

    @Test
    void packedSorterRunsAtMostTheConfiguredMergesAtOnce() throws Exception {
        packedSorterHonoursTheCap(1);
        packedSorterHonoursTheCap(3);
    }

    @Test
    void defaultConcurrencyFitsTheDescriptorBudget() {
        assertEquals(7, UltraSorterProvider.defaultMergeConcurrency(128, 32), "32 cores, fan-in 128: 1024 / 129");
        assertEquals(4, UltraSorterProvider.defaultMergeConcurrency(128, 4), "never more than the cores");
        assertEquals(1, UltraSorterProvider.defaultMergeConcurrency(2000, 64), "at least one");
        assertEquals(8, UltraSorterProvider.defaultMergeConcurrency(2, 8));
    }
}
