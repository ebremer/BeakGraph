package com.ebremer.beakgraph.hdf5.writers.hugeUltra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.StoreParity;
import com.ebremer.beakgraph.core.VoidMode;
import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.huge.HugeBuildPipeline;
import com.ebremer.beakgraph.huge.HugeRecords;
import com.ebremer.beakgraph.huge.HugeRecords.TermRow;
import com.ebremer.beakgraph.huge.RangeSorter;
import com.ebremer.beakgraph.huge.RecordSorter;
import com.ebremer.beakgraph.huge.SorterProvider;
import com.ebremer.beakgraph.huge.Workspaces;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

/**
 * The disk build merges its sorted term columns into the dictionaries and
 * the rows' ids a range of the term order at a time where the sorters have
 * ranges and there is a pool. However the ranges fall - one or many, read
 * from runs of several merge levels or from sorters that never spilled,
 * chosen for sources with every kind of term - the store is the bytes the
 * sequential engine's one merge writes. And a range that fails fails the
 * build at once, with nothing of the others left running or open.
 */
class RangedBuildTest {

    @TempDir
    Path dir;

    private final ForkJoinPool pool = new ForkJoinPool(4);

    @AfterEach
    void shutdown() {
        pool.shutdownNow();
    }

    /** Many subjects, few predicates, objects of several kinds with repeats, graphs, duplicate quads, long literals. */
    private static String manyQuads(int n) {
        StringBuilder nq = new StringBuilder(n * 90);
        for (int i = 0; i < n; i++) {
            String s = (i % 11 == 0) ? "_:b" + (i % 300) : "<http://ex.org/s" + (i % 700) + ">";
            String p = "<http://ex.org/p" + (i % 6) + ">";
            String o = switch (i % 7) {
                case 0 -> "<http://ex.org/o" + (i % 250) + ">";
                case 1 -> "\"str" + (i % 900) + "\"";
                case 2 -> "\"" + (i % 500 - 250) + "\"^^<http://www.w3.org/2001/XMLSchema#int>";
                case 3 -> "\"" + ((i % 90) / 4.0) + "\"^^<http://www.w3.org/2001/XMLSchema#double>";
                case 4 -> "\"mot " + (i % 120) + "\"@fr";
                case 5 -> "\"a literal long enough to be compressed in its dictionary, number " + (i % 2_000) + " of them, "
                        + "x".repeat(40 + i % 30) + "\"";
                default -> "\"" + (i % 400) + "\"^^<http://www.w3.org/2001/XMLSchema#integer>";
            };
            nq.append(s).append(' ').append(p).append(' ').append(o);
            if (i % 3 != 0) {
                nq.append(" <http://ex.org/g").append(i % 9).append('>');
            }
            nq.append(" .\n");
            if (i % 10 == 0) {
                nq.append(s).append(' ').append(p).append(' ').append(o).append(" .\n");
            }
        }
        return nq.toString();
    }

    /** Builds {@code src} with the pipeline; returns the store, and how many ranges it was merged in through {@code ranges}. */
    private Path build(String tag, File src, boolean spatial, SorterProvider provider, ForkJoinPool stagePool,
                       long rangeRows, AtomicInteger ranges) throws IOException {
        Path work = Files.createDirectories(dir.resolve("work-" + tag));
        Path out = dir.resolve(tag + ".h5");
        try (HugeBuildPipeline pipeline = new HugeBuildPipeline(List.of(src), spatial, spatial, VoidMode.NONE, work, provider, stagePool)) {
            pipeline.setMinRangeRows(rangeRows);
            pipeline.run(out);
            ranges.set(pipeline.ranges());
        }
        Workspaces.deleteTree(work, LoggerFactory.getLogger(RangedBuildTest.class));
        assertTrue(Files.notExists(work), tag + ": the build leaves its workspace removable");
        return out;
    }

    private void everyWayOfRangesWritesTheSameBytes(String name, String content, boolean spatial, long rangeRows) throws Exception {
        File src = dir.resolve(name).toFile();
        Files.writeString(src.toPath(), content, StandardCharsets.UTF_8);
        AtomicInteger ranges = new AtomicInteger();

        // The sequential engine: one merge of the whole columns, on this thread.
        Path sequential = build(name + "-sequential", src, spatial, SorterProvider.sequential(512, 4096, 16), null, rangeRows, ranges);
        assertEquals(1, ranges.get(), "no pool, no ranges");

        // The parallel sorters, but a range too large to have two of: one range, on the pool's machinery.
        Path one = build(name + "-one", src, spatial, new UltraSorterProvider(256, 512, 4, pool), pool, 1L << 40, ranges);
        assertEquals(1, ranges.get());
        assertEquals(-1, Files.mismatch(sequential, one), name + ": one range of the parallel sorters");

        // Small runs, fan-in 4: ranges read from runs that went through several levels of merges.
        Path runs = build(name + "-runs", src, spatial, new UltraSorterProvider(256, 512, 4, pool), pool, rangeRows, ranges);
        assertTrue(ranges.get() > 3, name + ": merged in several ranges of runs: " + ranges.get());
        assertEquals(-1, Files.mismatch(sequential, runs), name + ": " + ranges.get() + " ranges of merged runs");

        // Nothing spills: ranges of the sorted arrays.
        Path memory = build(name + "-memory", src, spatial, new UltraSorterProvider(1 << 19, 1 << 20, 16, pool), pool, rangeRows, ranges);
        assertTrue(ranges.get() > 3, name + ": merged in several ranges of arrays: " + ranges.get());
        assertEquals(-1, Files.mismatch(sequential, memory), name + ": " + ranges.get() + " ranges in memory");

        // Runs cut by bytes, one level of them (fan-in above their number).
        Path bytes = build(name + "-bytes", src, spatial, new UltraSorterProvider(1 << 19, 256, 512, 48L << 10, pool), pool, rangeRows, ranges);
        assertEquals(-1, Files.mismatch(sequential, bytes), name + ": " + ranges.get() + " ranges of runs cut by bytes");

        // A pool of one thread: the ranges in turn, by the one worker.
        ForkJoinPool single = new ForkJoinPool(1);
        try {
            Path alone = build(name + "-single", src, spatial, new UltraSorterProvider(256, 512, 4, single), single, rangeRows, ranges);
            assertEquals(-1, Files.mismatch(sequential, alone), name + ": " + ranges.get() + " ranges on one thread");
        } finally {
            single.shutdownNow();
        }
    }

    @Test
    void manyQuadsInManyRanges() throws Exception {
        everyWayOfRangesWritesTheSameBytes("many.nq", manyQuads(8_000), false, 64);
    }

    @Test
    void everyKindOfTermInManyRanges() throws Exception {
        // Triple terms and their interior terms, blank nodes, relative IRIs, composites, ill-typed literals.
        everyWayOfRangesWritesTheSameBytes("mixed.trig", StoreParity.mixedTrig(), false, 2);
    }

    @Test
    void spatialAndFeaturesInManyRanges() throws Exception {
        everyWayOfRangesWritesTheSameBytes("spatial.trig", StoreParity.SPATIAL_TRIG, true, 2);
    }

    @Test
    void oneTermHoldingMostRows() throws Exception {
        // One graph, one predicate value and one object for nearly every row: ranges of very unequal rows.
        StringBuilder nq = new StringBuilder();
        for (int i = 0; i < 6_000; i++) {
            nq.append("<http://ex.org/s").append(i % 50).append("> <http://ex.org/p> ")
              .append(i % 40 == 0 ? "\"rare " + i + "\"" : "\"common\"").append(" <http://ex.org/g> .\n");
        }
        everyWayOfRangesWritesTheSameBytes("skewed.nq", nq.toString(), false, 16);
    }

    /** The hugeUltra provider, with the object column's ranges rigged: the third one opened fails after ten records. */
    private SorterProvider failingInARange(AtomicInteger opened) {
        SorterProvider base = new UltraSorterProvider(64, 128, 2, pool);
        return new SorterProvider() {
            @Override
            public RecordSorter<TermRow> termSorter(Path workDir, String tag) {
                RangeSorter<TermRow> sorter = (RangeSorter<TermRow>) base.termSorter(workDir, tag);
                if (!tag.equals("ocol")) {
                    return sorter;
                }
                return new RangeSorter<>() {
                    @Override public void add(TermRow record) throws IOException { sorter.add(record); }
                    @Override public long size() { return sorter.size(); }
                    @Override public SortedCursor<TermRow> sorted() throws IOException { return sorter.sorted(); }
                    @Override public void close() { sorter.close(); }

                    @Override
                    public Ranges<TermRow> ranges() throws IOException {
                        Ranges<TermRow> ranges = sorter.ranges();
                        return new Ranges<>() {
                            @Override public List<Sample<TermRow>> samples() { return ranges.samples(); }
                            @Override public int maxOpen() { return ranges.maxOpen(); }

                            @Override
                            public SortedCursor<TermRow> open(TermRow from, TermRow to) throws IOException {
                                SortedCursor<TermRow> cursor = ranges.open(from, to);
                                if (opened.incrementAndGet() != 3) {
                                    return cursor;
                                }
                                return new SortedCursor<>() {
                                    private int given = 0;
                                    @Override public boolean hasNext() { return cursor.hasNext(); }
                                    @Override public TermRow next() {
                                        if (++given > 10) {
                                            throw new UncheckedIOException(new IOException("boom"));
                                        }
                                        return cursor.next();
                                    }
                                    @Override public void close() { cursor.close(); }
                                };
                            }
                        };
                    }
                };
            }

            @Override
            public RecordSorter<HugeRecords.RowId> rowIdSorter(Path workDir, String tag, long maxRow, long maxId) {
                return base.rowIdSorter(workDir, tag, maxRow, maxId);
            }

            @Override
            public RecordSorter<HugeRecords.IdQuad> quadSorter(Path workDir, String tag, Index order,
                                                               long numEntities, long numPredicates, long numObjects) {
                return base.quadSorter(workDir, tag, order, numEntities, numPredicates, numObjects);
            }
        };
    }

    @Test
    void aRangeThatFailsFailsTheBuildAndLeavesNothingOpen() throws Exception {
        File src = dir.resolve("failing.nq").toFile();
        Files.writeString(src.toPath(), manyQuads(8_000), StandardCharsets.UTF_8);
        Path work = Files.createDirectories(dir.resolve("work-failing"));
        AtomicInteger opened = new AtomicInteger();
        try (HugeBuildPipeline pipeline = new HugeBuildPipeline(List.of(src), false, false, VoidMode.NONE, work,
                failingInARange(opened), pool)) {
            pipeline.setMinRangeRows(64);
            IOException ex = assertThrows(IOException.class, () -> pipeline.run(dir.resolve("failing.h5")));
            assertEquals("boom", ex.getMessage());
            assertTrue(pipeline.ranges() > 3, "the build was merging in ranges: " + pipeline.ranges());
            assertTrue(opened.get() >= 3);
        }
        // Every range's files - its dictionaries' parts, its ids, the runs under it - are closed: on Windows
        // an open one could not be removed.
        Workspaces.deleteTree(work, LoggerFactory.getLogger(RangedBuildTest.class));
        assertTrue(Files.notExists(work), "the workspace is fully removable after the failure");
    }
}
