package com.ebremer.beakgraph.hdf5.writers.hugeUltra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.huge.HugeRecords;
import com.ebremer.beakgraph.huge.HugeRecords.TermRow;
import com.ebremer.beakgraph.huge.RangeSorter;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The term sorter read as ranges of its order ({@link RangeSorter}): the
 * ranges opened for bounds that follow one another hold every record once,
 * each in order and within its bounds - whether the records are in runs (one
 * level of them or several, their indexes whole or thinned) or never left
 * the heap, whichever bounds are asked for (records of the sorter, values
 * between them, values below and above them all), and from however many
 * threads at once. The dictionary merge of a build rests on exactly that.
 */
class SorterRangesTest {

    @TempDir
    Path dir;

    private final ForkJoinPool pool = new ForkJoinPool(6);

    @AfterEach
    void shutdown() {
        pool.shutdownNow();
    }

    private static final ParallelSpillSorter.RunFormat<String> STRINGS = new ParallelSpillSorter.RunFormat<>() {
        @Override
        public void writeRun(DataOutput out, Object[] sorted, int from, int to) throws IOException {
            for (int i = from; i < to; i++) out.writeUTF((String) sorted[i]);
        }

        @Override
        public RunStream<String> newStream() {
            return DataInput::readUTF;   // EOFException ends the run
        }
    };

    private static List<String> strings(int n, int distinct, long seed) {
        Random rnd = new Random(seed);
        List<String> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) out.add("s" + String.format("%06d", rnd.nextInt(distinct)));
        return out;
    }

    private static <T> List<T> drain(RecordSorter.SortedCursor<T> cursor) {
        List<T> out = new ArrayList<>();
        try (cursor) {
            while (cursor.hasNext()) out.add(cursor.next());
        }
        return out;
    }

    /** The ranges for {@code bounds} in turn, each checked against its bounds; together they are returned. */
    private static <T> List<T> readRanges(RangeSorter.Ranges<T> ranges, List<T> bounds, Comparator<? super T> order) throws IOException {
        List<T> all = new ArrayList<>();
        for (int r = 0; r <= bounds.size(); r++) {
            T from = (r == 0) ? null : bounds.get(r - 1);
            T to = (r == bounds.size()) ? null : bounds.get(r);
            List<T> range = drain(ranges.open(from, to));
            for (int i = 0; i < range.size(); i++) {
                T record = range.get(i);
                assertTrue(from == null || order.compare(record, from) >= 0, "range " + r + " holds " + record + " below its bound " + from);
                assertTrue(to == null || order.compare(record, to) < 0, "range " + r + " holds " + record + " at or above its end " + to);
                assertTrue(i == 0 || order.compare(range.get(i - 1), record) <= 0, "range " + r + " is out of order at " + i);
            }
            all.addAll(range);
        }
        return all;
    }

    private void stringRanges(String tag, int records, int distinct, int batch, int fanIn, List<String> bounds) throws Exception {
        List<String> input = strings(records, distinct, records + batch);
        List<String> expected = new ArrayList<>(input);
        expected.sort(Comparator.naturalOrder());
        Path work = Files.createDirectories(dir.resolve(tag));
        try (ParallelSpillSorter<String> sorter = new ParallelSpillSorter<>(work, "t", Comparator.naturalOrder(),
                STRINGS, batch, fanIn, pool)) {
            for (String s : input) sorter.add(s);
            RangeSorter.Ranges<String> ranges = sorter.ranges();
            List<String> chosen = new ArrayList<>(bounds);
            if (chosen.isEmpty()) {
                // The sorter's own samples, as a build takes its bounds: every 7th, without repeats.
                List<String> samples = new ArrayList<>();
                for (RangeSorter.Sample<String> sample : ranges.samples()) {
                    assertTrue(sample.weight() > 0, "a sample weighs something");
                    samples.add(sample.record());
                }
                samples.sort(Comparator.naturalOrder());
                for (int i = 3; i < samples.size(); i += 7) {
                    if (chosen.isEmpty() || !chosen.get(chosen.size() - 1).equals(samples.get(i))) chosen.add(samples.get(i));
                }
                assertTrue(chosen.size() > 3, "the samples give bounds to choose from: " + samples.size());
            }
            assertEquals(expected, readRanges(ranges, chosen, Comparator.naturalOrder()), tag);
            // Ranges are read as often as asked: the runs stay until the sorter is closed.
            assertEquals(expected, readRanges(ranges, List.of(), Comparator.naturalOrder()), tag + ", whole");
            assertTrue(ranges.maxOpen() >= 1);
        }
        try (Stream<Path> left = Files.list(work)) {
            assertEquals(List.of(), left.toList(), "close() removes the runs");
        }
    }

    @Test
    void rangesOfRunsHoldEveryRecordOnceInOrder() throws Exception {
        stringRanges("one-level", 3_000, 900, 64, 128, List.of());
        stringRanges("many-levels", 5_000, 1_500, 16, 3, List.of());
        stringRanges("few-distinct", 4_000, 9, 32, 4, List.of());
    }

    @Test
    void boundsNeedNotBeRecords() throws Exception {
        // Below every record, between records, a record that repeats, past every record - and the same one twice.
        stringRanges("odd-bounds", 3_000, 500, 32, 4,
                List.of("a", "s000100", "s000100", "s000250x", "s000499", "t"));
    }

    @Test
    void rangesOfASorterThatNeverSpilled() throws Exception {
        stringRanges("in-memory", 2_000, 700, 1 << 16, 8, List.of());
        stringRanges("in-memory-odd", 2_000, 700, 1 << 16, 8, List.of("a", "s000300", "s000300", "zz"));
        stringRanges("empty", 0, 1, 16, 2, List.of("m"));
    }

    @Test
    void rangesAreReadFromManyThreadsAtOnce() throws Exception {
        List<String> input = strings(20_000, 6_000, 7);
        List<String> expected = new ArrayList<>(input);
        expected.sort(Comparator.naturalOrder());
        Path work = Files.createDirectories(dir.resolve("threads"));
        ExecutorService readers = Executors.newFixedThreadPool(8);
        try (ParallelSpillSorter<String> sorter = new ParallelSpillSorter<>(work, "t", Comparator.naturalOrder(),
                STRINGS, 256, 6, pool)) {
            for (String s : input) sorter.add(s);
            RangeSorter.Ranges<String> ranges = sorter.ranges();
            List<String> bounds = new ArrayList<>();
            for (int i = 1; i < 40; i++) bounds.add("s" + String.format("%06d", i * 150));
            List<Future<List<String>>> parts = new ArrayList<>();
            for (int r = 0; r <= bounds.size(); r++) {
                final String from = (r == 0) ? null : bounds.get(r - 1);
                final String to = (r == bounds.size()) ? null : bounds.get(r);
                parts.add(readers.submit((Callable<List<String>>) () -> drain(ranges.open(from, to))));
            }
            List<String> all = new ArrayList<>();
            for (Future<List<String>> part : parts) all.addAll(part.get());
            assertEquals(expected, all);
        } finally {
            readers.shutdownNow();
        }
    }

    @Test
    void anIndexPastItsBudgetIsThinnedAndTheRangesStillHold() throws Exception {
        List<String> input = strings(4_000, 2_000, 11);
        List<String> expected = new ArrayList<>(input);
        expected.sort(Comparator.naturalOrder());
        Path work = Files.createDirectories(dir.resolve("thin"));
        // Every record is said to hold 256 KiB, and a batch 4 MiB: runs of sixteen records, and an index
        // budget of a megabyte - room for four records between all the runs there are.
        try (ParallelSpillSorter<String> sorter = new ParallelSpillSorter<>(work, "t", Comparator.naturalOrder(),
                STRINGS, 64, 1024, pool, s -> 1L << 18, 4L << 20)) {
            for (String s : input) sorter.add(s);
            RangeSorter.Ranges<String> ranges = sorter.ranges();
            long runs;
            try (Stream<Path> files = Files.list(work)) {
                runs = files.count();
            }
            assertTrue(runs > 30, "many runs: " + runs);
            int samples = ranges.samples().size();
            assertTrue(samples <= runs + 4, "every index was thinned to its first entries: " + samples + " samples of " + runs + " runs");
            assertEquals(expected, readRanges(ranges, List.of("s000400", "s000401", "s001000", "s001999"), Comparator.naturalOrder()));
        }
    }

    @Test
    void asManyRangesMayBeOpenAsTheMergesHaveFilesFor() throws Exception {
        Path work = Files.createDirectories(dir.resolve("open"));
        // Fan-in 4, two merges at a time: ten files. Four final runs at most, so two ranges of five files.
        try (ParallelSpillSorter<String> sorter = new ParallelSpillSorter<>(work, "t", Comparator.naturalOrder(),
                STRINGS, 16, 4, pool, null, Long.MAX_VALUE, 2)) {
            for (String s : strings(1_000, 400, 3)) sorter.add(s);
            RangeSorter.Ranges<String> ranges = sorter.ranges();
            long runs;
            try (Stream<Path> files = Files.list(work)) {
                runs = files.count();
            }
            assertTrue(runs >= 2 && runs <= 4, "merged down to the fan-in: " + runs);
            assertEquals(10 / (runs + 1), ranges.maxOpen());
        }
    }

    @Test
    void aSorterIsReadOneWayOnce() throws Exception {
        Path work = Files.createDirectories(dir.resolve("once"));
        try (ParallelSpillSorter<String> sorter = new ParallelSpillSorter<>(work, "a", Comparator.naturalOrder(), STRINGS, 16, 4, pool)) {
            sorter.add("x");
            sorter.ranges();
            assertThrows(IllegalStateException.class, sorter::sorted);
            assertThrows(IllegalStateException.class, sorter::ranges);
            assertThrows(IllegalStateException.class, () -> sorter.add("y"));
        }
        try (ParallelSpillSorter<String> sorter = new ParallelSpillSorter<>(work, "b", Comparator.naturalOrder(), STRINGS, 16, 4, pool)) {
            sorter.add("x");
            sorter.sorted().close();
            assertThrows(IllegalStateException.class, sorter::ranges);
        }
    }

    /**
     * The term sorter of a build: rows of terms of every macro kind, many
     * rows to a term, so that a term's rows cross the stretches of a run and
     * the chunks of an intermediate merge. A term is in one range with all
     * its rows, whichever run they came from.
     */
    @Test
    void termRowsOfOneTermStayInOneRange() throws Exception {
        Random rnd = new Random(5);
        List<Node> terms = new ArrayList<>();
        for (int i = 0; i < 60; i++) {
            terms.add(NodeFactory.createURI("http://ex.org/entity/" + i));
            terms.add(NodeFactory.createBlankNode("b" + i));
            terms.add(NodeFactory.createLiteralString("text " + i));
            terms.add(NodeFactory.createLiteralDT(String.valueOf(i - 30), XSDDatatype.XSDinteger));
            terms.add(NodeFactory.createLiteralDT((i / 4.0) + "", XSDDatatype.XSDdouble));
            terms.add(NodeFactory.createLiteralLang("mot " + i, "fr"));
        }
        List<TermRow> input = new ArrayList<>();
        for (long row = 0; row < 12_000; row++) {
            // Skewed: a few terms take most of the rows.
            int which = (rnd.nextInt(4) == 0) ? rnd.nextInt(terms.size()) : rnd.nextInt(6);
            input.add(new TermRow(terms.get(which), row));
        }
        Comparator<TermRow> exact = HugeRecords.TERM_ORDER.thenComparingLong(TermRow::row);
        List<TermRow> expected = new ArrayList<>(input);
        expected.sort(exact);
        Path work = Files.createDirectories(dir.resolve("terms"));
        UltraSorterProvider provider = new UltraSorterProvider(48, 128, 3, pool);
        try (RecordSorter<TermRow> sorter = provider.termSorter(work, "ocol")) {
            for (TermRow tr : input) sorter.add(tr);
            assertTrue(sorter instanceof RangeSorter, "the hugeUltra term sorter has ranges");
            RangeSorter.Ranges<TermRow> ranges = ((RangeSorter<TermRow>) sorter).ranges();
            List<TermRow> bounds = new ArrayList<>();
            List<RangeSorter.Sample<TermRow>> samples = new ArrayList<>(ranges.samples());
            samples.sort((a, b) -> HugeRecords.TERM_ORDER.compare(a.record(), b.record()));
            for (int i = 2; i < samples.size(); i += 5) {
                TermRow candidate = samples.get(i).record();
                if (bounds.isEmpty() || HugeRecords.TERM_ORDER.compare(bounds.get(bounds.size() - 1), candidate) < 0) bounds.add(candidate);
            }
            assertTrue(bounds.size() > 5, "bounds to read by: " + bounds.size());
            List<TermRow> all = readRanges(ranges, bounds, HugeRecords.TERM_ORDER);
            // Rows of one term may come in any order of rows: compare as the sorted multiset.
            List<TermRow> got = new ArrayList<>(all);
            got.sort(exact);
            assertEquals(expected.size(), got.size());
            for (int i = 0; i < expected.size(); i++) {
                assertEquals(expected.get(i).term(), got.get(i).term(), "term at " + i);
                assertEquals(expected.get(i).row(), got.get(i).row(), "row at " + i);
            }
        }
    }
}
