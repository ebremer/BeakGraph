package com.ebremer.beakgraph.hdf5.writers.hugeUltra;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.huge.HugeRecords;
import com.ebremer.beakgraph.huge.HugeRecords.IdQuad;
import com.ebremer.beakgraph.huge.HugeRecords.RowId;
import com.ebremer.beakgraph.huge.RecordSorter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * BG-137: the TWO-WORD packed key regime (totalBits > 63) is the one every
 * real -method 4/5 store runs in (a million entities is ~70 bits), yet no
 * fixture reached it. This drives PackedQuadSorter and PackedRowIdSorter
 * through 111-bit, 67-bit (a component straddling bit 64) and single-word
 * layouts with tiny batches and fan-in - many spill runs, several merge
 * levels - and requires the exact multiset in exact comparator order, with
 * every component round-tripping (maxima, 2^k-1 / 2^k values, duplicates).
 * No native HDF5 needed, so it runs on every CI leg.
 */
class PackedSortersTest {

    @TempDir
    Path dir;

    private final ForkJoinPool pool = new ForkJoinPool(3);

    @AfterEach
    void shutdown() {
        pool.shutdownNow();
    }

    private static long pick(Random rnd, long max) {
        switch (rnd.nextInt(8)) {
            case 0: return max;
            case 1: return Math.max(0, max - 1);
            case 2: return 0;
            case 3: return 1;
            case 4: {
                int k = rnd.nextInt(Math.max(1, 64 - Long.numberOfLeadingZeros(max)));
                return Math.min(max, (1L << k) - 1);
            }
            case 5: {
                int k = rnd.nextInt(Math.max(1, 64 - Long.numberOfLeadingZeros(max)));
                return Math.min(max, 1L << k);
            }
            default: return (max == 0) ? 0 : Math.floorMod(rnd.nextLong(), max + 1);
        }
    }

    private void quadRoundTrip(Index order, long entities, long predicates, long objects, long seed) throws Exception {
        Random rnd = new Random(seed);
        List<IdQuad> input = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            IdQuad q = new IdQuad(pick(rnd, entities), pick(rnd, entities), pick(rnd, predicates), pick(rnd, objects));
            input.add(q);
            if (rnd.nextInt(10) == 0) input.add(q);   // duplicates survive (dedup is the consumer's job)
        }
        List<IdQuad> expected = new ArrayList<>(input);
        expected.sort(order == Index.GSPO ? HugeRecords.GSPO_ORDER : HugeRecords.GPOS_ORDER);
        Path work = Files.createDirectories(dir.resolve(order.name() + "-" + seed));
        List<IdQuad> actual = new ArrayList<>();
        try (PackedQuadSorter sorter = new PackedQuadSorter(work, "q", order, entities, predicates, objects, 64, 2, pool, pool, 2)) {
            for (IdQuad q : input) sorter.add(q);
            assertEquals(input.size(), sorter.size());
            try (RecordSorter.SortedCursor<IdQuad> c = sorter.sorted()) {
                while (c.hasNext()) actual.add(c.next());
            }
        }
        assertEquals(expected, actual, order + " entities=" + entities + " predicates=" + predicates + " objects=" + objects);
        try (var files = Files.list(work)) {
            assertEquals(0, files.count(), "every spill run is consumed and deleted");
        }
    }

    @Test
    void twoWordQuadKeysSortAndRoundTrip() throws Exception {
        // 31 + 31 + 21 + 32 = 115 bits: three components live in the high word.
        quadRoundTrip(Index.GSPO, 1L << 30, 1L << 20, 1L << 31, 1);
        quadRoundTrip(Index.GPOS, 1L << 30, 1L << 20, 1L << 31, 2);
        // 21 + 21 + 10 + 22 = 74 bits: the first component straddles bit 64
        // in GSPO (g), and o/s straddle in GPOS.
        quadRoundTrip(Index.GSPO, 1L << 20, 1L << 9, 1L << 21, 3);
        quadRoundTrip(Index.GPOS, 1L << 20, 1L << 9, 1L << 21, 4);
        // Exactly one bit over a word (17 + 17 + 13 + 17 = 64 bits) and the largest legal width.
        quadRoundTrip(Index.GSPO, (1L << 17) - 1, (1L << 13) - 1, (1L << 17) - 1, 5);
        quadRoundTrip(Index.GPOS, (1L << 40) - 1, (1L << 5) - 1, (1L << 40) - 1, 6);
    }

    @Test
    void singleWordQuadKeysStillSort() throws Exception {
        quadRoundTrip(Index.GSPO, 400, 5, 300, 7);
        quadRoundTrip(Index.GPOS, 400, 5, 300, 8);
    }

    @Test
    void twoWordRowIdKeysSortAndRoundTrip() throws Exception {
        long maxRow = 1L << 34, maxId = 1L << 33;   // 35 + 34 = 69 bits, the row straddles the words
        Random rnd = new Random(9);
        List<RowId> input = new ArrayList<>();
        for (int i = 0; i < 4000; i++) {
            input.add(new RowId(pick(rnd, maxRow), pick(rnd, maxId)));
        }
        List<RowId> expected = new ArrayList<>(input);
        expected.sort(Comparator.comparingLong(RowId::row).thenComparingLong(RowId::id));
        Path work = Files.createDirectories(dir.resolve("rowid"));
        List<RowId> actual = new ArrayList<>();
        try (PackedRowIdSorter sorter = new PackedRowIdSorter(work, "r", maxRow, maxId, 64, 2, pool, pool, 2)) {
            for (RowId r : input) sorter.add(r);
            try (RecordSorter.SortedCursor<RowId> c = sorter.sorted()) {
                while (c.hasNext()) actual.add(c.next());
            }
        }
        assertEquals(expected, actual);
        // Row order is what the join needs; equal rows are ordered by id in the packed key.
        List<RowId> byRow = new ArrayList<>(actual);
        byRow.sort(HugeRecords.ROW_ORDER);
        assertEquals(byRow, actual);
    }

    /** BG-129: a fixed-width run cut mid-key used to read as a clean, shorter run. */
    @Test
    void aTruncatedRunIsReported() throws Exception {
        Path work = Files.createDirectories(dir.resolve("truncated"));
        try (PackedLongSorter sorter = new PackedLongSorter(work, "p", 63, 64, 2, pool, pool, 2)) {
            Random rnd = new Random(5);
            for (int i = 0; i < 1000; i++) {
                sorter.add(0, rnd.nextLong() >>> 1);
            }
            Path victim;
            try (var files = Files.list(work)) {
                victim = files.filter(f -> f.getFileName().toString().startsWith("p.prun")).sorted().findFirst().orElseThrow();
            }
            // The last run may still be spilling in the background; the first
            // one is complete once its successor exists.
            try (java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(victim, java.nio.file.StandardOpenOption.WRITE)) {
                ch.truncate(ch.size() - 5);
            }
            Exception ex = org.junit.jupiter.api.Assertions.assertThrows(Exception.class, () -> {
                try (PackedLongSorter.KeyCursor c = sorter.sorted()) {
                    while (c.advance()) { }
                }
            });
            String messages = "";
            for (Throwable t = ex; t != null; t = t.getCause()) messages += t.getMessage() + " | ";
            org.junit.jupiter.api.Assertions.assertTrue(messages.contains("truncated"), messages);
        }
    }

    @Test
    void extractCoversEveryStraddle() {
        // A 111-bit key: fields of 31, 31, 21, 28 bits (off3 = 0, off2 = 28, off1 = 49, off0 = 80).
        int b3 = 28, b2 = 21, b1 = 31;
        long v0 = (1L << 31) - 5, v1 = (1L << 31) - 1, v2 = 1L << 20, v3 = (1L << 28) - 3;
        long lo = v0;
        long hi = 0;
        hi = (hi << b1) | (lo >>> (64 - b1)); lo = (lo << b1) | v1;
        hi = (hi << b2) | (lo >>> (64 - b2)); lo = (lo << b2) | v2;
        hi = (hi << b3) | (lo >>> (64 - b3)); lo = (lo << b3) | v3;
        assertEquals(v3, PackedQuadSorter.extract(hi, lo, 0, b3), "low word only");
        assertEquals(v2, PackedQuadSorter.extract(hi, lo, b3, b2), "ends at bit 49");
        assertEquals(v1, PackedQuadSorter.extract(hi, lo, b2 + b3, b1), "straddles bit 64");
        assertEquals(v0, PackedQuadSorter.extract(hi, lo, b1 + b2 + b3, 63), "high word only");
    }
}
