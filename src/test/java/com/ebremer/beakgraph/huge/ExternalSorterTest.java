package com.ebremer.beakgraph.huge;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * External merge sort correctness: tiny in-RAM batches and a small fan-in force
 * many spill runs and multi-level merges; the output must be a permutation of
 * the input in exact comparator order, and every spill file must be gone
 * afterwards.
 */
class ExternalSorterTest {

    @TempDir
    Path dir;

    private static final ExternalSorter.Codec<Long> LONGS = new ExternalSorter.Codec<>() {
        @Override public void write(DataOutput out, Long record) throws IOException { out.writeLong(record); }
        @Override public Long read(DataInput in) throws IOException { return in.readLong(); }
    };

    @Test
    void multiLevelMergeSortsAndCleansUp() throws Exception {
        Random rnd = new Random(7);
        int n = 100_000;
        List<Long> expected = new ArrayList<>(n);
        try (ExternalSorter<Long> sorter = new ExternalSorter<>(dir, "t", LONGS, Comparator.naturalOrder(), 1_000, 4)) {
            for (int i = 0; i < n; i++) {
                long v = rnd.nextLong(1_000_000); // duplicates likely
                expected.add(v);
                sorter.add(v);
            }
            expected.sort(Comparator.naturalOrder());
            assertEquals(n, sorter.size());
            List<Long> actual = new ArrayList<>(n);
            try (ExternalSorter.SortedStream<Long> s = sorter.sorted()) {
                while (s.hasNext()) {
                    actual.add(s.next());
                }
            }
            assertEquals(expected, actual, "sorted output must be the exact sorted multiset of the input");
        }
        try (var files = Files.list(dir)) {
            assertTrue(files.findAny().isEmpty(), "all spill runs must be deleted after consumption");
        }
    }

    @Test
    void allInMemoryPathSkipsDisk() throws Exception {
        try (ExternalSorter<Long> sorter = new ExternalSorter<>(dir, "m", LONGS, Comparator.naturalOrder(), 1_000_000, 8)) {
            for (long v : new long[]{5, 3, 9, 1, 3}) {
                sorter.add(v);
            }
            List<Long> actual = new ArrayList<>();
            try (ExternalSorter.SortedStream<Long> s = sorter.sorted()) {
                s.forEachRemaining(actual::add);
            }
            assertEquals(List.of(1L, 3L, 3L, 5L, 9L), actual);
        }
        try (var files = Files.list(dir)) {
            assertFalse(files.findAny().isPresent(), "in-memory sort must not create files");
        }
    }
}
