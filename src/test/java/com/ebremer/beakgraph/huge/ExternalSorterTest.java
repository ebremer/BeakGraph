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
            try (RecordSorter.SortedCursor<Long> s = sorter.sorted()) {
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

    private static final ExternalSorter.Codec<String> STRINGS = new ExternalSorter.Codec<>() {
        @Override public void write(DataOutput out, String record) throws IOException { out.writeUTF(record); }
        @Override public String read(DataInput in) throws IOException { return in.readUTF(); }
    };

    private static List<String> bigStrings(int n, int length) {
        Random rnd = new Random(11);
        List<String> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            StringBuilder sb = new StringBuilder(length);
            sb.append(rnd.nextInt(1_000_000)).append('|');
            while (sb.length() < length) sb.append('x');
            out.add(sb.toString());
        }
        return out;
    }

    /** BG-125: a byte budget spills long before a generous record cap would. */
    @Test
    void byteBudgetSpillsBigRecordsBeforeTheRecordCap() throws Exception {
        List<String> input = bigStrings(200, 4096);
        List<String> expected = new ArrayList<>(input);
        expected.sort(Comparator.naturalOrder());
        try (ExternalSorter<String> sorter = new ExternalSorter<>(dir, "b", STRINGS, Comparator.naturalOrder(),
                1_000_000, 4, s -> 2L * s.length(), 64 << 10)) {
            for (String s : input) {
                sorter.add(s);
            }
            // 200 x ~8 KB estimated against a 64 KiB budget: a run every 8 records.
            assertTrue(sorter.runsSpilled() >= 20, "runs spilled on the byte budget: " + sorter.runsSpilled());
            List<String> actual = new ArrayList<>();
            try (RecordSorter.SortedCursor<String> s = sorter.sorted()) {
                s.forEachRemaining(actual::add);
            }
            assertEquals(expected, actual, "multi-level merge of byte-budgeted runs must still sort exactly");
        }
        try (var files = Files.list(dir)) {
            assertTrue(files.findAny().isEmpty(), "all spill runs must be deleted after consumption");
        }
    }

    /** BG-129: a run cut mid-record used to read as a clean, shorter run. */
    @Test
    void aTruncatedRunIsReportedNotSilentlyShortened() throws Exception {
        try (ExternalSorter<Long> sorter = new ExternalSorter<>(dir, "t2", LONGS, Comparator.naturalOrder(), 1_000, 4)) {
            Random rnd = new Random(3);
            for (int i = 0; i < 10_000; i++) {
                sorter.add(rnd.nextLong());
            }
            Path victim;
            try (var files = Files.list(dir)) {
                victim = files.filter(f -> f.getFileName().toString().startsWith("t2.run")).sorted().findFirst().orElseThrow();
            }
            try (java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(victim, java.nio.file.StandardOpenOption.WRITE)) {
                ch.truncate(ch.size() - 3);
            }
            Exception ex = org.junit.jupiter.api.Assertions.assertThrows(Exception.class, () -> {
                try (RecordSorter.SortedCursor<Long> s = sorter.sorted()) {
                    while (s.hasNext()) {
                        s.next();
                    }
                }
            });
            String messages = "";
            for (Throwable t = ex; t != null; t = t.getCause()) messages += t.getMessage() + " | ";
            assertTrue(messages.contains("truncated"), messages);
        }
    }

    @Test
    void withoutASizerOnlyTheRecordCapSpills() throws Exception {
        try (ExternalSorter<String> sorter = new ExternalSorter<>(dir, "r", STRINGS, Comparator.naturalOrder(), 1_000_000, 4)) {
            for (String s : bigStrings(200, 4096)) {
                sorter.add(s);
            }
            assertEquals(0, sorter.runsSpilled(), "no sizer: 200 records stay in RAM under a 1M cap");
        }
    }

    @Test
    void allInMemoryPathSkipsDisk() throws Exception {
        try (ExternalSorter<Long> sorter = new ExternalSorter<>(dir, "m", LONGS, Comparator.naturalOrder(), 1_000_000, 8)) {
            for (long v : new long[]{5, 3, 9, 1, 3}) {
                sorter.add(v);
            }
            List<Long> actual = new ArrayList<>();
            try (RecordSorter.SortedCursor<Long> s = sorter.sorted()) {
                s.forEachRemaining(actual::add);
            }
            assertEquals(List.of(1L, 3L, 3L, 5L, 9L), actual);
        }
        try (var files = Files.list(dir)) {
            assertFalse(files.findAny().isPresent(), "in-memory sort must not create files");
        }
    }
}
