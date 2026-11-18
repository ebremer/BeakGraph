package com.ebremer.beakgraph.huge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link RangeIds}: what a range's merge writes for a column - each row with
 * the code of its term - is what the id stage reads back, in the order
 * written, a term's rows together; a term of more rows than a group holds is
 * several groups of one code; and a file cut short is told from a whole one.
 */
class RangeIdsTest {

    @TempDir
    Path dir;

    private record Row(long code, long row) {}

    private static List<Row> readAll(RangeIds ids) throws IOException {
        List<Row> out = new ArrayList<>();
        try (RangeIds.Reader groups = ids.read()) {
            long last = -1;
            while (groups.next()) {
                assertTrue(groups.size >= 1 && groups.size <= RangeIds.GROUP_ROWS);
                assertTrue(groups.code >= last, "codes never descend");
                last = groups.code;
                for (int i = 0; i < groups.size; i++) out.add(new Row(groups.code, groups.rows[i]));
            }
        }
        return out;
    }

    @Test
    void rowsComeBackInOrderWithTheirCodes() throws Exception {
        Random rnd = new Random(1);
        List<Row> rows = new ArrayList<>();
        long code = 2;
        for (int term = 0; term < 3_000; term++) {
            // One row for most terms, a few thousand for some: past a group, and past the write buffer.
            int n = (term % 500 == 7) ? 3 * RangeIds.GROUP_ROWS + 5 : 1 + rnd.nextInt(3);
            for (int i = 0; i < n; i++) rows.add(new Row(code, Math.abs(rnd.nextLong()) >>> rnd.nextInt(60)));
            code += 1 + rnd.nextInt(3);   // entity codes and literal codes (odd) both
        }
        RangeIds ids = new RangeIds(dir.resolve("ocol.ids.0"));
        for (Row r : rows) ids.add(r.code(), r.row());
        ids.finish();
        assertEquals(rows.size(), ids.count());
        assertEquals(rows, readAll(ids));
        assertEquals(rows, readAll(ids), "read as often as asked");
        ids.close();
        assertTrue(Files.notExists(dir.resolve("ocol.ids.0")), "close() removes the file");
    }

    @Test
    void aTermOfManyRowsIsSeveralGroupsOfOneCode() throws Exception {
        RangeIds ids = new RangeIds(dir.resolve("gcol.ids.0"));
        int n = 2 * RangeIds.GROUP_ROWS + 1;
        for (int i = 0; i < n; i++) ids.add(2, i);
        ids.finish();
        int groups = 0;
        long rows = 0;
        try (RangeIds.Reader reader = ids.read()) {
            while (reader.next()) {
                assertEquals(2, reader.code);
                groups++;
                rows += reader.size;
            }
        }
        assertEquals(3, groups);
        assertEquals(n, rows);
        ids.close();
    }

    @Test
    void aRangeWithoutRowsIsAnEmptyFile() throws Exception {
        Path path = dir.resolve("scol.ids.3");
        RangeIds ids = new RangeIds(path);
        ids.finish();
        assertTrue(Files.exists(path));
        assertEquals(0, Files.size(path));
        assertEquals(List.of(), readAll(ids));
        ids.close();
    }

    @Test
    void aFileCutShortIsNotReadAsWhole() throws Exception {
        Path path = dir.resolve("cut.ids");
        RangeIds ids = new RangeIds(path);
        for (int term = 1; term <= 200; term++) {
            for (int i = 0; i < 5; i++) ids.add(term * 2L, term * 1000L + i);
        }
        ids.finish();
        long size = Files.size(path);
        // Inside the last group, and at its beginning (a clean end of the groups before it).
        for (long cut : new long[]{size - 2, size - 13}) {
            Path copy = dir.resolve("cut-" + cut + ".ids");
            Files.copy(path, copy);
            try (FileChannel ch = FileChannel.open(copy, StandardOpenOption.WRITE)) {
                ch.truncate(cut);
            }
            Files.move(copy, path, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            assertThrows(IOException.class, () -> readAll(ids), "cut at " + cut + " of " + size);
        }
        ids.close();
    }

    @Test
    void unfinishedOrFinishedTwice() throws Exception {
        RangeIds ids = new RangeIds(dir.resolve("state.ids"));
        ids.add(2, 1);
        assertThrows(IllegalStateException.class, ids::read);
        ids.finish();
        ids.finish();
        assertThrows(IllegalStateException.class, () -> ids.add(2, 2));
        assertEquals(1, readAll(ids).size());
        ids.close();
        ids.close();
        // Closed before it was finished (a failed build): the stream is released, the file removed.
        RangeIds open = new RangeIds(dir.resolve("open.ids"));
        for (int i = 0; i < 200_000; i++) open.add(2, i);   // past the buffer: the file is open
        assertTrue(Files.exists(dir.resolve("open.ids")));
        open.close();
        assertTrue(Files.notExists(dir.resolve("open.ids")));
    }
}
