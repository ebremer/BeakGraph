package com.ebremer.beakgraph.huge;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** BG-129 / BG-124: a truncated record file is an error, and a RecordFile closes on close(). */
class RecordFileTest {

    @TempDir
    Path dir;

    private static final ExternalSorter.Codec<String> STRINGS = new ExternalSorter.Codec<>() {
        @Override public void write(DataOutput out, String record) throws IOException { out.writeUTF(record); }
        @Override public String read(DataInput in) throws IOException { return in.readUTF(); }
    };

    @Test
    void intactFileReadsEveryRecordAndTruncatedFileFails() throws Exception {
        Path path = dir.resolve("records.bin");
        RecordFile<String> rf = new RecordFile<>(path, STRINGS);
        for (int i = 0; i < 100; i++) {
            rf.append("record-" + i);
        }
        rf.finish();
        assertEquals(100, rf.count());
        List<String> back = new ArrayList<>();
        try (RecordSorter.SortedCursor<String> c = rf.read()) {
            while (c.hasNext()) back.add(c.next());
        }
        assertEquals(100, back.size());
        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.WRITE)) {
            ch.truncate(ch.size() - 2);
        }
        UncheckedIOException ex = org.junit.jupiter.api.Assertions.assertThrows(UncheckedIOException.class, () -> {
            try (RecordSorter.SortedCursor<String> c = rf.read()) {
                while (c.hasNext()) c.next();
            }
        });
        assertTrue(ex.getCause().getMessage().contains("truncated: read 99 of 100"), ex.getCause().getMessage());
        rf.close();
        assertTrue(Files.notExists(path), "close() removes the file");
    }

    @Test
    void closeMidWriteReleasesTheStream() throws Exception {
        Path path = dir.resolve("open.bin");
        RecordFile<String> rf = new RecordFile<>(path, STRINGS);
        rf.append("one");   // stream open, never finished
        rf.close();
        assertTrue(Files.notExists(path));
        assertTrue(Files.deleteIfExists(path) == false);
    }
}
