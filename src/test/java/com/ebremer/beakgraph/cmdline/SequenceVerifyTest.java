package com.ebremer.beakgraph.cmdline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.RdfArraySequenceStorageTest;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@code -verify -deep} on format v7 stores (RDFARRY.md step 3): a store with
 * sequences passes, in both containers, and a store whose {@code numSlots}
 * disagrees with its elements is named for it.
 */
class SequenceVerifyTest {

    @TempDir
    Path dir;

    private File build(String name) throws Exception {
        Path src = dir.resolve("seq.trig");
        Files.writeString(src, RdfArraySequenceStorageTest.fixture());
        File out = dir.resolve(name).toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(out).build().write();
        return out;
    }

    private static String verify(File store, int expectedExit) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        int exit = new VerifyCommand(store, true, new PrintStream(bytes, true, StandardCharsets.UTF_8)).run();
        String report = bytes.toString(StandardCharsets.UTF_8);
        assertEquals(expectedExit, exit, report);
        return report;
    }

    @Test
    void aStoreWithSequencesVerifiesDeep() throws Exception {
        assertTrue(verify(build("seq.h5"), 0).contains("(format v7)"));
        verify(build("seq.zarr"), 0);
    }

    @Test
    void aSlotCountThatDisagreesIsNamed() throws Exception {
        File zarr = build("bad.zarr");
        // numSlots is a JSON attribute in a Zarr store: 4 slots, recorded as 3.
        int edited = 0;
        try (Stream<Path> files = Files.walk(zarr.toPath())) {
            for (Path f : files.filter(p -> p.getFileName().toString().equals("zarr.json")).toList()) {
                String json = Files.readString(f);
                String changed = json.replaceAll("\"numSlots\"\\s*:\\s*4", "\"numSlots\":3");
                if (!changed.equals(json)) {
                    Files.writeString(f, changed);
                    edited++;
                }
            }
        }
        assertTrue(edited >= 1, "the store records numSlots");
        String report = verify(zarr, 2);
        assertTrue(report.contains("holds 4 slots but numSlots records 3"), report);
    }
}
