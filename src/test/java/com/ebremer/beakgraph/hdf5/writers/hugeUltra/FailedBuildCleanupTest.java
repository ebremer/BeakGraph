package com.ebremer.beakgraph.hdf5.writers.hugeUltra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.huge.HugeHDF5Writer;
import com.ebremer.beakgraph.huge.StreamingHdf5;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * BG-134: a build that fails while spills and merges are in flight must
 * drain them before the workspace is removed - on Windows a run file still
 * open for writing cannot be deleted, and the whole {@code .bghugeultra-*}
 * directory used to survive next to the destination. BG-135: the installed
 * backend is probed before any parsing, so a missing or broken backend fails
 * in milliseconds instead of after the whole sort.
 */
class FailedBuildCleanupTest {

    @TempDir
    Path dir;

    private static String goodLines(int n) {
        StringBuilder nq = new StringBuilder();
        for (int i = 0; i < n; i++) {
            nq.append("<http://ex.org/s").append(i % 700).append("> <http://ex.org/p").append(i % 7)
              .append("> \"literal number ").append(i).append("\" .\n");
        }
        return nq.toString();
    }

    private static List<Path> entries(Path p) throws IOException {
        try (Stream<Path> s = Files.list(p)) {
            return s.toList();
        }
    }

    @Test
    void aBuildFailingMidSpillLeavesNoWorkspaceBehind() throws Exception {
        com.ebremer.beakgraph.NativeTestSupport.assumeNative();
        File src = dir.resolve("broken.nq").toFile();
        Files.writeString(src.toPath(), goodLines(6000) + "this is not an n-quads line @@@\n", StandardCharsets.UTF_8);
        Path work = Files.createDirectories(dir.resolve("work"));
        File dest = dir.resolve("broken.h5").toFile();
        assertThrows(IOException.class, () -> HugeUltraHDF5Writer.Builder().setSource(src).setDestination(dest)
                .setWorkDirectory(work).setCores(3)
                .setTermSpillBatch(64).setIdSpillBatch(128).setMergeFanIn(2)
                .build().write());
        assertEquals(List.of(), entries(work), "the workspace (and every in-flight run file) is gone");
        assertFalse(dest.exists(), "no destination was published");
        try (Stream<Path> s = Files.list(dir)) {
            assertTrue(s.noneMatch(p -> p.getFileName().toString().endsWith(".tmp")), "no temp output left next to the destination");
        }
    }

    @Test
    void aBrokenBackendFailsBeforeParsing() throws Exception {
        File src = dir.resolve("unparsed.nq").toFile();
        Files.writeString(src.toPath(), "this would fail to parse @@@\n", StandardCharsets.UTF_8);
        Path work = Files.createDirectories(dir.resolve("work2"));
        StreamingHdf5.setProvider(path -> {
            throw new IOException("no HDF5 here");
        });
        try {
            assertFalse(StreamingHdf5.isDefaultProvider());
            for (int method : new int[]{4, 1}) {
                File dest = dir.resolve("unparsed-" + method + ".h5").toFile();
                IOException ex = assertThrows(IOException.class, () -> {
                    if (method == 4) {
                        HugeUltraHDF5Writer.Builder().setSource(src).setDestination(dest).setWorkDirectory(work).setCores(2).build().write();
                    } else {
                        HugeHDF5Writer.Builder().setSource(src).setDestination(dest).setWorkDirectory(work).build().write();
                    }
                });
                assertTrue(ex.getMessage().contains("cannot write a file in"), "the probe, not the parser, failed: " + ex.getMessage());
                assertTrue(ex.getCause() != null && "no HDF5 here".equals(ex.getCause().getMessage()), String.valueOf(ex.getCause()));
                assertEquals(List.of(), entries(work), "the workspace is removed even after a probe failure");
                assertFalse(dest.exists());
            }
        } finally {
            StreamingHdf5.resetProvider();
        }
        assertTrue(StreamingHdf5.isDefaultProvider());
    }
}
