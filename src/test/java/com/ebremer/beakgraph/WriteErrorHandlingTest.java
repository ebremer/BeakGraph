package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A failed build must leave nothing behind and destroy nothing that was
 * there - on EVERY engine (BG-421): the tmp/move/cleanup sequence is written
 * six times (HDF5Writer, Ultra, Parallel, Huge, HugeUltra, Plaid), the
 * native ones additionally own a workspace directory, and only method 0 was
 * covered. Each engine builds into the destination's directory, so that
 * directory is the whole observable surface: no partial .h5, no .tmp, no
 * retained .new, no .bghuge-* / .bghugeultra-* / .bgplaid-* workspace.
 */
class WriteErrorHandlingTest {

    @TempDir
    Path dir;

    static Stream<WriterEngines.Engine> engines() {
        return WriterEngines.all();
    }

    /** Everything in the destination directory except the source and the store itself. */
    private List<String> leftovers(File h5, File src) throws Exception {
        try (Stream<Path> list = Files.list(dir)) {
            return list.map(p -> p.getFileName().toString())
                    .filter(n -> !n.equals(h5.getName()) && !n.equals(src.getName()))
                    .sorted().toList();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("engines")
    void malformedRdfAbortsTheWriteInsteadOfSwallowing(WriterEngines.Engine engine) throws Exception {
        engine.assumeAvailable();
        File ttl = dir.resolve("bad.ttl").toFile();
        File h5 = dir.resolve("bad.ttl.h5").toFile();
        Files.write(ttl.toPath(), "this is not valid turtle at all @@@ }}}".getBytes(StandardCharsets.UTF_8));

        // The parse error must propagate (the write aborts) rather than being
        // swallowed, which previously left a silently truncated dictionary behind.
        assertThrows(Exception.class, () -> engine.buildStore(ttl, h5));

        assertFalse(h5.exists(), "a failed write must not leave a partial .h5 behind");
        assertEquals(List.of(), leftovers(h5, ttl), "no temp file, retained build or workspace may survive a failed build");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("engines")
    void failedRebuildPreservesThePreviousGoodArtifact(WriterEngines.Engine engine) throws Exception {
        engine.assumeAvailable();
        File ttl = dir.resolve("data.ttl").toFile();
        File h5 = dir.resolve("data.ttl.h5").toFile();
        Files.write(ttl.toPath(),
                "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n".getBytes(StandardCharsets.UTF_8));
        engine.buildStore(ttl, h5);
        byte[] good = Files.readAllBytes(h5.toPath());
        assertEquals(List.of(), leftovers(h5, ttl), "a successful build leaves only the store");

        // Rebuild over the same destination with a broken source: the failure
        // happens before anything is written, and the old cleanup deleted the
        // previous, fully valid artifact anyway.
        Files.write(ttl.toPath(), "no longer valid turtle @@@".getBytes(StandardCharsets.UTF_8));
        assertThrows(Exception.class, () -> engine.buildStore(ttl, h5));

        assertTrue(h5.exists(), "a failed rebuild must not destroy the previous artifact");
        assertArrayEquals(good, Files.readAllBytes(h5.toPath()),
                "the previous artifact must be byte-identical after a failed rebuild");
        assertEquals(List.of(), leftovers(h5, ttl), "the temp file and any workspace must be cleaned up after a failed build");
    }
}
