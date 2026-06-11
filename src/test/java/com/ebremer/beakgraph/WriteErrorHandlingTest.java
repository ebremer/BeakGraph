package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WriteErrorHandlingTest {

    @TempDir
    Path dir;

    @Test
    void malformedRdfAbortsTheWriteInsteadOfSwallowing() throws Exception {
        File ttl = dir.resolve("bad.ttl").toFile();
        File h5 = dir.resolve("bad.ttl.h5").toFile();
        Files.write(ttl.toPath(), "this is not valid turtle at all @@@ }}}".getBytes(StandardCharsets.UTF_8));

        // The parse error must propagate (the write aborts) rather than being swallowed,
        // which previously left a silently truncated dictionary behind.
        assertThrows(Exception.class, () ->
                HDF5Writer.Builder().setSource(ttl).setDestination(h5)
                        .setSpatial(false).setFeatures(false).build().write());

        // And no .h5 should have been produced for the failed input.
        assertFalse(h5.exists(), "a failed write must not leave a partial .h5 behind");
    }

    @Test
    void failedRebuildPreservesThePreviousGoodArtifact() throws Exception {
        File ttl = dir.resolve("data.ttl").toFile();
        File h5 = dir.resolve("data.ttl.h5").toFile();
        Files.write(ttl.toPath(),
                "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n".getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setSource(ttl).setDestination(h5)
                .setSpatial(false).setFeatures(false).build().write();
        byte[] good = Files.readAllBytes(h5.toPath());

        // Rebuild over the same destination with a broken source: the failure
        // happens before anything is written, and the old cleanup deleted the
        // previous, fully valid artifact anyway.
        Files.write(ttl.toPath(), "no longer valid turtle @@@".getBytes(StandardCharsets.UTF_8));
        assertThrows(Exception.class, () ->
                HDF5Writer.Builder().setSource(ttl).setDestination(h5)
                        .setSpatial(false).setFeatures(false).build().write());

        assertTrue(h5.exists(), "a failed rebuild must not destroy the previous artifact");
        assertArrayEquals(good, Files.readAllBytes(h5.toPath()),
                "the previous artifact must be byte-identical after a failed rebuild");
        assertFalse(dir.resolve("data.ttl.h5.tmp").toFile().exists(),
                "the temp file must be cleaned up after a failed build");
    }
}
