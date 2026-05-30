package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
}
