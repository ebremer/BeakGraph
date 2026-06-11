package com.ebremer.beakgraph.pool;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.pool2.PooledObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Pool validation must actually probe the reader (testOnBorrow is on): an
 * isOpen-only check re-issued a poisoned instance to every borrower forever,
 * since a reader whose backing data went bad still reports "open".
 */
class BeakGraphPoolValidationTest {

    @TempDir
    Path dir;

    @Test
    void validateObjectProbesRealDataAndRejectsClosedReaders() throws Exception {
        File ttl = dir.resolve("pool.ttl").toFile();
        File h5 = dir.resolve("pool.ttl.h5").toFile();
        Files.write(ttl.toPath(),
                "<http://ex.org/a> <http://ex.org/p> <http://ex.org/b> .\n".getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setSource(ttl).setDestination(h5)
                .setSpatial(false).setFeatures(false).build().write();

        BeakGraphPoolFactory factory = new BeakGraphPoolFactory();
        BeakGraph bg = factory.create(h5.toURI());
        PooledObject<BeakGraph> pooled = factory.wrap(bg);
        try {
            assertTrue(factory.validateObject(h5.toURI(), pooled),
                    "a healthy instance must validate (probe included)");
        } finally {
            bg.close();
        }
        assertFalse(factory.validateObject(h5.toURI(), pooled),
                "a closed instance must fail validation and be discarded");
    }
}
