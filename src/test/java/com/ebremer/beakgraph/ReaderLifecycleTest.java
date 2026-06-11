package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.pool.BeakGraphPool;
import io.jhdf.HdfFile;
import io.jhdf.WritableHdfFile;
import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression tests for the reader lifecycle fixes:
 * <ul>
 *   <li>H5 - opening a structurally valid HDF5 file that is not a BeakGraph
 *       file must fail with a clear exception AND release the underlying file
 *       mapping (a leaked HdfFile pins the file lock on Windows).</li>
 *   <li>H6 - closing a named-graph wrapper obtained from the dataset must not
 *       close the shared reader under the rest of the dataset, and the keyed
 *       pool must not re-issue an instance whose reader has been closed.</li>
 * </ul>
 */
class ReaderLifecycleTest {

    private static final String TTL = """
        @prefix ex: <http://ex.org/> .
        ex:s ex:p ex:o .
        ex:s ex:p "v" .
        """;

    @TempDir
    static Path dir;
    static File h5;

    @BeforeAll
    static void build() throws Exception {
        File ttl = dir.resolve("life.ttl").toFile();
        h5 = dir.resolve("life.ttl.h5").toFile();
        Files.write(ttl.toPath(), TTL.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder()
                .setSource(ttl).setDestination(h5)
                .setSpatial(false).setFeatures(false)
                .build().write();
    }

    @AfterAll
    static void drainPool() {
        BeakGraphPool.getPool().clear(h5.toURI());
    }

    private static int count(Dataset ds) {
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create("SELECT * WHERE { ?s ?p ?o }")).build()) {
            ResultSet rs = qe.execSelect();
            int n = 0;
            while (rs.hasNext()) { rs.next(); n++; }
            return n;
        }
    }

    // --- H5 ---------------------------------------------------------------

    @Test
    void malformedFileIsRejectedAndReleased() throws Exception {
        Path notBG = dir.resolve("notbeakgraph.h5");
        try (WritableHdfFile out = HdfFile.write(notBG)) {
            out.putGroup("SomethingElse").putAttribute("hello", 1);
        }
        assertThrows(IllegalStateException.class, () -> new HDF5Reader(notBG.toFile()),
            "a non-BeakGraph HDF5 file must be rejected explicitly");
        // The failed constructor must have closed the mapped file: on Windows an
        // open HdfFile mapping makes this delete fail.
        Files.delete(notBG);
    }

    // --- H6: named-graph wrapper close ------------------------------------

    @Test
    void closingNamedGraphWrapperDoesNotKillDataset() throws Exception {
        try (BeakGraph bg = new BeakGraph(new HDF5Reader(h5))) {
            Dataset ds = bg.getDataset();
            // Routine Jena usage: obtain a named model, read it, close it - BEFORE
            // anything else has touched the file, so a close that wrongly cascades
            // to the shared reader cannot hide behind already-cached index buffers.
            ds.getNamedModel(Params.VOIDSTRING).close();
            assertTrue(count(ds) > 0,
                "closing a named-graph view must not close the shared reader");
            assertTrue(bg.getReader().isOpen(), "shared reader must remain open");
        }
    }

    // --- H6: pool must not re-issue a closed instance -----------------------

    @Test
    void poolDoesNotReissueClosedInstance() throws Exception {
        URI key = h5.toURI();
        BeakGraph first = BeakGraphPool.getPool().borrowObject(key);
        assertNotNull(first);
        // Poison the instance the way a buggy/abusive caller would, then return it.
        first.close();
        BeakGraphPool.getPool().returnObject(key, first);

        BeakGraph second = BeakGraphPool.getPool().borrowObject(key);
        try {
            // With testOnBorrow active and a working validateObject, the poisoned
            // instance is destroyed and a fresh one is created - so this works.
            assertTrue(count(second.getDataset()) > 0,
                "borrowed instance must be usable after a poisoned return");
        } finally {
            BeakGraphPool.getPool().returnObject(key, second);
        }
    }
}
