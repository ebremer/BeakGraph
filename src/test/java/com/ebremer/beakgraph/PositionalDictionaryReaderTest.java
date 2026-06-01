package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.Dictionary;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PositionalDictionaryReaderTest {

    @TempDir
    static Path dir;
    static File h5;

    @BeforeAll
    static void build() throws Exception {
        String ttl = """
            @prefix ex: <http://ex.org/> .
            ex:s ex:p ex:o .
            ex:s ex:n "hi" .
            """;
        File t = dir.resolve("d.ttl").toFile();
        h5 = dir.resolve("d.ttl.h5").toFile();
        Files.write(t.toPath(), ttl.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setSource(t).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
    }

    @Test
    void getObjectsReturnsACachedFunctionalInstance() throws Exception {
        try (HDF5Reader reader = new HDF5Reader(h5)) {
            PositionalDictionaryReader dict = (PositionalDictionaryReader) reader.getDictionary();

            Dictionary a = dict.getObjects();
            assertSame(a, dict.getObjects(), "getObjects() must return one cached instance, not a fresh wrapper per call");

            // The cached view still resolves objects correctly (locate <-> extract round-trip).
            Node o = NodeFactory.createURI("http://ex.org/o");
            long id = a.locate(o);
            assertTrue(id >= 1, "ex:o should be a known object");
            assertEquals(o, a.extract(id));
        }
    }
}
