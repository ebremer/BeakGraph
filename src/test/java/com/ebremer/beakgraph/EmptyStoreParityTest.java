package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.cmdline.VerifyCommand;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.jena.IndexExport;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.core.DatasetGraph;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * BG-327: a truly empty store - an empty source, VoID off (the default) - had
 * never been built or queried on any engine; the one empty-source test turns
 * VoID on and so always has quads. Every engine must produce the same file
 * shape as method 0 and a store that answers every read path with nothing:
 * dictionary, graph list, SPARQL counts, the index export and -verify.
 */
class EmptyStoreParityTest {

    @TempDir
    static Path dir;

    static Stream<WriterEngines.Engine> engines() {
        return WriterEngines.all();
    }

    private static long count(Dataset ds, String query) {
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(query)).build()) {
            return qe.execSelect().next().getLiteral("c").getLong();
        }
    }

    private static int rows(Dataset ds, String query) {
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(query)).build()) {
            int n = 0;
            var rs = qe.execSelect();
            while (rs.hasNext()) { rs.next(); n++; }
            return n;
        }
    }

    private static File reference() throws Exception {
        File ref = dir.resolve("reference.h5").toFile();
        if (!ref.exists()) {
            File src = dir.resolve("reference.ttl").toFile();
            Files.write(src.toPath(), new byte[0]);
            HDF5Writer.Builder().setSource(src).setDestination(ref).build().write();
        }
        return ref;
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("engines")
    void anEmptySourceBuildsAnEmptyStore(WriterEngines.Engine engine) throws Exception {
        engine.assumeAvailable();
        File src = dir.resolve(engine.name() + ".ttl").toFile();
        Files.write(src.toPath(), new byte[0]);
        File h5 = dir.resolve(engine.name() + ".h5").toFile();
        engine.buildStore(src, h5);   // VoidMode.NONE: nothing at all is written

        StoreParity.assertSameStructure(reference().toPath(), h5.toPath());
        try (BeakGraph bg = BG.getBeakGraph(h5)) {
            HDF5Reader reader = (HDF5Reader) bg.getReader();
            assertTrue(((PositionalDictionaryReader) reader.getDictionary()).isEmpty(), "empty dictionary");
            Dataset ds = bg.getDataset();
            DatasetGraph dsg = ds.asDatasetGraph();
            assertFalse(dsg.listGraphNodes().hasNext(), "no named graphs");
            assertFalse(dsg.containsGraph(NodeFactory.createURI("http://ex.org/g")));
            assertEquals(0, count(ds, "SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }"));
            assertEquals(0, count(ds, "SELECT (COUNT(DISTINCT ?s) AS ?c) WHERE { ?s ?p ?o }"));
            assertEquals(0, rows(ds, "SELECT DISTINCT ?p WHERE { ?s ?p ?o }"));
            assertEquals(0, rows(ds, "SELECT ?g WHERE { GRAPH ?g { ?s ?p ?o } }"));
            for (boolean quads : new boolean[]{true, false}) {
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                boolean fast = IndexExport.tryWrite(reader, os, quads);
                assertTrue(!fast || os.size() == 0, "the index export of an empty store writes nothing (quads=" + quads + ")");
            }
        }
        assertEquals(0, new VerifyCommand(h5, false).run(), "-verify");
        assertEquals(0, new VerifyCommand(h5, true).run(), "-verify (deep)");
    }
}
