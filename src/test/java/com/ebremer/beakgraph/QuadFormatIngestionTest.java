package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression tests for source-syntax handling: the parser language must be
 * detected from the file name instead of being hardcoded to Turtle - BeakGraph
 * is a quad store, and named graphs can only arrive through a quad-capable
 * syntax (TriG, N-Quads).
 */
class QuadFormatIngestionTest {

    @TempDir
    static Path dir;

    private static int count(Dataset ds, String query) {
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create("PREFIX ex: <http://ex.org/> " + query)).build()) {
            ResultSet rs = qe.execSelect();
            int n = 0;
            while (rs.hasNext()) { rs.next(); n++; }
            return n;
        }
    }

    @Test
    void trigSourceWithNamedGraphRoundTrips() throws Exception {
        String trig = """
            @prefix ex: <http://ex.org/> .
            ex:s0 ex:p ex:o0 .
            ex:g1 {
                ex:s1 ex:p ex:o1 .
                ex:s2 ex:p ex:o2 .
            }
            """;
        File src = dir.resolve("data.trig").toFile();
        File h5 = dir.resolve("data.trig.h5").toFile();
        Files.write(src.toPath(), trig.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setVoidMode(com.ebremer.beakgraph.core.VoidMode.EXACT).setSource(src).setDestination(h5)
                .setSpatial(false).setFeatures(false).build().write();
        try (BeakGraph bg = new BeakGraph(new HDF5Reader(h5))) {
            Dataset ds = bg.getDataset();
            assertEquals(2, count(ds, "SELECT * WHERE { GRAPH ex:g1 { ?s ?p ?o } }"),
                "the TriG named graph must be stored and queryable");
            assertEquals(1, count(ds, "SELECT * WHERE { ?s ex:p ex:o0 }"),
                "the TriG default graph must be stored");
            assertTrue(ds.asDatasetGraph().containsGraph(
                    org.apache.jena.graph.NodeFactory.createURI("http://ex.org/g1")));
        }
    }

    @Test
    void nquadsSourceRoundTrips() throws Exception {
        String nq = """
            <http://ex.org/s1> <http://ex.org/p> <http://ex.org/o1> <http://ex.org/g2> .
            <http://ex.org/s2> <http://ex.org/p> <http://ex.org/o2> .
            """;
        File src = dir.resolve("data.nq").toFile();
        File h5 = dir.resolve("data.nq.h5").toFile();
        Files.write(src.toPath(), nq.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setVoidMode(com.ebremer.beakgraph.core.VoidMode.EXACT).setSource(src).setDestination(h5)
                .setSpatial(false).setFeatures(false).build().write();
        try (BeakGraph bg = new BeakGraph(new HDF5Reader(h5))) {
            Dataset ds = bg.getDataset();
            assertEquals(1, count(ds, "SELECT * WHERE { GRAPH <http://ex.org/g2> { ?s ?p ?o } }"));
            assertEquals(1, count(ds, "SELECT * WHERE { ?s ex:p <http://ex.org/o2> }"));
        }
    }

    @Test
    void emptySourceProducesConsistentFile() throws Exception {
        File src = dir.resolve("empty.ttl").toFile();
        File h5 = dir.resolve("empty.ttl.h5").toFile();
        Files.write(src.toPath(), "# nothing here\n".getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setVoidMode(com.ebremer.beakgraph.core.VoidMode.EXACT).setSource(src).setDestination(h5)
                .setSpatial(false).setFeatures(false).build().write();
        try (BeakGraph bg = new BeakGraph(new HDF5Reader(h5))) {
            Dataset ds = bg.getDataset();
            // The VoID metadata graph is always written; the columnar graph list
            // must therefore exist too, or enumeration/union/contains disagree
            // with direct GRAPH queries on the same file.
            int direct = count(ds, "SELECT * WHERE { GRAPH <" + Params.VOIDSTRING + "> { ?s ?p ?o } }");
            assertTrue(direct > 0, "VoID metadata must be present");
            assertEquals(Boolean.TRUE,
                ds.asDatasetGraph().containsGraph(org.apache.jena.graph.NodeFactory.createURI(Params.VOIDSTRING)),
                "containsGraph must agree with the stored VoID graph");
            assertEquals(direct, count(ds, "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }"),
                "graph enumeration must see the same rows as the direct query");
        }
    }
}
