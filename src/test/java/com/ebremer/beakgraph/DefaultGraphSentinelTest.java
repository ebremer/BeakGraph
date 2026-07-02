package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.lib.NodeComparator;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.sparql.core.Quad;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The two ARQ default-graph sentinel URIs are DISTINCT RDF terms. The
 * comparator ranked both "default" and returned 0 for the pair, so a source
 * that used urn:x-arq:DefaultGraphNode as a data term collapsed it onto the
 * same dictionary id as urn:x-arq:DefaultGraph - queries then returned the
 * wrong term or nothing.
 */
class DefaultGraphSentinelTest {

    @Test
    void sentinelTermsAreDistinctAndDefaultGraphSortsFirst() {
        NodeComparator cmp = NodeComparator.INSTANCE;
        Node dg = Quad.defaultGraphIRI;
        Node dgn = Quad.defaultGraphNodeGenerated;
        assertTrue(cmp.compare(dg, dgn) != 0, "distinct sentinel terms must not compare equal");
        assertTrue(cmp.compare(dg, dgn) < 0, "urn:x-arq:DefaultGraph must keep the lowest rank");
        assertEquals(-Integer.signum(cmp.compare(dgn, dg)), Integer.signum(cmp.compare(dg, dgn)));
        // Both sentinels still rank before ordinary terms.
        Node uri = NodeFactory.createURI("http://ex.org/a");
        assertTrue(cmp.compare(dg, uri) < 0);
        assertTrue(cmp.compare(dgn, uri) < 0);
        assertEquals(0, cmp.compare(dg, dg));
        assertEquals(0, cmp.compare(dgn, dgn));
    }

    @TempDir
    Path dir;

    @Test
    void sentinelUriAsDataTermRoundTrips() throws Exception {
        String ttl = """
            @prefix ex: <http://ex.org/> .
            <urn:x-arq:DefaultGraphNode> ex:p <urn:x-arq:DefaultGraph> .
            <urn:x-arq:DefaultGraphNode> ex:q "v" .
            ex:normal ex:p ex:other .
            """;
        File src = dir.resolve("sentinel.ttl").toFile();
        File h5 = dir.resolve("sentinel.ttl.h5").toFile();
        Files.write(src.toPath(), ttl.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setSource(src).setDestination(h5)
                .setSpatial(false).setFeatures(false).build().write();

        try (BeakGraph bg = new BeakGraph(new HDF5Reader(h5))) {
            Dataset ds = bg.getDataset();
            assertEquals(1, count(ds, "SELECT ?o WHERE { <urn:x-arq:DefaultGraphNode> ex:p ?o }"),
                    "the sentinel-URI subject must be findable as its own term");
            assertEquals(1, count(ds, "SELECT ?s WHERE { ?s ex:q \"v\" }"));
            assertEquals(3, count(ds, "SELECT * WHERE { ?s ?p ?o }"));
        }
    }

    private static int count(Dataset ds, String query) {
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create("PREFIX ex: <http://ex.org/> " + query)).build()) {
            ResultSet rs = qe.execSelect();
            int n = 0;
            while (rs.hasNext()) { rs.next(); n++; }
            return n;
        }
    }
}
