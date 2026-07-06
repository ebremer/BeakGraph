package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.jena.NodeId;
import com.ebremer.beakgraph.hdf5.jena.NodeType;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A URI used both as a predicate AND as a subject/object lives in two different
 * dictionary id-spaces. These tests confirm such "dual-role" URIs resolve
 * correctly no matter how they enter a query (as a literal predicate, bound by
 * VALUES/BIND, or bound by an earlier triple in the same BGP).
 */
class DualRoleUriTest {

    private static final String TTL = """
        @prefix ex:  <http://ex.org/> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        ex:knows rdf:type rdf:Property .
        ex:knows ex:label "KNOWS" .
        ex:alice ex:knows ex:bob .
        ex:bob   ex:knows ex:carol .
        ex:alice ex:label "ALICE" .
        """;

    private static final String P = "PREFIX ex: <http://ex.org/> ";

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;
    static File h5;

    @BeforeAll
    static void build() throws Exception {
        File ttl = dir.resolve("dual.ttl").toFile();
        h5 = dir.resolve("dual.ttl.h5").toFile();
        Files.write(ttl.toPath(), TTL.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setSource(ttl).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        bg = new BeakGraph(new HDF5Reader(h5));
        ds = bg.getDataset();
    }

    @org.junit.jupiter.api.AfterAll
    static void closeReader() {
        // Release the mapped file: a leaked reader makes @TempDir cleanup flaky on Windows.
        if (bg != null) bg.close();
    }

    private static String one(String var, String query) {
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(P + query)).build()) {
            ResultSet rs = qe.execSelect();
            if (!rs.hasNext()) return null;
            var sol = rs.next();
            String v = sol.get(var).isLiteral() ? sol.getLiteral(var).getLexicalForm() : sol.getResource(var).getURI();
            return rs.hasNext() ? "<multiple>" : v;
        }
    }

    @Test
    void usedAsPredicate() {
        // ex:knows in its predicate role.
        assertEquals("http://ex.org/bob", one("o", "SELECT ?o WHERE { ex:alice ex:knows ?o }"));
    }

    @Test
    void usedAsSubject() {
        // ex:knows in its subject (entity) role.
        assertEquals("KNOWS", one("l", "SELECT ?l WHERE { ex:knows ex:label ?l }"));
    }

    @Test
    void boundByValuesThenUsedAsSubject() {
        // Enters as a bound value (VALUES), then is used at a subject position.
        assertEquals("KNOWS", one("l", "SELECT ?l WHERE { VALUES ?v { ex:knows } ?v ex:label ?l }"));
    }

    @Test
    void boundByBindThenUsedAsSubject() {
        assertEquals("KNOWS", one("l", "SELECT ?l WHERE { BIND(ex:knows AS ?v) ?v ex:label ?l }"));
    }

    @Test
    void boundAsPredicateThenUsedAsSubjectInSameBgp() {
        // ?v is bound by matching it in a PREDICATE position, then reused at a
        // SUBJECT position within the same basic graph pattern.
        assertEquals("KNOWS", one("l", "SELECT ?l WHERE { ex:alice ?v ex:bob . ?v ex:label ?l }"));
    }

    @Test
    void forwardNodeIdMappingIsStableForDualRoleUri() throws Exception {
        // The node table's Node -> NodeId mapping must stay deterministic for a
        // dual-role URI, even after the other role's id has been reconstructed in the
        // reverse direction (which previously flipped the cached mapping).
        try (HDF5Reader reader = new HDF5Reader(h5)) {
            NodeTable nt = reader.getNodeTable();
            Node knows = NodeFactory.createURI("http://ex.org/knows");

            long first = nt.getNodeIdForNode(knows);

            // Reconstruct ex:knows in its entity (subject) role via the reverse mapping.
            long entityId = reader.getDictionary().getSubjects().locate(knows);
            nt.getNodeForNodeId(NodeId.pack(NodeType.SUBJECT, entityId));

            long second = nt.getNodeIdForNode(knows);
            assertEquals(first, second, "dual-role URI must keep a stable Node -> NodeId mapping");
        }
    }
}
