package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.RDFNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression tests for C1: a variable repeated within one triple pattern must
 * only match rows where both positions hold the same term. BindingNodeId.put
 * previously kept the first value and silently ignored the second, so
 * {@code ?s ?p ?s} matched every triple.
 */
class RepeatedVariablePatternTest {

    // ex:loop is subject, predicate AND object of one triple (S=P=O);
    // ex:a and ex:c are S=O with distinct predicates; (a p b) and (d q e)
    // are ordinary triples that must NOT match repeated-variable patterns.
    private static final String TTL = """
        @prefix ex: <http://ex.org/> .
        ex:loop ex:loop ex:loop .
        ex:a ex:p ex:a .
        ex:a ex:p ex:b .
        ex:c ex:q ex:c .
        ex:d ex:q ex:e .
        """;

    private static final String PREFIX = "PREFIX ex: <http://ex.org/> ";

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;

    @BeforeAll
    static void buildAndOpen() throws Exception {
        File ttl = dir.resolve("repvar.ttl").toFile();
        File h5 = dir.resolve("repvar.ttl.h5").toFile();
        Files.write(ttl.toPath(), TTL.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder()
                .setSource(ttl).setDestination(h5)
                .setSpatial(false).setFeatures(false)
                .build().write();
        bg = new BeakGraph(new HDF5Reader(h5));
        ds = bg.getDataset();
    }

    @AfterAll
    static void closeReader() {
        if (bg != null) bg.close();
    }

    private static Set<String> select(String var, String query) {
        Set<String> uris = new HashSet<>();
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create(PREFIX + query)).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                QuerySolution qs = rs.next();
                RDFNode n = qs.get(var);
                if (n != null && n.isURIResource()) uris.add(n.asResource().getURI());
            }
        }
        return uris;
    }

    @Test
    void subjectObjectRepeatMatchesOnlyEqualRows() {
        // ?s ?p ?s : subject must equal object -> loop, a, c (NOT b/d/e rows).
        assertEquals(Set.of("http://ex.org/loop", "http://ex.org/a", "http://ex.org/c"),
            select("s", "SELECT ?s WHERE { ?s ?p ?s }"));
    }

    @Test
    void subjectObjectRepeatWithBoundPredicate() {
        // ?x ex:p ?x : only (a p a); (a p b) must not match.
        assertEquals(Set.of("http://ex.org/a"),
            select("x", "SELECT ?x WHERE { ?x ex:p ?x }"));
        assertEquals(Set.of("http://ex.org/c"),
            select("x", "SELECT ?x WHERE { ?x ex:q ?x }"));
    }

    @Test
    void subjectPredicateRepeatMatchesOnlyEqualRows() {
        // ?x ?x ?o : subject must equal predicate -> only the loop triple.
        // (Subject ids live in the entity dictionary, predicate ids in their own
        // dictionary, so this also exercises the cross-id-space comparison.)
        assertEquals(Set.of("http://ex.org/loop"),
            select("x", "SELECT ?x WHERE { ?x ?x ?o }"));
        assertEquals(Set.of("http://ex.org/loop"),
            select("o", "SELECT ?o WHERE { ?x ?x ?o }"));
    }

    @Test
    void tripleRepeatMatchesOnlyAllEqualRow() {
        // ?x ?x ?x : S = P = O -> only the loop triple.
        assertEquals(Set.of("http://ex.org/loop"),
            select("x", "SELECT ?x WHERE { ?x ?x ?x }"));
    }

    @Test
    void askVariantsAgree() {
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create(PREFIX + "ASK { ex:d ?p ex:d }")).build()) {
            assertFalse(qe.execAsk(), "ex:d is never its own object");
        }
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create(PREFIX + "ASK { ex:a ?p ex:a }")).build()) {
            assertTrue(qe.execAsk());
        }
    }
}
