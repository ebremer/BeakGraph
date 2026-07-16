package com.ebremer.beakgraph;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.jena.AggregateCountFastPath;
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
import org.apache.jena.sparql.core.Quad;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Whole-graph COUNT aggregates answered from index structure
 * ({@link AggregateCountFastPath} over {@link com.ebremer.beakgraph.hdf5.readers.IndexCounts})
 * must agree exactly with scan-computed counts, and every unsupported shape must
 * fall back with unchanged results. HITS distinguishes the two paths.
 */
class AggregateCountFastPathTest {

    private static final String NS = "http://ex.org/";
    private static final String PREFIX = "PREFIX ex: <" + NS + ">\n";

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;

    @BeforeAll
    static void build() throws Exception {
        // Default graph: 4 triples, 3 subjects, 4 predicates, 4 objects.
        // g1: 2 triples (1 subject); g2: 1 triple.
        String trig = """
                @prefix ex: <http://ex.org/> .
                ex:s0 ex:p0 ex:o0 .
                ex:s0 ex:p1 "lit" .
                ex:s1 ex:shared ex:s0 .
                ex:s2 ex:refl ex:s2 .
                ex:g1 { ex:a ex:p2 ex:b . ex:a ex:shared ex:c . }
                ex:g2 { ex:d ex:p3 "x" . }
                """;
        File src = dir.resolve("agg.trig").toFile();
        File h5 = dir.resolve("agg.trig.h5").toFile();
        Files.write(src.toPath(), trig.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setSource(src).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        bg = new BeakGraph(new HDF5Reader(h5));
        ds = bg.getDataset();
    }

    @AfterAll
    static void close() {
        if (bg != null) bg.close();
    }

    /** Runs a one-row/one-var aggregate query and returns the count. */
    private static long count(String queryBody) {
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create(PREFIX + queryBody)).build()) {
            ResultSet rs = qe.execSelect();
            String var = rs.getResultVars().get(0);
            long n = rs.next().getLiteral(var).getLong();
            assertEquals(false, rs.hasNext(), "aggregate must yield exactly one row");
            return n;
        }
    }

    private static void check(String queryBody, long expected, boolean expectFastPath) {
        long before = AggregateCountFastPath.HITS.get();
        assertEquals(expected, count(queryBody), "count for: " + queryBody);
        long used = AggregateCountFastPath.HITS.get() - before;
        assertEquals(expectFastPath ? 1 : 0, used, "fast-path activations for: " + queryBody);
    }

    @Test
    void countStarDefaultGraph() {
        check("SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }", 4, true);
    }

    @Test
    void countBoundVarEqualsRowCount() {
        check("SELECT (COUNT(?o) AS ?n) WHERE { ?s ?p ?o }", 4, true);
    }

    @Test
    void countDistinctStarEqualsRowCount() {
        check("SELECT (COUNT(DISTINCT *) AS ?n) WHERE { ?s ?p ?o }", 4, true);
    }

    @Test
    void countDistinctSubjects() {
        check("SELECT (COUNT(DISTINCT ?s) AS ?numSub) WHERE { ?s ?p ?o }", 3, true);
    }

    @Test
    void countDistinctPredicates() {
        check("SELECT (COUNT(DISTINCT ?p) AS ?n) WHERE { ?s ?p ?o }", 4, true);
    }

    @Test
    void countDistinctObjectsFallsBack() {
        // No direct index level for distinct objects - must scan, same answer.
        check("SELECT (COUNT(DISTINCT ?o) AS ?n) WHERE { ?s ?p ?o }", 4, false);
    }

    @Test
    void namedGraphCounts() {
        check("SELECT (COUNT(*) AS ?n) WHERE { GRAPH ex:g1 { ?s ?p ?o } }", 2, true);
        check("SELECT (COUNT(DISTINCT ?s) AS ?n) WHERE { GRAPH ex:g1 { ?s ?p ?o } }", 1, true);
    }

    @Test
    void nonGraphEntityCountsZero() {
        // ex:s0 is an entity but not a graph: its first-level slot is a padding
        // block, which must read as empty - not as a neighbor's rows.
        check("SELECT (COUNT(*) AS ?n) WHERE { GRAPH ex:s0 { ?s ?p ?o } }", 0, true);
    }

    @Test
    void absentGraphCountsZero() {
        check("SELECT (COUNT(*) AS ?n) WHERE { GRAPH ex:nope { ?s ?p ?o } }", 0, true);
    }

    @Test
    void unionGraphFallsBack() {
        check("SELECT (COUNT(*) AS ?n) WHERE { GRAPH <" + Quad.unionGraph.getURI() + "> { ?s ?p ?o } }",
                3, false);
    }

    @Test
    void filterShapeFallsBack() {
        check("SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o FILTER(isLiteral(?o)) }", 1, false);
    }

    @Test
    void concreteTermFallsBack() {
        check("SELECT (COUNT(*) AS ?n) WHERE { ex:s0 ?p ?o }", 2, false);
    }

    @Test
    void repeatedVariableFallsBack() {
        check("SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?s }", 1, false);
    }

    @Test
    void countExpressionFallsBack() {
        check("SELECT (COUNT(DISTINCT STR(?s)) AS ?n) WHERE { ?s ?p ?o }", 3, false);
    }

    @Test
    void unboundCountVarFallsBack() {
        // COUNT of a variable the pattern never binds is 0 - not the row count.
        check("SELECT (COUNT(?nope) AS ?n) WHERE { ?s ?p ?o }", 0, false);
    }

    @Test
    void groupByFallsBack() {
        long before = AggregateCountFastPath.HITS.get();
        int rows = 0;
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(
                PREFIX + "SELECT ?s (COUNT(?o) AS ?c) WHERE { ?s ?p ?o } GROUP BY ?s")).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) { rs.next(); rows++; }
        }
        assertEquals(3, rows);
        assertEquals(before, AggregateCountFastPath.HITS.get());
    }

    @Test
    void modelSizeUsesIndexCount() {
        assertEquals(4, ds.getDefaultModel().size());
    }
}
