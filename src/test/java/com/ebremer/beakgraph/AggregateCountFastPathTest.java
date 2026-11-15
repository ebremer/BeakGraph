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

    // --- every named graph: GRAPH ?g -------------------------------------

    @Test
    void countOverEveryNamedGraph() {
        // g1 holds 2 quads and g2 1; the default graph's 4 are not a named graph's.
        check("SELECT (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } }", 3, true);
        check("SELECT (COUNT(?g) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } }", 3, true);
        check("SELECT (COUNT(?o) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } }", 3, true);
        check("SELECT (COUNT(DISTINCT *) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } }", 3, true);
    }

    @Test
    void distinctCountsOverEveryNamedGraph() {
        check("SELECT (COUNT(DISTINCT ?g) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } }", 2, true);
        check("SELECT (COUNT(DISTINCT ?p) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } }", 3, true); // p2, shared, p3
        // A subject may be in several graphs: counting it once needs the rows.
        check("SELECT (COUNT(DISTINCT ?s) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } }", 2, false);
        check("SELECT (COUNT(DISTINCT ?o) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } }", 3, false);
    }

    @Test
    void theGraphVariableInThePatternFallsBack() {
        // The graph's name constrains the rows: no index count is the answer.
        check("SELECT (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?g ?p ?o } }", 0, false);
        check("SELECT (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?g } }", 0, false);
    }

    @Test
    void aChosenSetOfNamedGraphsFallsBack() {
        // FROM NAMED makes another dataset: its named graphs are not the store's.
        check("SELECT (COUNT(*) AS ?n) FROM NAMED ex:g1 WHERE { GRAPH ?g { ?s ?p ?o } }", 2, false);
    }

    // --- groups ----------------------------------------------------------

    /** The groups of a grouped count: the key variables' terms (in the order given) to the count. */
    private static java.util.Map<String, Long> groups(String queryBody, String... keys) {
        java.util.Map<String, Long> out = new java.util.TreeMap<>();
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(PREFIX + queryBody)).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                org.apache.jena.query.QuerySolution row = rs.next();
                StringBuilder key = new StringBuilder();
                for (String k : keys) {
                    key.append(row.get(k).asNode().toString().replace(NS, "ex:")).append(' ');
                }
                Long before = out.put(key.toString().trim(), row.getLiteral("n").getLong());
                assertEquals(null, before, "one row a group: " + key);
            }
        }
        return out;
    }

    private static void checkGroups(String queryBody, java.util.Map<String, Long> expected, boolean expectFastPath, String... keys) {
        long before = AggregateCountFastPath.HITS.get();
        assertEquals(new java.util.TreeMap<>(expected), groups(queryBody, keys), "groups of: " + queryBody);
        assertEquals(expectFastPath ? 1 : 0, AggregateCountFastPath.HITS.get() - before, "fast-path activations for: " + queryBody);
    }

    @Test
    void groupedByPredicateInOneGraph() {
        checkGroups("SELECT ?p (COUNT(*) AS ?n) WHERE { ?s ?p ?o } GROUP BY ?p",
                java.util.Map.of("ex:p0", 1L, "ex:p1", 1L, "ex:shared", 1L, "ex:refl", 1L), true, "p");
        checkGroups("SELECT ?p (COUNT(?s) AS ?n) WHERE { GRAPH ex:g1 { ?s ?p ?o } } GROUP BY ?p",
                java.util.Map.of("ex:p2", 1L, "ex:shared", 1L), true, "p");
        // A graph that is none, and an entity that is no graph: no group at all.
        checkGroups("SELECT ?p (COUNT(*) AS ?n) WHERE { GRAPH ex:nope { ?s ?p ?o } } GROUP BY ?p", java.util.Map.of(), true, "p");
        checkGroups("SELECT ?p (COUNT(*) AS ?n) WHERE { GRAPH ex:s0 { ?s ?p ?o } } GROUP BY ?p", java.util.Map.of(), true, "p");
    }

    @Test
    void groupedOverEveryNamedGraph() {
        checkGroups("SELECT ?g (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g",
                java.util.Map.of("ex:g1", 2L, "ex:g2", 1L), true, "g");
        checkGroups("SELECT ?p (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?p",
                java.util.Map.of("ex:p2", 1L, "ex:shared", 1L, "ex:p3", 1L), true, "p");
        checkGroups("SELECT ?g ?p (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g ?p",
                java.util.Map.of("ex:g1 ex:p2", 1L, "ex:g1 ex:shared", 1L, "ex:g2 ex:p3", 1L), true, "g", "p");
        checkGroups("SELECT ?p ?g (COUNT(DISTINCT *) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?p ?g",
                java.util.Map.of("ex:g1 ex:p2", 1L, "ex:g1 ex:shared", 1L, "ex:g2 ex:p3", 1L), true, "g", "p");
    }

    @Test
    void distinctCountsAGraph() {
        checkGroups("SELECT ?g (COUNT(DISTINCT ?s) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g",
                java.util.Map.of("ex:g1", 1L, "ex:g2", 1L), true, "g");
        checkGroups("SELECT ?g (COUNT(DISTINCT ?p) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g",
                java.util.Map.of("ex:g1", 2L, "ex:g2", 1L), true, "g");
        checkGroups("SELECT ?g (COUNT(DISTINCT ?g) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g",
                java.util.Map.of("ex:g1", 1L, "ex:g2", 1L), true, "g");
        checkGroups("SELECT ?g (COUNT(DISTINCT ?o) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g",
                java.util.Map.of("ex:g1", 2L, "ex:g2", 1L), false, "g");
    }

    @Test
    void groupsWithoutAnIndexStructureFallBack() {
        checkGroups("SELECT ?p (COUNT(DISTINCT ?s) AS ?n) WHERE { ?s ?p ?o } GROUP BY ?p",
                java.util.Map.of("ex:p0", 1L, "ex:p1", 1L, "ex:shared", 1L, "ex:refl", 1L), false, "p");
        checkGroups("SELECT ?p (COUNT(DISTINCT ?g) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?p",
                java.util.Map.of("ex:p2", 1L, "ex:shared", 1L, "ex:p3", 1L), false, "p");
        checkGroups("SELECT ?o (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?o",
                java.util.Map.of("ex:b", 1L, "ex:c", 1L, "\"x\"", 1L), false, "o");
        checkGroups("SELECT ?g ?s (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g ?s",
                java.util.Map.of("ex:g1 ex:a", 2L, "ex:g2 ex:d", 1L), false, "g", "s");
        checkGroups("SELECT ?p (COUNT(*) AS ?n) WHERE { ?s ?p ?o FILTER(isIRI(?o)) } GROUP BY ?p",
                java.util.Map.of("ex:p0", 1L, "ex:shared", 1L, "ex:refl", 1L), false, "p");
        checkGroups("SELECT ?p (COUNT(*) AS ?n) WHERE { GRAPH <" + Quad.unionGraph.getURI() + "> { ?s ?p ?o } } GROUP BY ?p",
                java.util.Map.of("ex:p2", 1L, "ex:shared", 1L, "ex:p3", 1L), false, "p");
        checkGroups("SELECT ?x (COUNT(*) AS ?n) WHERE { ?s ?p ?o } GROUP BY (STR(?p) AS ?x)",
                java.util.Map.of("\"ex:p0\"", 1L, "\"ex:p1\"", 1L, "\"ex:shared\"", 1L, "\"ex:refl\"", 1L), // the helper shortens the namespace
                false, "x");
    }

    @Test
    void whatStandsAboveTheGroupStillApplies() {
        // HAVING, ORDER BY and LIMIT work on the group's rows, whichever way they were made.
        checkGroups("SELECT ?g (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g HAVING (COUNT(*) > 1)",
                java.util.Map.of("ex:g1", 2L), true, "g");
        long before = AggregateCountFastPath.HITS.get();
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(
                PREFIX + "SELECT ?g (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g ORDER BY DESC(?n) LIMIT 1")).build()) {
            ResultSet rs = qe.execSelect();
            assertEquals(NS + "g1", rs.next().getResource("g").getURI());
            assertEquals(false, rs.hasNext());
        }
        assertEquals(1, AggregateCountFastPath.HITS.get() - before);
    }

    /**
     * Stores of many graphs and predicates, of every shape the counts read
     * past: the index counts must be the counts of the rows themselves, graph
     * by graph and predicate by predicate, to the last graph and the last
     * predicate of the store.
     */
    @Test
    void indexCountsAreTheRowsCounts() throws Exception {
        java.util.Random random = new java.util.Random(42);
        for (int round = 0; round < 4; round++) {
            int graphs = 1 + random.nextInt(40);
            int predicates = 1 + random.nextInt(9);
            java.util.Map<String, Long> byGraph = new java.util.TreeMap<>();
            java.util.Map<String, Long> byPredicate = new java.util.TreeMap<>();
            java.util.Map<String, Long> byBoth = new java.util.TreeMap<>();
            java.util.Map<String, Long> ofDefault = new java.util.TreeMap<>();
            java.util.Set<String> quads = new java.util.LinkedHashSet<>();
            StringBuilder nq = new StringBuilder();
            int rows = 20 + random.nextInt(600);
            for (int i = 0; i < rows; i++) {
                int g = random.nextInt(graphs + 1); // 0: the default graph
                String s = "<" + NS + "s" + random.nextInt(60) + ">";
                String p = "ex:q" + random.nextInt(predicates);
                String o = random.nextBoolean() ? "<" + NS + "s" + random.nextInt(60) + ">" : "\"v" + random.nextInt(30) + "\"";
                if (!quads.add(g + " " + s + " " + p + " " + o)) {
                    continue; // a quad is stored once
                }
                String pIri = "<" + NS + p.substring(3) + ">";
                nq.append(s).append(' ').append(pIri).append(' ').append(o);
                if (g == 0) {
                    ofDefault.merge(p, 1L, Long::sum);
                } else {
                    nq.append(" <").append(NS).append("n").append(g).append(">");
                    byGraph.merge("ex:n" + g, 1L, Long::sum);
                    byPredicate.merge(p, 1L, Long::sum);
                    byBoth.merge("ex:n" + g + " " + p, 1L, Long::sum);
                }
                nq.append(" .\n");
            }
            File src = dir.resolve("random" + round + ".nq").toFile();
            File h5 = dir.resolve("random" + round + ".nq.h5").toFile();
            Files.write(src.toPath(), nq.toString().getBytes(StandardCharsets.UTF_8));
            HDF5Writer.Builder().setSource(src).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
            Dataset saved = ds;
            try (BeakGraph random1 = new BeakGraph(new HDF5Reader(h5))) {
                ds = random1.getDataset();
                long total = byGraph.values().stream().mapToLong(Long::longValue).sum();
                check("SELECT (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } }", total, true);
                check("SELECT (COUNT(DISTINCT ?g) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } }", byGraph.size(), true);
                check("SELECT (COUNT(DISTINCT ?p) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } }", byPredicate.size(), true);
                checkGroups("SELECT ?g (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g", byGraph, true, "g");
                checkGroups("SELECT ?p (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?p", byPredicate, true, "p");
                checkGroups("SELECT ?g ?p (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g ?p", byBoth, true, "g", "p");
                checkGroups("SELECT ?p (COUNT(*) AS ?n) WHERE { ?s ?p ?o } GROUP BY ?p", ofDefault, true, "p");
                for (String g : byGraph.keySet()) {
                    java.util.Map<String, Long> ofGraph = new java.util.TreeMap<>();
                    byBoth.forEach((k, v) -> {
                        if (k.startsWith(g + " ")) {
                            ofGraph.put(k.substring(g.length() + 1), v);
                        }
                    });
                    checkGroups("SELECT ?p (COUNT(*) AS ?n) WHERE { GRAPH " + g + " { ?s ?p ?o } } GROUP BY ?p", ofGraph, true, "p");
                }
            } finally {
                ds = saved;
            }
        }
    }
}
