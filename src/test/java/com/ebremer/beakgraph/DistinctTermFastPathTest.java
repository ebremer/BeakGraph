package com.ebremer.beakgraph;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.jena.DistinctTermFastPath;
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
import org.apache.jena.sparql.core.Quad;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code SELECT DISTINCT ?p} (GPOS predicate level) and {@code SELECT DISTINCT ?s}
 * (GSPO subject level, streamed) answered by {@link DistinctTermFastPath} must
 * return exactly what a full scan returns - notably NOT over-reporting terms that
 * exist only in other named graphs (the flaw that got the previous DISTINCT
 * optimization removed) - and every shape outside the fast path's guards must
 * fall back to normal execution with unchanged results. HITS distinguishes the
 * two paths.
 */
class DistinctTermFastPathTest {

    private static final String NS = "http://ex.org/";
    private static final String PREFIX = "PREFIX ex: <" + NS + ">\n";

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;

    @BeforeAll
    static void build() throws Exception {
        String trig = """
                @prefix ex: <http://ex.org/> .
                ex:s0 ex:p0 ex:o0 .
                ex:s0 ex:p1 "lit" .
                ex:s1 ex:shared ex:s0 .
                ex:s2 ex:refl ex:s2 .
                ex:g1 { ex:a ex:p2 ex:b . ex:a ex:shared ex:c . }
                ex:g2 { ex:d ex:p3 "x" . }
                """;
        File src = dir.resolve("dt.trig").toFile();
        File h5 = dir.resolve("dt.trig.h5").toFile();
        Files.write(src.toPath(), trig.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setSource(src).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        bg = new BeakGraph(new HDF5Reader(h5));
        ds = bg.getDataset();
    }

    @AfterAll
    static void close() {
        if (bg != null) bg.close();
    }

    /** Runs the query and collects the single projected variable's term strings. */
    private static Set<String> terms(String queryBody) {
        Set<String> out = new HashSet<>();
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create(PREFIX + queryBody)).build()) {
            ResultSet rs = qe.execSelect();
            String var = rs.getResultVars().get(0);
            while (rs.hasNext()) {
                QuerySolution row = rs.next();
                out.add(row.get(var).toString());
            }
        }
        return out;
    }

    private static Set<String> ex(String... locals) {
        Set<String> s = new HashSet<>();
        for (String l : locals) s.add(NS + l);
        return s;
    }

    /** Asserts the query returns {@code expected} AND whether the fast path answered it. */
    private static void check(String queryBody, Set<String> expected, boolean expectFastPath) {
        long before = DistinctTermFastPath.HITS.get();
        assertEquals(expected, terms(queryBody), "results for: " + queryBody);
        long used = DistinctTermFastPath.HITS.get() - before;
        assertEquals(expectFastPath ? 1 : 0, used, "fast-path activations for: " + queryBody);
    }

    // ---------------- DISTINCT ?p (GPOS predicate level) ----------------

    @Test
    void predicatesDefaultGraphAnswersFromIndexWithoutOverReporting() {
        // p2/p3 exist only in named graphs - the old unsound fast path leaked them.
        check("SELECT DISTINCT ?p WHERE { ?s ?p ?o }",
                ex("p0", "p1", "shared", "refl"), true);
    }

    @Test
    void predicatesConcreteNamedGraphAnswersFromIndex() {
        check("SELECT DISTINCT ?p WHERE { GRAPH ex:g1 { ?s ?p ?o } }",
                ex("p2", "shared"), true);
    }

    @Test
    void predicatesUnionGraphAnswersFromIndexAcrossNamedGraphsOnly() {
        check("SELECT DISTINCT ?p WHERE { GRAPH <" + Quad.unionGraph.getURI() + "> { ?s ?p ?o } }",
                ex("p2", "p3", "shared"), true);
    }

    @Test
    void predicatesAbsentGraphAnswersEmpty() {
        check("SELECT DISTINCT ?p WHERE { GRAPH ex:nope { ?s ?p ?o } }",
                Set.of(), true);
    }

    // ---------------- DISTINCT ?s (GSPO subject level, streamed) ----------------

    @Test
    void subjectsDefaultGraphAnswersFromIndexWithoutOverReporting() {
        // a/d are subjects only in named graphs - must not leak into the default graph.
        check("SELECT DISTINCT ?s WHERE { ?s ?p ?o }",
                ex("s0", "s1", "s2"), true);
    }

    @Test
    void subjectsConcreteNamedGraphAnswersFromIndex() {
        check("SELECT DISTINCT ?s WHERE { GRAPH ex:g1 { ?s ?p ?o } }",
                ex("a"), true);
    }

    @Test
    void subjectsUnionGraphFallsBack() {
        // Cross-graph dedup of subject ids is not index-answerable (yet): scan.
        check("SELECT DISTINCT ?s WHERE { GRAPH <" + Quad.unionGraph.getURI() + "> { ?s ?p ?o } }",
                ex("a", "d"), false);
    }

    @Test
    void subjectsAbsentGraphAnswersEmpty() {
        check("SELECT DISTINCT ?s WHERE { GRAPH ex:nope { ?s ?p ?o } }",
                Set.of(), true);
    }

    @Test
    void subjectsNonGraphEntityAnswersEmpty() {
        // ex:o0 is an entity but not a graph: its first-level slot is a padding
        // block, which must read as empty - not as a neighbor's subjects.
        check("SELECT DISTINCT ?s WHERE { GRAPH ex:o0 { ?s ?p ?o } }",
                Set.of(), true);
    }

    @Test
    void subjectsStreamUnderLimit() {
        long before = DistinctTermFastPath.HITS.get();
        Set<String> got = terms("SELECT DISTINCT ?s WHERE { ?s ?p ?o } LIMIT 2");
        assertEquals(2, got.size());
        assertTrue(ex("s0", "s1", "s2").containsAll(got), "LIMIT rows must be real subjects: " + got);
        assertEquals(1, DistinctTermFastPath.HITS.get() - before);
    }

    // ---------------- fallback shapes (unchanged results, no fast path) ----------------

    @Test
    void distinctObjectsFallsBack() {
        check("SELECT DISTINCT ?o WHERE { ?s ?p ?o }",
                Set.of(NS + "o0", "lit", NS + "s0", NS + "s2"), false);
    }

    @Test
    void filterShapeFallsBackAndRespectsFilter() {
        check("SELECT DISTINCT ?p WHERE { ?s ?p ?o FILTER(isLiteral(?o)) }",
                ex("p1"), false);
        check("SELECT DISTINCT ?s WHERE { ?s ?p ?o FILTER(isLiteral(?o)) }",
                ex("s0"), false);
    }

    @Test
    void concreteTermFallsBack() {
        check("SELECT DISTINCT ?p WHERE { ex:s0 ?p ?o }", ex("p0", "p1"), false);
        check("SELECT DISTINCT ?s WHERE { ?s ex:shared ?o }", ex("s1"), false);
    }

    @Test
    void repeatedVariableFallsBack() {
        check("SELECT DISTINCT ?p WHERE { ?s ?p ?s }", ex("refl"), false);
        check("SELECT DISTINCT ?s WHERE { ?s ?p ?s }", ex("s2"), false);
    }

    @Test
    void valuesBoundVariableFallsBack() {
        check("SELECT DISTINCT ?p WHERE { VALUES ?s { ex:s0 } ?s ?p ?o }",
                ex("p0", "p1"), false);
    }

    @Test
    void graphVariableFallsBack() {
        check("SELECT DISTINCT ?p WHERE { GRAPH ?g { ?s ?p ?o } }",
                ex("p2", "p3", "shared"), false);
        check("SELECT DISTINCT ?s WHERE { GRAPH ?g { ?s ?p ?o } }",
                ex("a", "d"), false);
    }

    @Test
    void multiPatternFallsBack() {
        check("SELECT DISTINCT ?p WHERE { ?s ?p ?o . ?s ex:p1 ?lit }",
                ex("p0", "p1"), false);
    }
}
