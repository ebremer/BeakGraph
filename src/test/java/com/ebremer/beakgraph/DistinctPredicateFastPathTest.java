package com.ebremer.beakgraph;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.jena.DistinctPredicateFastPath;
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

/**
 * {@code SELECT DISTINCT ?p} answered from the GPOS per-graph predicate level
 * ({@link DistinctPredicateFastPath}) must return exactly what a full scan
 * returns - notably NOT over-reporting predicates that exist only in other
 * named graphs (the flaw that got the previous DISTINCT optimization removed) -
 * and every shape outside the fast path's guards must fall back to normal
 * execution with unchanged results. HITS distinguishes the two paths.
 */
class DistinctPredicateFastPathTest {

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
        File src = dir.resolve("dp.trig").toFile();
        File h5 = dir.resolve("dp.trig.h5").toFile();
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
        long before = DistinctPredicateFastPath.HITS.get();
        assertEquals(expected, terms(queryBody), "results for: " + queryBody);
        long used = DistinctPredicateFastPath.HITS.get() - before;
        assertEquals(expectFastPath ? 1 : 0, used, "fast-path activations for: " + queryBody);
    }

    @Test
    void defaultGraphAnswersFromIndexWithoutOverReporting() {
        // p2/p3 exist only in named graphs - the old unsound fast path leaked them.
        check("SELECT DISTINCT ?p WHERE { ?s ?p ?o }",
                ex("p0", "p1", "shared", "refl"), true);
    }

    @Test
    void concreteNamedGraphAnswersFromIndex() {
        check("SELECT DISTINCT ?p WHERE { GRAPH ex:g1 { ?s ?p ?o } }",
                ex("p2", "shared"), true);
    }

    @Test
    void unionGraphAnswersFromIndexAcrossNamedGraphsOnly() {
        check("SELECT DISTINCT ?p WHERE { GRAPH <" + Quad.unionGraph.getURI() + "> { ?s ?p ?o } }",
                ex("p2", "p3", "shared"), true);
    }

    @Test
    void absentGraphAnswersEmpty() {
        check("SELECT DISTINCT ?p WHERE { GRAPH ex:nope { ?s ?p ?o } }",
                Set.of(), true);
    }

    @Test
    void filterShapeFallsBackAndRespectsFilter() {
        check("SELECT DISTINCT ?p WHERE { ?s ?p ?o FILTER(isLiteral(?o)) }",
                ex("p1"), false);
    }

    @Test
    void concreteSubjectFallsBack() {
        check("SELECT DISTINCT ?p WHERE { ex:s0 ?p ?o }",
                ex("p0", "p1"), false);
    }

    @Test
    void repeatedVariableFallsBack() {
        check("SELECT DISTINCT ?p WHERE { ?s ?p ?s }",
                ex("refl"), false);
    }

    @Test
    void distinctSubjectFallsBack() {
        check("SELECT DISTINCT ?s WHERE { ?s ?p ?o }",
                ex("s0", "s1", "s2"), false);
    }

    @Test
    void valuesBoundSubjectFallsBack() {
        check("SELECT DISTINCT ?p WHERE { VALUES ?s { ex:s0 } ?s ?p ?o }",
                ex("p0", "p1"), false);
    }

    @Test
    void graphVariableFallsBack() {
        check("SELECT DISTINCT ?p WHERE { GRAPH ?g { ?s ?p ?o } }",
                ex("p2", "p3", "shared"), false);
    }

    @Test
    void multiPatternFallsBack() {
        check("SELECT DISTINCT ?p WHERE { ?s ?p ?o . ?s ex:p1 ?lit }",
                ex("p0", "p1"), false);
    }
}
