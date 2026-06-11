package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Binding a variable (via VALUES/BIND) to a term not present in the store must yield no
 * rows when that variable is used in a pattern, and must carry the original term through
 * when it is not - instead of failing to resolve the "does not exist" marker.
 */
class NonExistentBindingTest {

    private static final String TTL = """
        @prefix ex: <http://ex.org/> .
        ex:a ex:p ex:b .
        ex:a ex:p ex:c .
        ex:a ex:name "Alpha" .
        """;

    private static final String P = "PREFIX ex: <http://ex.org/> ";

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;

    @BeforeAll
    static void build() throws Exception {
        File ttl = dir.resolve("ne.ttl").toFile();
        File h5 = dir.resolve("ne.ttl.h5").toFile();
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

    private static int count(String body) {
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create(P + "SELECT * WHERE { " + body + " }")).build()) {
            ResultSet rs = qe.execSelect();
            int n = 0;
            while (rs.hasNext()) { rs.next(); n++; }
            return n;
        }
    }

    @Test
    void missingBoundVarUsedInPatternYieldsNoRows() {
        assertEquals(0, count("VALUES ?v { ex:missing } ?v ex:p ?o"));
        assertEquals(0, count("BIND(ex:missing AS ?v) ?v ex:p ?o"));
    }

    @Test
    void existingBoundVarStillMatches() {
        // Sanity: the same shape with an existing term still resolves and matches.
        assertEquals(2, count("VALUES ?v { ex:a } ?v ex:p ?o"));
    }

    @Test
    void missingBoundVarUnusedIsCarriedThrough() {
        Set<String> vs = new HashSet<>();
        Set<String> objects = new HashSet<>();
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(
                P + "SELECT ?v ?o WHERE { BIND(ex:missing AS ?v) ?s ex:p ?o }")).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                QuerySolution s = rs.next();
                vs.add(s.getResource("v").getURI());
                objects.add(s.getResource("o").getURI());
            }
        }
        assertEquals(Set.of("http://ex.org/missing"), vs, "?v must carry through as the original term");
        assertEquals(Set.of("http://ex.org/b", "http://ex.org/c"), objects);
    }
}
