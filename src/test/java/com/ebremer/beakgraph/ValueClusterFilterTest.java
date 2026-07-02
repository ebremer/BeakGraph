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
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.RDFNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression tests for C6b: range-filter pushdown must enclose the whole
 * cluster of value-equal but term-distinct literals. "5"^^xsd:int, 5
 * (xsd:integer) and "5.0"^^xsd:double compare value-equal and occupy distinct
 * adjacent dictionary ids; deriving the scan bound from the exact-term
 * insertion point landed inside that cluster and silently dropped qualifying
 * rows at the boundary (e.g. FILTER(?o >= 5) missed "5"^^xsd:int).
 */
class ValueClusterFilterTest {

    private static final String TTL = """
        @prefix ex:  <http://ex.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        ex:m1 ex:value "5"^^xsd:int .
        ex:m2 ex:value 5 .
        ex:m3 ex:value "5.0"^^xsd:double .
        ex:m4 ex:value 4 .
        ex:m5 ex:value 6 .
        """;

    private static final String PREFIX =
        "PREFIX ex: <http://ex.org/> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ";

    private static final Set<String> ALL_FIVES_AND_UP =
        Set.of("http://ex.org/m1", "http://ex.org/m2", "http://ex.org/m3", "http://ex.org/m5");
    private static final Set<String> ALL_FIVES_AND_DOWN =
        Set.of("http://ex.org/m1", "http://ex.org/m2", "http://ex.org/m3", "http://ex.org/m4");

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;

    @BeforeAll
    static void buildAndOpen() throws Exception {
        File ttl = dir.resolve("cluster.ttl").toFile();
        File h5 = dir.resolve("cluster.ttl.h5").toFile();
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

    private static Set<String> subjects(String where) {
        Set<String> uris = new HashSet<>();
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create(PREFIX + "SELECT ?s WHERE { " + where + " }")).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                RDFNode n = rs.next().get("s");
                if (n != null && n.isURIResource()) uris.add(n.asResource().getURI());
            }
        }
        return uris;
    }

    // --- bound predicate (GPOS / BGIteratorPOS route) -----------------------

    @Test
    void greaterOrEqualIncludesWholeValueCluster() {
        assertEquals(ALL_FIVES_AND_UP, subjects("?s ex:value ?o FILTER(?o >= 5)"));
    }

    @Test
    void lessOrEqualIncludesWholeValueCluster() {
        assertEquals(ALL_FIVES_AND_DOWN, subjects("?s ex:value ?o FILTER(?o <= 5)"));
    }

    @Test
    void strictBoundsExcludeWholeValueCluster() {
        assertEquals(Set.of("http://ex.org/m5"), subjects("?s ex:value ?o FILTER(?o > 5)"));
        assertEquals(Set.of("http://ex.org/m4"), subjects("?s ex:value ?o FILTER(?o < 5)"));
    }

    @Test
    void absentConstantInsideClusterRange() {
        // 4.5 is not a stored term; the insertion point sits next to the 5-cluster.
        assertEquals(ALL_FIVES_AND_UP, subjects("?s ex:value ?o FILTER(?o > 4.5)"));
        assertEquals(Set.of("http://ex.org/m4"), subjects("?s ex:value ?o FILTER(?o < 4.5)"));
    }

    @Test
    void typedConstantVariantsAgree() {
        // The same value written as a different term must produce identical results.
        assertEquals(ALL_FIVES_AND_UP, subjects("?s ex:value ?o FILTER(?o >= \"5\"^^xsd:int)"));
        assertEquals(ALL_FIVES_AND_UP, subjects("?s ex:value ?o FILTER(?o >= \"5.0\"^^xsd:double)"));
    }

    // --- variable predicate (GSPO full scan / BGIteratorSPO_All route) ------

    @Test
    void fullScanRouteAgrees() {
        assertEquals(ALL_FIVES_AND_UP, subjects("?s ?p ?o FILTER(?o >= 5)"));
        assertEquals(ALL_FIVES_AND_DOWN, subjects("?s ?p ?o FILTER(?o <= 5)"));
        assertEquals(Set.of("http://ex.org/m5"), subjects("?s ?p ?o FILTER(?o > 5)"));
    }
}
