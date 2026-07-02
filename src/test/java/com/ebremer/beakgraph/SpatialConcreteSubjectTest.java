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
import java.util.ArrayList;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * geof:sfIntersects with a CONCRETE feature subject. The spatial index stores
 * geometry SUBJECT ids, so with nothing to seed (no variable subject in the
 * geometry triple) the accelerator must step aside and leave the answer to the
 * pattern solve + sfIntersects verification. The old code seeded the geometry
 * OBJECT variable with subject ids instead: every candidate row failed the
 * triple pattern and any pinned-subject spatial query silently returned zero
 * rows (ASK said false for a geometry that genuinely intersects).
 */
class SpatialConcreteSubjectTest {

    private static final String REGION = "POLYGON((100 100,300 100,300 300,100 300,100 100))";

    private static final String TTL = """
        @prefix ex:  <http://ex.org/> .
        @prefix geo: <http://www.opengis.net/ont/geosparql#> .
        # entirely inside the region
        ex:small geo:asWKT "POLYGON((150 150,160 150,160 160,150 160,150 150))"^^geo:wktLiteral .
        # far away - disjoint from the region
        ex:far geo:asWKT "POLYGON((5000 5000,5100 5000,5100 5100,5000 5100,5000 5000))"^^geo:wktLiteral .
        """;

    private static final String PREFIXES = """
        PREFIX ex:   <http://ex.org/>
        PREFIX geo:  <http://www.opengis.net/ont/geosparql#>
        PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
        """;

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;

    @BeforeAll
    static void buildAndOpen() throws Exception {
        File ttl = dir.resolve("concrete.ttl").toFile();
        File h5 = dir.resolve("concrete.ttl.h5").toFile();
        Files.write(ttl.toPath(), TTL.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setSource(ttl).setDestination(h5)
                .setSpatial(true).setFeatures(false).build().write();
        bg = new BeakGraph(new HDF5Reader(h5));
        ds = bg.getDataset();
    }

    @AfterAll
    static void closeReader() {
        if (bg != null) bg.close();
    }

    private static List<String> select(String query, String var) {
        List<String> out = new ArrayList<>();
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(PREFIXES + query)).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                org.apache.jena.rdf.model.RDFNode n = rs.next().get(var);
                out.add(n.isLiteral() ? n.asLiteral().getLexicalForm() : n.toString());
            }
        }
        return out;
    }

    private static boolean ask(String query) {
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(PREFIXES + query)).build()) {
            return qe.execAsk();
        }
    }

    @Test
    void concreteSubjectIntersectingReturnsItsGeometry() {
        List<String> rows = select(
                "SELECT ?w WHERE { ex:small geo:asWKT ?w FILTER(geof:sfIntersects(?w, \""
                        + REGION + "\"^^geo:wktLiteral)) }", "w");
        assertEquals(1, rows.size(), "pinned-subject spatial query must return its intersecting geometry");
        assertTrue(rows.get(0).startsWith("POLYGON((150 150"), "the bound WKT must be ex:small's geometry");
    }

    @Test
    void concreteSubjectAskIsTrue() {
        assertTrue(ask("ASK { ex:small geo:asWKT ?w FILTER(geof:sfIntersects(?w, \""
                + REGION + "\"^^geo:wktLiteral)) }"));
    }

    @Test
    void concreteSubjectDisjointReturnsNothing() {
        assertEquals(List.of(), select(
                "SELECT ?w WHERE { ex:far geo:asWKT ?w FILTER(geof:sfIntersects(?w, \""
                        + REGION + "\"^^geo:wktLiteral)) }", "w"));
        assertFalse(ask("ASK { ex:far geo:asWKT ?w FILTER(geof:sfIntersects(?w, \""
                + REGION + "\"^^geo:wktLiteral)) }"));
    }

    @Test
    void preBoundSubjectFromValuesStillMatches() {
        // The subject var is seeded with index candidates while the incoming
        // binding already pins it - putCompatible must keep the compatible row.
        List<String> rows = select(
                "SELECT ?w WHERE { VALUES ?f { ex:small } ?f geo:asWKT ?w FILTER(geof:sfIntersects(?w, \""
                        + REGION + "\"^^geo:wktLiteral)) }", "w");
        assertEquals(1, rows.size());
    }

    @Test
    void variableSubjectControlStillWorks() {
        List<String> rows = select(
                "SELECT ?f WHERE { ?f geo:asWKT ?w FILTER(geof:sfIntersects(?w, \""
                        + REGION + "\"^^geo:wktLiteral)) }", "f");
        assertEquals(List.of("http://ex.org/small"), rows);
    }
}
