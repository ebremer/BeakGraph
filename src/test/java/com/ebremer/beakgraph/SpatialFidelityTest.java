package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression tests for spatial write fidelity:
 * <ul>
 *   <li>polygon holes (donut annotations - lumens in pathology) must survive
 *       into the scaled WKT pyramid instead of being dropped;</li>
 *   <li>every part of a MULTIPOLYGON must be indexed, not just the first;</li>
 *   <li>POINT geometries must be indexed (via their envelope) instead of being
 *       silently skipped.</li>
 * </ul>
 */
class SpatialFidelityTest {

    private static final String TTL = """
        @prefix ex:  <http://ex.org/> .
        @prefix geo: <http://www.opengis.net/ont/geosparql#> .
        ex:donut geo:asWKT "POLYGON((0 0,64 0,64 64,0 64,0 0),(16 16,48 16,48 48,16 48,16 16))"^^geo:wktLiteral .
        ex:multi geo:asWKT "MULTIPOLYGON(((0 0,32 0,32 32,0 32,0 0)),((1000 1000,1032 1000,1032 1032,1000 1032,1000 1000)))"^^geo:wktLiteral .
        ex:pt    geo:asWKT "POINT(500 500)"^^geo:wktLiteral .
        """;

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;

    @BeforeAll
    static void buildAndOpen() throws Exception {
        File ttl = dir.resolve("fidelity.ttl").toFile();
        File h5 = dir.resolve("fidelity.ttl.h5").toFile();
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

    private static int count(String where) {
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(
                "PREFIX ex: <http://ex.org/> PREFIX hal: <https://halcyon.is/ns/> SELECT * WHERE { " + where + " }")).build()) {
            ResultSet rs = qe.execSelect();
            int n = 0;
            while (rs.hasNext()) { rs.next(); n++; }
            return n;
        }
    }

    private static String levelZeroWkt(String subject) {
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(
                "PREFIX ex: <http://ex.org/> PREFIX hal: <https://halcyon.is/ns/> " +
                "SELECT ?w WHERE { GRAPH <" + Params.SPATIALSTRING + "> { " + subject + " hal:asWKT0 ?w } }")).build()) {
            ResultSet rs = qe.execSelect();
            assertTrue(rs.hasNext(), "expected a level-0 scaled WKT for " + subject);
            return rs.next().getLiteral("w").getLexicalForm();
        }
    }

    @Test
    void polygonHolesSurviveScaling() {
        String wkt0 = levelZeroWkt("ex:donut");
        // A polygon with an interior ring serializes with two coordinate lists.
        assertTrue(wkt0.indexOf('(') != wkt0.lastIndexOf('('),
            "level-0 WKT must be a polygon");
        assertTrue(wkt0.contains("), ("),
            "the donut's interior ring must survive into the scaled WKT, got: " + wkt0);
    }

    @Test
    void allMultiPolygonPartsAreIndexed() {
        // A query region overlapping ONLY the second part must still find the
        // geometry - first-part-only indexing left the other parts unfindable.
        String q = "PREFIX ex: <http://ex.org/> " +
            "PREFIX geo: <http://www.opengis.net/ont/geosparql#> " +
            "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> " +
            "SELECT ?f WHERE { ?f geo:asWKT ?w FILTER(geof:sfIntersects(?w, " +
            "\"POLYGON((1010 1010,1020 1010,1020 1020,1010 1020,1010 1010))\"^^geo:wktLiteral)) }";
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(q)).build()) {
            ResultSet rs = qe.execSelect();
            assertTrue(rs.hasNext(), "the second MULTIPOLYGON part must be findable");
            assertEquals("http://ex.org/multi", rs.next().get("f").asResource().getURI());
        }
    }

    @Test
    void pointGeometriesAreIndexed() {
        assertTrue(count("GRAPH <" + Params.SPATIALSTRING + "> { ex:pt ?p ?o }") > 0,
            "a POINT must be indexed via its envelope, not silently skipped");
    }
}
