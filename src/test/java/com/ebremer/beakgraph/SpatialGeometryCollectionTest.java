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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * EVERY member of a mixed GEOMETRYCOLLECTION must be spatially indexed. The
 * envelope fallback for non-polygonal geometry used to fire only when NO
 * polygonal part existed, so a point member sitting next to a polygon member
 * was silently unindexed - invisible to every sfIntersects query.
 */
class SpatialGeometryCollectionTest {

    private static final String TTL = """
        @prefix ex:  <http://ex.org/> .
        @prefix geo: <http://www.opengis.net/ont/geosparql#> .
        ex:mixed geo:asWKT "GEOMETRYCOLLECTION(POLYGON((0 0,10 0,10 10,0 10,0 0)), POINT(500 500))"^^geo:wktLiteral .
        ex:nested geo:asWKT "GEOMETRYCOLLECTION(MULTIPOLYGON(((20 20,30 20,30 30,20 30,20 20)),((1000 1000,1010 1000,1010 1010,1000 1010,1000 1000))), POINT(700 700))"^^geo:wktLiteral .
        ex:deep geo:asWKT "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(MULTIPOINT((900 900),(950 950))))"^^geo:wktLiteral .
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
        File ttl = dir.resolve("mixed.ttl").toFile();
        File h5 = dir.resolve("mixed.ttl.h5").toFile();
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

    private static Set<String> intersecting(String regionWkt) {
        Set<String> hits = new HashSet<>();
        String q = PREFIXES +
            "SELECT ?f WHERE { ?f geo:asWKT ?w FILTER(geof:sfIntersects(?w, \"" + regionWkt + "\"^^geo:wktLiteral)) }";
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(q)).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                hits.add(rs.next().getResource("f").getURI());
            }
        }
        return hits;
    }

    @Test
    void pointMemberNextToAPolygonMemberIsFindable() {
        assertEquals(Set.of("http://ex.org/mixed"),
            intersecting("POLYGON((495 495,505 495,505 505,495 505,495 495))"));
    }

    @Test
    void polygonMemberIsStillFindable() {
        assertEquals(Set.of("http://ex.org/mixed"),
            intersecting("POLYGON((2 2,8 2,8 8,2 8,2 2))"));
    }

    /** BG-371: a MULTIPOLYGON nested in a GEOMETRYCOLLECTION was neither a Polygon nor a "non-areal member", so it was never indexed. */
    @Test
    void multiPolygonNestedInACollectionIsFindable() {
        assertEquals(Set.of("http://ex.org/nested"),
                intersecting("POLYGON((1002 1002,1008 1002,1008 1008,1002 1008,1002 1002))"));
        assertEquals(Set.of("http://ex.org/nested"),
                intersecting("POLYGON((22 22,28 22,28 28,22 28,22 22))"));
        assertEquals(Set.of("http://ex.org/nested"),
                intersecting("POLYGON((695 695,705 695,705 705,695 705,695 695))"));
    }

    @Test
    void leavesOfNestedCollectionsAreIndexedIndividually() {
        assertEquals(Set.of("http://ex.org/deep"),
                intersecting("POLYGON((945 945,955 945,955 955,945 955,945 945))"));
        assertEquals(Set.of("http://ex.org/deep"),
                intersecting("POLYGON((895 895,905 895,905 905,895 905,895 895))"));
    }

    /** The variable-subject (index-seeded) answer must agree with the concrete-subject (JTS only) answer. */
    @Test
    void variableAndConcreteSubjectAgree() {
        String region = "POLYGON((1002 1002,1008 1002,1008 1008,1002 1008,1002 1002))";
        String ask = PREFIXES + "ASK { ex:nested geo:asWKT ?w FILTER(geof:sfIntersects(?w, \"" + region + "\"^^geo:wktLiteral)) }";
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(ask)).build()) {
            assertEquals(true, qe.execAsk(), "the concrete subject intersects (JTS on the whole collection)");
        }
        assertEquals(Set.of("http://ex.org/nested"), intersecting(region), "the seeded scan must find the same subject");
    }

    @Test
    void regionTouchingNeitherMemberMatchesNothing() {
        assertEquals(Set.of(),
            intersecting("POLYGON((100 100,200 100,200 200,100 200,100 100))"));
    }
}
