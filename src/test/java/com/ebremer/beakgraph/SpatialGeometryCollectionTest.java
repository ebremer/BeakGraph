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

    @Test
    void regionTouchingNeitherMemberMatchesNothing() {
        assertEquals(Set.of(),
            intersecting("POLYGON((100 100,200 100,200 200,100 200,100 100))"));
    }
}
