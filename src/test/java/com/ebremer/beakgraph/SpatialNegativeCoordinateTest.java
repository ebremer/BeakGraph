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
 * Spatial correctness for geometry and query regions in NEGATIVE coordinate
 * space (real GeoSPARQL lon/lat data lives there). The Hilbert domain is
 * non-negative, so both sides clamp their bboxes into it with the same
 * monotone projection - overlap survives the clamp and the exact JTS
 * verification removes the clamp's false positives. Before the fix,
 * fully-negative geometry was skipped at build (unfindable by any query) and
 * negative query regions bailed out to zero candidates: genuine intersections
 * were silent false negatives.
 */
class SpatialNegativeCoordinateTest {

    private static final String TTL = """
        @prefix ex:  <http://ex.org/> .
        @prefix geo: <http://www.opengis.net/ont/geosparql#> .
        # entirely in negative space
        ex:neg geo:asWKT "POLYGON((-100 -100,-50 -100,-50 -50,-100 -50,-100 -100))"^^geo:wktLiteral .
        # straddles the origin
        ex:strad geo:asWKT "POLYGON((-10 -10,10 -10,10 10,-10 10,-10 -10))"^^geo:wktLiteral .
        # entirely positive, far from the origin
        ex:pos geo:asWKT "POLYGON((500 500,560 500,560 560,500 560,500 500))"^^geo:wktLiteral .
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
        File ttl = dir.resolve("negspace.ttl").toFile();
        File h5 = dir.resolve("negspace.ttl.h5").toFile();
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
    void negativeRegionFindsFullyNegativeGeometry() {
        assertEquals(Set.of("http://ex.org/neg"),
            intersecting("POLYGON((-90 -90,-60 -90,-60 -60,-90 -60,-90 -90))"));
    }

    @Test
    void negativeRegionFindsStraddlingGeometry() {
        assertEquals(Set.of("http://ex.org/strad"),
            intersecting("POLYGON((-8 -8,-2 -8,-2 -2,-8 -2,-8 -8))"));
    }

    @Test
    void straddlingRegionFindsBothNegativeAndStraddlingGeometry() {
        assertEquals(Set.of("http://ex.org/neg", "http://ex.org/strad"),
            intersecting("POLYGON((-60 -60,5 -60,5 5,-60 5,-60 -60))"));
    }

    @Test
    void positiveRegionIsUnaffectedByOriginClustering() {
        // The clamped negative geometries cluster at the axis cells; a positive
        // region far away must still return only its true intersections.
        assertEquals(Set.of("http://ex.org/pos"),
            intersecting("POLYGON((520 520,540 520,540 540,520 540,520 520))"));
    }

    @Test
    void disjointNegativeRegionMatchesNothing() {
        // Clamping makes every negative-space geometry a CANDIDATE here; the
        // JTS verification stage must reject them all.
        assertEquals(Set.of(),
            intersecting("POLYGON((-2000 -2000,-1900 -2000,-1900 -1900,-2000 -1900,-2000 -2000))"));
    }
}
