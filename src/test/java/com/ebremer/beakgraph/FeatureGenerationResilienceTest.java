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
 * Regression test for the FEATURES pipeline's bad-geometry policy: a
 * structurally invalid WKT (two-point ring - JTS throws
 * IllegalArgumentException, not ParseException) must not abort the build
 * through MajorMinor/Gen2DFeatures. One geometry's features are skipped with a
 * warning; every other geometry still gets its features.
 */
class FeatureGenerationResilienceTest {

    private static final String TTL = """
        @prefix ex:  <http://ex.org/> .
        @prefix geo: <http://www.opengis.net/ont/geosparql#> .
        ex:good geo:asWKT "POLYGON((0 0,64 0,64 32,0 32,0 0))"^^geo:wktLiteral .
        ex:degen geo:asWKT "POLYGON((0 0,1 1))"^^geo:wktLiteral .
        ex:crs geo:asWKT "<http://www.opengis.net/def/crs/EPSG/0/4326> POLYGON((200 200,264 200,264 232,200 232,200 200))"^^geo:wktLiteral .
        """;

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;

    @BeforeAll
    static void buildAndOpen() throws Exception {
        File ttl = dir.resolve("features.ttl").toFile();
        File h5 = dir.resolve("features.ttl.h5").toFile();
        Files.write(ttl.toPath(), TTL.getBytes(StandardCharsets.UTF_8));
        // Pre-fix this aborted: MajorMinor.add let the IllegalArgumentException
        // escape into the spatial task's Future, failing the whole write.
        HDF5Writer.Builder().setSource(ttl).setDestination(h5)
                .setSpatial(true).setFeatures(true).build().write();
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

    @Test
    void goodGeometryGetsFeatures() {
        assertEquals(1, count("ex:good hal:centroid ?c"),
            "the well-formed geometry must get its derived features");
    }

    @Test
    void degenerateGeometryIsSkippedWithoutFeatures() {
        assertEquals(0, count("ex:degen hal:centroid ?c"));
    }

    @Test
    void crsPrefixedGeometryGetsFeatures() {
        // The GeoSPARQL-standard "<crs-uri> WKT" form: the spatial indexing path
        // stripped the prefix but feature generation re-read the raw lexical, so
        // JTS threw and CRS-prefixed geometries silently got no features.
        assertEquals(1, count("ex:crs hal:centroid ?c"),
            "a CRS-prefixed geometry must get the same derived features as a plain one");
    }

    @Test
    void bothSourceQuadsSurvive() {
        assertTrue(count("ex:good ?p ?o") >= 1);
        assertTrue(count("ex:degen ?p ?o") >= 1);
    }
}
