package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.halcyon.hilbert.PolygonScaler;
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
 * Regression tests for the bad-geometry policy: one problematic polygon must
 * never abort a (possibly multi-hour) spatial build, and the failure modes are
 * uniform - parse failure, snap-collapse and unfixable geometry all skip that
 * geometry's spatial indexing (with a warning) while the source quad itself is
 * still stored and every other geometry is indexed normally.
 */
class BadPolygonResilienceTest {

    private static final String TTL = """
        @prefix ex:  <http://ex.org/> .
        @prefix geo: <http://www.opengis.net/ont/geosparql#> .
        ex:good geo:asWKT "POLYGON((0 0,64 0,64 64,0 64,0 0))"^^geo:wktLiteral .
        ex:tiny geo:asWKT "POLYGON((0.1 0.1,0.4 0.1,0.4 0.4,0.1 0.4,0.1 0.1))"^^geo:wktLiteral .
        ex:bow  geo:asWKT "POLYGON((0 0,10 10,10 0,0 10,0 0))"^^geo:wktLiteral .
        """;

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;

    @BeforeAll
    static void buildAndOpen() throws Exception {
        File ttl = dir.resolve("badpoly.ttl").toFile();
        File h5 = dir.resolve("badpoly.ttl.h5").toFile();
        Files.write(ttl.toPath(), TTL.getBytes(StandardCharsets.UTF_8));
        // The collapsing and self-intersecting geometries must not abort this.
        HDF5Writer.Builder()
                .setSource(ttl).setDestination(h5)
                .setSpatial(true).setFeatures(false)
                .build().write();
        bg = new BeakGraph(new HDF5Reader(h5));
        ds = bg.getDataset();
    }

    @AfterAll
    static void closeReader() {
        if (bg != null) bg.close();
    }

    private static int count(String where) {
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(
                "PREFIX ex: <http://ex.org/> SELECT * WHERE { " + where + " }")).build()) {
            ResultSet rs = qe.execSelect();
            int n = 0;
            while (rs.hasNext()) { rs.next(); n++; }
            return n;
        }
    }

    @Test
    void goodGeometryIsIndexedDespiteBadNeighbors() {
        assertTrue(count("GRAPH <" + Params.SPATIALSTRING + "> { ex:good ?p ?o }") > 0,
            "the well-formed geometry must be spatially indexed");
    }

    @Test
    void collapsedGeometryHasNoScaledPyramidButBuildSurvives() {
        // Snapping collapses the sub-pixel polygon, so no scaled-WKT pyramid is
        // produced for it (and - critically - the build did not abort). Its
        // recall-safe index cells ARE still written: the geometry stays findable,
        // and sfIntersects verification uses the precise original WKT.
        assertEquals(0, count("GRAPH <" + Params.SPATIALSTRING + "> { ex:tiny <https://halcyon.is/ns/asWKT0> ?o }"));
    }

    @Test
    void sourceQuadsSurviveRegardlessOfGeometryFate() {
        assertEquals(3, count("?s <http://www.opengis.net/ont/geosparql#asWKT> ?o"));
    }

    @Test
    void unfixablePolygonReturnsNullInsteadOfThrowing() {
        // Direct unit check of the policy seam: a geometry the fixer cannot repair
        // must come back as "skip" (null), never as an exception that would abort
        // the build. An empty polygon exercises the null/empty guard; the
        // collapsing TTL geometry above exercises the snap-collapse path end-to-end.
        assertNull(PolygonScaler.toPolygons(
            new org.locationtech.jts.geom.GeometryFactory().createPolygon()));
    }
}
