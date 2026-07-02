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

    // ex:degen is structurally invalid WKT (a two-point ring): JTS throws
    // IllegalArgumentException for it, not ParseException - real pathology data
    // contains these, and one of them must not abort the parse of a whole file.
    private static final String TTL = """
        @prefix ex:  <http://ex.org/> .
        @prefix geo: <http://www.opengis.net/ont/geosparql#> .
        ex:good geo:asWKT "POLYGON((0 0,64 0,64 64,0 64,0 0))"^^geo:wktLiteral .
        ex:tiny geo:asWKT "POLYGON((0.1 0.1,0.4 0.1,0.4 0.4,0.1 0.4,0.1 0.1))"^^geo:wktLiteral .
        ex:bow  geo:asWKT "POLYGON((0 0,10 10,10 0,0 10,0 0))"^^geo:wktLiteral .
        ex:degen geo:asWKT "POLYGON((0 0,1 1))"^^geo:wktLiteral .
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
        assertEquals(4, count("?s <http://www.opengis.net/ont/geosparql#asWKT> ?o"));
    }

    @Test
    void structurallyInvalidWktIsKeptAsATermAndSkippedSpatially() {
        // The two-point ring survives as an RDF term (lexical + datatype intact)...
        assertEquals(1, count("ex:degen <http://www.opengis.net/ont/geosparql#asWKT> "
            + "\"POLYGON((0 0,1 1))\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>"));
        // ...but contributes nothing to the spatial index.
        assertEquals(0, count("GRAPH <" + Params.SPATIALSTRING + "> { ex:degen ?p ?o }"));
    }

    @Test
    void selfIntersectingStoredGeometryIsReturnedBySpatialQueries() {
        // The bowtie is deliberately indexed at build time; the sfIntersects
        // verification stage must repair it rather than throw (which made Jena
        // drop the row - an indexed geometry no query could ever return).
        // Region inside the bowtie's left lobe; it also overlaps ex:good.
        assertEquals(1, count("?f <http://www.opengis.net/ont/geosparql#asWKT> ?w . "
            + "FILTER(?f = ex:bow) "
            + "FILTER(<http://www.opengis.net/def/function/geosparql/sfIntersects>(?w, "
            + "\"POLYGON((1 4,2 4,2 6,1 6,1 4))\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>))"),
            "the self-intersecting geometry must be returned by a query over its real area");
    }

    @Test
    void invalidQueryRegionStillAnswersInsteadOfEmptying() {
        // A self-intersecting QUERY region must be repaired too, not silently
        // turned into zero rows for every candidate.
        assertEquals(1, count("ex:good <http://www.opengis.net/ont/geosparql#asWKT> ?w . "
            + "FILTER(<http://www.opengis.net/def/function/geosparql/sfIntersects>(?w, "
            + "\"POLYGON((0 0,20 20,20 0,0 20,0 0))\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>))"));
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
