package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
 * End-to-end correctness tests for the spatial index + geof:sfIntersects.
 * The index must be a recall-safe PRE-FILTER (bbox cell-cover overlap - never a
 * false negative) and the JTS-backed sfIntersects filter must stay in the plan
 * as the verification stage (killing the index's false positives). The old
 * corner-only design failed both ways: a geometry containing or straddling the
 * query region never matched, candidates were never re-verified, and one match
 * could appear up to four times.
 */
class SpatialQueryCorrectnessTest {

    // Query region: the square [100,300] x [100,300].
    private static final String REGION = "POLYGON((100 100,300 100,300 300,100 300,100 100))";

    private static final String TTL = """
        @prefix ex:  <http://ex.org/> .
        @prefix geo: <http://www.opengis.net/ont/geosparql#> .
        # entirely inside the region
        ex:small geo:asWKT "POLYGON((150 150,160 150,160 160,150 160,150 150))"^^geo:wktLiteral .
        # CONTAINS the whole region - the classic corner-index false negative
        ex:big geo:asWKT "POLYGON((0 0,1000 0,1000 1000,0 1000,0 0))"^^geo:wktLiteral .
        # straddles the region's left edge - corners outside, overlap real
        ex:strad geo:asWKT "POLYGON((50 120,150 120,150 200,50 200,50 120))"^^geo:wktLiteral .
        # far away - bbox disjoint, excluded by the index
        ex:far geo:asWKT "POLYGON((5000 5000,5100 5000,5100 5100,5000 5100,5000 5000))"^^geo:wktLiteral .
        # bbox overlaps the region corner but the triangle itself does not
        # (x + y >= 620 everywhere; the region maxes out at 600) - an index
        # candidate that the JTS verification stage must reject
        ex:trap geo:asWKT "POLYGON((260 360,360 260,370 370,260 360))"^^geo:wktLiteral .
        # residual-filter sanity data
        ex:n1 ex:name "axb" .
        ex:n2 ex:name "ab" .
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
        File ttl = dir.resolve("spatialq.ttl").toFile();
        File h5 = dir.resolve("spatialq.ttl.h5").toFile();
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

    private static List<String> intersecting(String regionWkt) {
        List<String> hits = new ArrayList<>();
        String q = PREFIXES +
            "SELECT ?f WHERE { ?f geo:asWKT ?w FILTER(geof:sfIntersects(?w, \"" + regionWkt + "\"^^geo:wktLiteral)) }";
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(q)).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                RDFNode n = rs.next().get("f");
                if (n != null && n.isURIResource()) hits.add(n.asResource().getURI());
            }
        }
        return hits;
    }

    @Test
    void intersectionMatchesGeometryNotCorners() {
        List<String> hits = intersecting(REGION);
        Set<String> distinct = new HashSet<>(hits);
        assertEquals(Set.of("http://ex.org/small", "http://ex.org/big", "http://ex.org/strad"), distinct,
            "containing and straddling geometries must match; disjoint and bbox-only-overlap must not");
        assertEquals(distinct.size(), hits.size(),
            "each matching geometry must appear exactly once (no per-corner/per-range duplicates)");
    }

    @Test
    void containingGeometryAloneIsFound() {
        // A region buried deep inside only ex:big - zero stored corners or cells
        // of ex:big lie anywhere near it; only a recall-safe cover finds it.
        assertEquals(List.of("http://ex.org/big"),
            intersecting("POLYGON((600 600,610 600,610 610,600 610,600 600))"));
    }

    @Test
    void disjointRegionMatchesNothing() {
        assertEquals(List.of(), intersecting("POLYGON((8000 8000,8100 8000,8100 8100,8000 8100,8000 8000))"));
    }

    @Test
    void residualFiltersStillApplyToOrdinaryPatterns() {
        // Sanity guard for the same plan machinery the verify stage rides on:
        // a non-pushdownable FILTER (regex) must still be evaluated.
        String q = "PREFIX ex: <http://ex.org/> SELECT ?s WHERE { ?s ex:name ?n FILTER(regex(?n, \"x\")) }";
        List<String> hits = new ArrayList<>();
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(q)).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) hits.add(rs.next().getResource("s").getURI());
        }
        assertEquals(List.of("http://ex.org/n1"), hits);
    }
}
