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
 * Regression test for BG-66: on a store built WITHOUT the spatial index (the
 * CLI and builder default), a {@code geof:sfIntersects} query used to be seeded
 * from an empty candidate set and silently returned zero rows. Without the
 * index the filter must fall back to plain JTS evaluation and give the same
 * exact answer an indexed store gives.
 */
class SpatialFallbackWithoutIndexTest {

    private static final String REGION = "POLYGON((100 100,300 100,300 300,100 300,100 100))";

    private static final String TTL = """
        @prefix ex:  <http://ex.org/> .
        @prefix geo: <http://www.opengis.net/ont/geosparql#> .
        ex:small geo:asWKT "POLYGON((150 150,160 150,160 160,150 160,150 150))"^^geo:wktLiteral .
        ex:big   geo:asWKT "POLYGON((0 0,1000 0,1000 1000,0 1000,0 0))"^^geo:wktLiteral .
        ex:strad geo:asWKT "POLYGON((50 120,150 120,150 200,50 200,50 120))"^^geo:wktLiteral .
        ex:far   geo:asWKT "POLYGON((5000 5000,5100 5000,5100 5100,5000 5100,5000 5000))"^^geo:wktLiteral .
        ex:trap  geo:asWKT "POLYGON((260 360,360 260,370 370,260 360))"^^geo:wktLiteral .
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
        File ttl = dir.resolve("nospatial.ttl").toFile();
        File h5 = dir.resolve("nospatial.ttl.h5").toFile();
        Files.write(ttl.toPath(), TTL.getBytes(StandardCharsets.UTF_8));
        // Deliberately NO spatial index.
        HDF5Writer.Builder().setSource(ttl).setDestination(h5)
                .setSpatial(false).setFeatures(false).build().write();
        bg = new BeakGraph(new HDF5Reader(h5));
        ds = bg.getDataset();
    }

    @AfterAll
    static void closeReader() {
        if (bg != null) bg.close();
    }

    private static List<String> intersecting(String pattern, String regionWkt) {
        List<String> hits = new ArrayList<>();
        String q = PREFIXES + "SELECT ?f WHERE { " + pattern
                + " FILTER(geof:sfIntersects(?w, \"" + regionWkt + "\"^^geo:wktLiteral)) }";
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
    void variableSubjectFallsBackToJtsWithoutIndex() {
        List<String> hits = intersecting("?f geo:asWKT ?w", REGION);
        assertEquals(Set.of("http://ex.org/small", "http://ex.org/big", "http://ex.org/strad"),
                new HashSet<>(hits), "sfIntersects must be answered by JTS when no spatial index exists");
        assertEquals(3, hits.size(), "each match exactly once");
    }

    @Test
    void containingGeometryFoundWithoutIndex() {
        assertEquals(List.of("http://ex.org/big"),
                intersecting("?f geo:asWKT ?w", "POLYGON((600 600,610 600,610 610,600 610,600 600))"));
    }

    @Test
    void disjointRegionStillEmpty() {
        assertEquals(List.of(),
                intersecting("?f geo:asWKT ?w", "POLYGON((8000 8000,8100 8000,8100 8100,8000 8100,8000 8000))"));
    }

    @Test
    void concreteSubjectUnchanged() {
        assertEquals(List.of(), intersecting("ex:far geo:asWKT ?w . BIND(ex:far AS ?f)", REGION));
        assertEquals(List.of("http://ex.org/strad"), intersecting("ex:strad geo:asWKT ?w . BIND(ex:strad AS ?f)", REGION));
    }
}
