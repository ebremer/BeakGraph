package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.huge.SpatialAugmenter;
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
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * BG-93: the tile pyramid enumerated every 512-unit tile of a geometry's
 * envelope with a JTS intersection each, unbounded in extent: a 5e6-unit
 * outline meant ~1e8 tiles at the finest level (hours), and each hit
 * allocated a quad. Levels over {@link SpatialAugmenter#MAX_GRID_TILES}
 * are now skipped; the coarser levels and the (always bounded) Hilbert
 * index cells keep the geometry findable.
 */
@Timeout(180)
class SpatialWideExtentTest {

    private static final String PREFIXES = """
        PREFIX ex: <http://ex.org/>
        PREFIX geo: <http://www.opengis.net/ont/geosparql#>
        PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
        """;
    private static final String TTL = """
        @prefix ex: <http://ex.org/> .
        @prefix geo: <http://www.opengis.net/ont/geosparql#> .
        ex:wide geo:asWKT "POLYGON((0 0,5000000 0,5000000 5000000,0 5000000,0 0))"^^geo:wktLiteral .
        ex:small geo:asWKT "POLYGON((100 100,110 100,110 110,100 110,100 100))"^^geo:wktLiteral .
        """;

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;

    @BeforeAll
    static void build() throws Exception {
        File ttl = dir.resolve("wide.ttl").toFile();
        File h5 = dir.resolve("wide.ttl.h5").toFile();
        Files.write(ttl.toPath(), TTL.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setSource(ttl).setDestination(h5).setSpatial(true).setFeatures(false).build().write();
        bg = new BeakGraph(new HDF5Reader(h5));
        ds = bg.getDataset();
    }

    @AfterAll
    static void close() {
        if (bg != null) bg.close();
    }

    private static Set<String> intersecting(String regionWkt) {
        Set<String> hits = new HashSet<>();
        String q = PREFIXES + "SELECT ?f WHERE { ?f geo:asWKT ?w FILTER(geof:sfIntersects(?w, \"" + regionWkt + "\"^^geo:wktLiteral)) }";
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(q)).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                hits.add(rs.next().getResource("f").getURI());
            }
        }
        return hits;
    }

    @Test
    void aWideGeometryBuildsInBoundedTimeAndStaysFindable() {
        assertEquals(Set.of("http://ex.org/wide"), intersecting("POLYGON((1000 1000,2000 1000,2000 2000,1000 2000,1000 1000))"));
        assertEquals(Set.of("http://ex.org/wide", "http://ex.org/small"), intersecting("POLYGON((102 102,108 102,108 108,102 108,102 102))"));
        assertEquals(Set.of("http://ex.org/wide"), intersecting("POLYGON((4999000 4999000,4999900 4999000,4999900 4999900,4999000 4999900,4999000 4999000))"));
        long tileGraphs;
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(
                "SELECT (COUNT(DISTINCT ?g) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } FILTER(STRSTARTS(STR(?g), \"urn:x-beakgraph:grid\")) }")).build()) {
            tileGraphs = qe.execSelect().next().getLiteral("n").getLong();
        }
        assertTrue(tileGraphs > 0, "the coarse pyramid levels within budget are still emitted");
        assertTrue(tileGraphs <= 2 * SpatialAugmenter.MAX_GRID_TILES,
                "the finest levels must be skipped, not enumerated: " + tileGraphs + " tile graphs");
    }
}
