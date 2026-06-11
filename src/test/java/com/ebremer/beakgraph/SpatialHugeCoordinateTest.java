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
import java.util.Locale;
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
 * Coordinates at or beyond 2^31 exceed the 31-bit Hilbert curve's domain. The
 * davidmoten curve silently MASKS such coordinates onto unrelated cells, so
 * the written cover and the queried cover aliased differently and genuine
 * intersections were silently lost. Both sides now clamp into the domain with
 * the same monotone projection: out-of-domain geometry indexes coarsely at
 * the domain-edge cells (recall-safe) and JTS verification does the rest.
 */
class SpatialHugeCoordinateTest {

    private static final long BASE = 1L << 31;

    private static final String PREFIXES = """
        PREFIX ex:   <http://ex.org/>
        PREFIX geo:  <http://www.opengis.net/ont/geosparql#>
        PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
        """;

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;

    private static String square(long minX, long minY, long size) {
        long maxX = minX + size, maxY = minY + size;
        return String.format(Locale.ROOT, "POLYGON((%d %d,%d %d,%d %d,%d %d,%d %d))",
                minX, minY, maxX, minY, maxX, maxY, minX, maxY, minX, minY);
    }

    @BeforeAll
    static void buildAndOpen() throws Exception {
        String ttl = "@prefix ex: <http://ex.org/> .\n"
                + "@prefix geo: <http://www.opengis.net/ont/geosparql#> .\n"
                + "ex:huge geo:asWKT \"" + square(BASE + 100, BASE + 100, 100) + "\"^^geo:wktLiteral .\n"
                + "ex:small geo:asWKT \"" + square(500, 500, 60) + "\"^^geo:wktLiteral .\n";
        File src = dir.resolve("huge.ttl").toFile();
        File h5 = dir.resolve("huge.ttl.h5").toFile();
        Files.write(src.toPath(), ttl.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setSource(src).setDestination(h5)
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
    void beyondDomainGeometryIsFoundByOverlappingRegion() {
        assertEquals(Set.of("http://ex.org/huge"),
            intersecting(square(BASE + 50, BASE + 50, 100)));
    }

    @Test
    void beyondDomainRegionWithNoOverlapMatchesNothing() {
        // Both clamp onto the domain-edge cells (candidates!), so the JTS
        // verification must reject the non-overlapping pair.
        assertEquals(Set.of(), intersecting(square(BASE + 10_000, BASE + 10_000, 100)));
    }

    @Test
    void inDomainQueriesAreUnaffected() {
        assertEquals(Set.of("http://ex.org/small"), intersecting(square(480, 480, 60)));
    }
}
