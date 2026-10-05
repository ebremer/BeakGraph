package com.ebremer.beakgraph.hdf5.jena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.halcyon.hilbert.HilbertSpace;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.davidmoten.hilbert.Range;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression tests for BG-67: the query-side Hilbert cover walked the
 * PERIMETER of the region's cell box (the davidmoten range query materialises
 * one boxed index per perimeter cell). A region clamping to the whole
 * [0, 2^31) domain - any coordinate at or beyond 2^31, or simply a huge
 * polygon - walked ~2^33 cells at scale 0 inside the iterator constructor:
 * an unauthenticated request could hang or OOM the endpoint. Boxes over the
 * perimeter budget are now covered on a coarser block curve, which the
 * curve's self-similarity makes an exact superset of the cell cover.
 */
@Timeout(120)
class SpatialHugeRegionTest {

    private static final long DOMAIN_MAX = (1L << 31) - 1;

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
        // A point and small polygons index at scale 0 (2x2 / few-cell covers),
        // which is where a domain-spanning region's walk is longest.
        String ttl = "@prefix ex: <http://ex.org/> .\n"
                + "@prefix geo: <http://www.opengis.net/ont/geosparql#> .\n"
                + "ex:pt geo:asWKT \"POINT(1000 1000)\"^^geo:wktLiteral .\n"
                + "ex:near geo:asWKT \"" + square(500, 500, 60) + "\"^^geo:wktLiteral .\n"
                + "ex:far geo:asWKT \"" + square(1_500_000_000L, 1_500_000_000L, 60) + "\"^^geo:wktLiteral .\n"
                + "ex:wide geo:asWKT \"" + square(0, 0, 1_000_000) + "\"^^geo:wktLiteral .\n";
        File src = dir.resolve("region.ttl").toFile();
        File h5 = dir.resolve("region.ttl.h5").toFile();
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

    // --- end to end -------------------------------------------------------

    @Test
    void domainSpanningRegionAnswersPromptly() {
        long t0 = System.nanoTime();
        Set<String> hits = intersecting("POLYGON((0 0,1e10 0,1e10 1e10,0 1e10,0 0))");
        long ms = (System.nanoTime() - t0) / 1_000_000;
        assertEquals(Set.of("http://ex.org/pt", "http://ex.org/near", "http://ex.org/far", "http://ex.org/wide"), hits);
        assertTrue(ms < 30_000, "domain-spanning region took " + ms + " ms");
    }

    @Test
    void hugeButPartialRegionKeepsPrecision() {
        // Over budget at the fine scales (coarsened), yet the JTS verification
        // still rejects the geometry outside the region.
        assertEquals(Set.of("http://ex.org/pt", "http://ex.org/near", "http://ex.org/wide"),
                intersecting(square(0, 0, 1_000_000_000L)));
        assertEquals(Set.of("http://ex.org/far"),
                intersecting(square(1_400_000_000L, 1_400_000_000L, 700_000_000L)));
    }

    @Test
    void negativeToBeyondDomainRegionAnswersPromptly() {
        assertEquals(Set.of("http://ex.org/pt", "http://ex.org/near", "http://ex.org/far", "http://ex.org/wide"),
                intersecting("POLYGON((-1e12 -1e12,1e12 -1e12,1e12 1e12,-1e12 1e12,-1e12 -1e12))"));
    }

    // --- the cover itself ---------------------------------------------------

    @Test
    void domainSpanningCoverIsBoundedAndCheap() {
        long t0 = System.nanoTime();
        List<Range> cover = SpatialIndexIterator.coverRanges(new long[]{0, 0}, new long[]{DOMAIN_MAX, DOMAIN_MAX});
        long ms = (System.nanoTime() - t0) / 1_000_000;
        assertEquals(1, cover.size(), "the whole domain is one range");
        assertEquals(0L, cover.get(0).low());
        assertEquals((1L << 62) - 1, cover.get(0).high());
        assertTrue(ms < 5_000, "domain cover took " + ms + " ms");
    }

    @Test
    void coarsenedCoverIsAnExactSupersetOfTheFineCover() {
        // Just over budget: perimeter 4 * 5000 > 2^14, so the cover is built on
        // the k=1 block curve; the direct fine cover is still cheap enough to
        // compare against here.
        long[] lo = {12_345, 67_890};
        long[] hi = {lo[0] + 4_999, lo[1] + 4_999};
        List<Range> coarse = SpatialIndexIterator.coverRanges(lo, hi);
        assertTrue(coarse.size() <= 256);
        for (Range fine : HilbertSpace.hc.query(lo, hi)) {
            boolean covered = false;
            for (Range c : coarse) {
                if (c.low() <= fine.low() && fine.high() <= c.high()) { covered = true; break; }
            }
            assertTrue(covered, "fine range " + fine + " not inside the coarsened cover");
        }
        // And every stored cell of every geometry in the box is inside the cover.
        Random rnd = new Random(7);
        for (int i = 0; i < 10_000; i++) {
            long x = lo[0] + rnd.nextInt(5_000), y = lo[1] + rnd.nextInt(5_000);
            long idx = HilbertSpace.hc.index(new long[]{x, y});
            boolean covered = false;
            for (Range c : coarse) {
                if (c.low() <= idx && idx <= c.high()) { covered = true; break; }
            }
            assertTrue(covered, "cell (" + x + "," + y + ") not inside the coarsened cover");
        }
    }

    @Test
    void underBudgetBoxesUseTheDirectCover() {
        long[] lo = {100, 200};
        long[] hi = {1_100, 1_200};   // perimeter 4004 < 2^14: no coarsening
        List<Range> cover = SpatialIndexIterator.coverRanges(lo, hi);
        assertTrue(cover.size() <= 256);
        // The (merged, capped) cover still contains every direct range.
        for (Range fine : HilbertSpace.hc.query(lo, hi)) {
            boolean covered = false;
            for (Range c : cover) {
                if (c.low() <= fine.low() && fine.high() <= c.high()) { covered = true; break; }
            }
            assertTrue(covered, "direct range " + fine + " not inside the cover");
        }
    }

    @Test
    void blockCurveIsSelfSimilarWithTheCellCurve() {
        // The property the coarsened cover relies on; guards the hilbert-curve
        // dependency against a version that changes the curve's construction.
        Random rnd = new Random(42);
        for (int k = 1; k <= 30; k++) {
            for (int i = 0; i < 500; i++) {
                long x = rnd.nextLong() & DOMAIN_MAX, y = rnd.nextLong() & DOMAIN_MAX;
                long fine = HilbertSpace.hc.index(new long[]{x, y});
                long block = HilbertSpace.blocks(k).index(new long[]{x >> k, y >> k});
                assertEquals(fine >> (2 * k), block, "k=" + k + " at (" + x + "," + y + ")");
            }
        }
    }
}
