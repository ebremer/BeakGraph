package com.ebremer.beakgraph.hdf5.jena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.BG;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.VoidMode;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * BG-58: filter placement runs AFTER the BG reorder. With VoID statistics
 * the reorder moves the rarer predicate first; when that is the triple
 * binding the FILTER's variable, Jena's placement turns
 * {@code (filter e (bgp name asWKT))} into
 * {@code (sequence (filter e (bgp asWKT)) (bgp name))}, and the plain
 * executor used to see an OpSequence and pass no filter to the inner BGP -
 * no spatial-index seeding, no range pushdown. Whether the index was used
 * therefore depended on the textual order of the triples and on the data.
 * Both orders must seed the Hilbert index / push the range hint now.
 */
class FilterPlacementAfterReorderTest {

    private static final String PREFIX = "PREFIX ex: <http://ex.org/> "
            + "PREFIX geo: <http://www.opengis.net/ont/geosparql#> "
            + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
            + "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ";
    private static final String REGION = "POLYGON((100 100,300 100,300 300,100 300,100 100))";

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;
    static Dataset reference;

    /** Many ex:name / ex:score triples, few geo:asWKT: the statistics rank asWKT as the selective predicate. */
    private static String fixture() {
        StringBuilder sb = new StringBuilder();
        sb.append("@prefix ex: <http://ex.org/> .\n@prefix geo: <http://www.opengis.net/ont/geosparql#> .\n");
        for (int i = 0; i < 40; i++) {
            sb.append("ex:f").append(i).append(" ex:name \"feature ").append(i).append("\" ; ex:score ").append(i).append(" .\n");
        }
        sb.append("ex:f1 geo:asWKT \"POLYGON((150 150,160 150,160 160,150 160,150 150))\"^^geo:wktLiteral .\n");
        sb.append("ex:f2 geo:asWKT \"POLYGON((0 0,1000 0,1000 1000,0 1000,0 0))\"^^geo:wktLiteral .\n");
        sb.append("ex:f3 geo:asWKT \"POLYGON((5000 5000,5100 5000,5100 5100,5000 5100,5000 5000))\"^^geo:wktLiteral .\n");
        sb.append("ex:f4 geo:asWKT \"POINT(200 200)\"^^geo:wktLiteral .\n");
        sb.append("ex:f5 geo:asWKT \"POINT(900 900)\"^^geo:wktLiteral .\n");
        return sb.toString();
    }

    @BeforeAll
    static void build() throws Exception {
        Path src = dir.resolve("placement.ttl");
        Files.writeString(src, fixture());
        File h5 = dir.resolve("placement.h5").toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(h5)
                .setVoidMode(VoidMode.EXACT).setSpatial(true).setFeatures(false).build().write();
        bg = BG.getBeakGraph(h5);
        ds = bg.getDataset();
        DatasetGraph dsg = DatasetGraphFactory.create();
        RDFParser.create().source(src.toUri().toString()).lang(Lang.TURTLE).parse(dsg);
        reference = DatasetFactory.wrap(dsg);
        assertEquals("BGReorderTransform", bg.getReorderTransform().getClass().getSimpleName(), "the store must carry usable VoID statistics");
        assertTrue(SpatialIndexIterator.isAvailable(bg), "the store must carry the spatial index");
    }

    @AfterAll
    static void close() {
        if (bg != null) bg.close();
    }

    private static List<String> rows(Dataset d, String query) {
        List<String> out = new ArrayList<>();
        try (QueryExecution qe = QueryExecution.dataset(d).query(QueryFactory.create(PREFIX + query)).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                QuerySolution qs = rs.next();
                StringBuilder sb = new StringBuilder();
                for (String v : rs.getResultVars()) sb.append(v).append('=').append(qs.get(v)).append(' ');
                out.add(sb.toString());
            }
        }
        Collections.sort(out);
        return out;
    }

    private static final String SPATIAL_FILTER = " FILTER(geof:sfIntersects(?w, \"" + REGION + "\"^^geo:wktLiteral)) }";
    private static final String NAME_FIRST = "SELECT ?f ?n WHERE { ?f ex:name ?n . ?f geo:asWKT ?w" + SPATIAL_FILTER;
    private static final String WKT_FIRST = "SELECT ?f ?n WHERE { ?f geo:asWKT ?w . ?f ex:name ?n" + SPATIAL_FILTER;

    @Test
    void spatialSeedingSurvivesReorderInducedSequence() {
        List<String> expected = rows(reference, NAME_FIRST);
        assertEquals(List.of("f=http://ex.org/f1 n=feature 1 ", "f=http://ex.org/f2 n=feature 2 ", "f=http://ex.org/f4 n=feature 4 "),
                expected, "fixture sanity: three features intersect the region");

        // Textual order name, asWKT: ARQ leaves (filter e (bgp name asWKT)); the
        // BG reorder puts the rarer asWKT first, and placement yields an OpSequence.
        long before = SpatialIndexIterator.HITS.get();
        assertEquals(expected, rows(ds, NAME_FIRST), NAME_FIRST);
        assertTrue(SpatialIndexIterator.HITS.get() > before,
                "the geometry triple must be seeded from the spatial index when the filter is nested in an OpSequence");

        // Textual order asWKT, name: ARQ pre-places the filter around the single-triple BGP.
        before = SpatialIndexIterator.HITS.get();
        assertEquals(expected, rows(ds, WKT_FIRST), WKT_FIRST);
        assertTrue(SpatialIndexIterator.HITS.get() > before, "the pre-placed order must keep seeding too");
    }

    @Test
    void rangePushdownSurvivesReorderInducedSequence() {
        // ex:score is as frequent as ex:name here; with a bound object the reorder
        // still prefers the score triple (an object-bound pattern of equal count
        // costs the same, so the filtered variable's triple is first either way
        // after placement). Both orders must hand the >= bound to the iterator.
        String scoreLast = "SELECT ?f ?v WHERE { ?f ex:name ?n . ?f ex:score ?v FILTER(?v >= 37) }";
        String scoreFirst = "SELECT ?f ?v WHERE { ?f ex:score ?v . ?f ex:name ?n FILTER(?v >= 37) }";
        List<String> expected = rows(reference, scoreLast);
        assertEquals(3, expected.size(), "fixture sanity: scores 37, 38, 39");
        for (String q : new String[]{scoreLast, scoreFirst}) {
            long before = FilterBounds.HITS.get();
            assertEquals(expected, rows(ds, q), q);
            assertTrue(FilterBounds.HITS.get() > before, "the >= bound must reach the reader: " + q);
        }
    }

    @Test
    void filterSemanticsAreStillEnforcedByThePlan() {
        // The hints are recall-safe candidates; the OpFilter must still verify.
        // A region touching nothing must answer nothing even though the index
        // seeding and the range bound both run.
        String none = "SELECT ?f WHERE { ?f ex:name ?n . ?f geo:asWKT ?w FILTER(geof:sfIntersects(?w, \"POINT(7777 7777)\"^^geo:wktLiteral)) }";
        assertEquals(List.of(), rows(ds, none));
        String strict = "SELECT ?f WHERE { ?f ex:name ?n . ?f ex:score ?v FILTER(?v > 38 && ?v < 39) }";
        assertEquals(List.of(), rows(ds, strict));
        assertFalse(rows(ds, "SELECT ?f WHERE { ?f ex:name ?n . ?f ex:score ?v FILTER(?v > 38) }").isEmpty());
    }
}
