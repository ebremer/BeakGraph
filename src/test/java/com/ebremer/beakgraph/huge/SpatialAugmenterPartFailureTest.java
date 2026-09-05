package com.ebremer.beakgraph.huge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.hdf5.writers.PositionalDictionaryWriterBuilder;
import com.ebremer.ns.GEO;
import com.ebremer.halcyon.hilbert.PolygonScaler;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.Quad;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.TopologyException;

/**
 * BG-372: one catch-all around the per-part loop meant a part whose scaled
 * pyramid failed cost every LATER part its recall-safe Hilbert cells (and its
 * pyramid), while the log said only "Skipping". Cells are now emitted for
 * every part before any pyramid, and a pyramid failure is isolated per part.
 */
class SpatialAugmenterPartFailureTest {

    private static final String WKT = "MULTIPOLYGON(((0 0,32 0,32 32,0 32,0 0)),"
            + "((1000 1000,1032 1000,1032 1032,1000 1032,1000 1000)),"
            + "((2000 2000,2032 2000,2032 2032,2000 2032,2000 2000)))";

    private static Quad geometry() {
        return Quad.create(Quad.defaultGraphIRI, NodeFactory.createURI("http://ex.org/s"),
                NodeFactory.createURI("http://www.opengis.net/ont/geosparql#asWKT"),
                NodeFactory.createLiteralDT(WKT, TypeMapper.getInstance().getSafeTypeByName(GEO.wktLiteral.getURI())));
    }

    private static Set<Quad> cells(List<Quad> quads) {
        Set<Quad> out = new HashSet<>();
        for (Quad q : quads) {
            if (q.getPredicate().getURI().startsWith(PositionalDictionaryWriterBuilder.HILBERT_CELL_NS)) out.add(q);
        }
        return out;
    }

    private static long levelZeroPyramids(List<Quad> quads) {
        return quads.stream().filter(q -> Params.SPATIAL.equals(q.getGraph())
                && q.getPredicate().getURI().equals("https://halcyon.is/ns/asWKT0")).count();
    }

    @Test
    void aFailingMiddlePartCostsOnlyItsOwnPyramid() {
        int[] calls = {0};
        SpatialAugmenter failing = new SpatialAugmenter(false, part -> {
            if (++calls[0] == 2) {
                throw new TopologyException("simulated pyramid failure");
            }
            return PolygonScaler.toPolygons(part);
        });
        List<Quad> withFailure = failing.addSpatial(geometry());
        List<Quad> healthy = new SpatialAugmenter(false).addSpatial(geometry());
        assertEquals(3, calls[0], "every part's pyramid is attempted");

        Set<Quad> healthyCells = cells(healthy);
        assertTrue(healthyCells.size() >= 3, "each of the three parts covers at least one cell: " + healthyCells.size());
        assertEquals(healthyCells, cells(withFailure), "no part loses its Hilbert cells to another part's failure");
        assertEquals(3, levelZeroPyramids(healthy));
        assertEquals(2, levelZeroPyramids(withFailure), "only the failing part's pyramid is missing");
    }
}
