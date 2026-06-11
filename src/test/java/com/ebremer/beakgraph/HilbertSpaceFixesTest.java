package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.hdf5.jena.HilbertPolygon;
import com.ebremer.halcyon.geometry.Point;
import com.ebremer.halcyon.hilbert.HilbertSpace;
import java.util.ArrayList;
import org.davidmoten.hilbert.Range;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for two HilbertSpace bugs:
 * <ul>
 *   <li>H8 - {@code fromWkt} did not strip the optional CRS prefix GeoSPARQL
 *       wktLiterals may carry and threw a bare {@code java.lang.Error} on any
 *       parse problem, crashing the spatial query path for standard-form data.</li>
 *   <li>H10 - both {@code inRange} overloads computed {@code contains(...)} in
 *       arrow-switch statements, discarded the result, and always returned
 *       false (breaking the {@code getPolygon} boundary walk).</li>
 * </ul>
 */
class HilbertSpaceFixesTest {

    private static final String CRS = "<http://www.opengis.net/def/crs/EPSG/0/4326> ";
    private static final String SQUARE = "POLYGON((0 0,8 0,8 8,0 8,0 0))";

    // --- H8 -----------------------------------------------------------------

    @Test
    void fromWktAcceptsCrsPrefixedLiterals() {
        org.locationtech.jts.geom.Polygon p = HilbertSpace.fromWkt(CRS + SQUARE);
        assertNotNull(p);
        assertEquals(64.0, p.getArea(), 1e-9);
        // And the plain form keeps working.
        assertEquals(64.0, HilbertSpace.fromWkt(SQUARE).getArea(), 1e-9);
    }

    @Test
    void polygon2HilbertAcceptsCrsPrefixedLiterals() {
        ArrayList<Range> ranges = HilbertPolygon.Polygon2Hilbert(CRS + SQUARE, 0);
        assertFalse(ranges.isEmpty(), "a real polygon must produce Hilbert ranges");
    }

    @Test
    void fromWktRejectsGarbageWithCatchableException() {
        // Not a java.lang.Error: callers must be able to catch and degrade.
        try {
            HilbertSpace.fromWkt("NOT A POLYGON AT ALL");
        } catch (RuntimeException expected) {
            return;
        }
        throw new AssertionError("malformed WKT must raise a RuntimeException");
    }

    // --- H10 ----------------------------------------------------------------

    @Test
    void inRangeReportsNeighborMembership() {
        // A single-cell range at Hilbert index of (5,4).
        long idx = HilbertSpace.hc.index(5, 4);
        ArrayList<Range> ranges = new ArrayList<>();
        ranges.add(new Range(idx, idx));

        // From (5,5): the N neighbour is (5,4) -> in range; S is (5,6) -> not.
        assertTrue(HilbertSpace.inRange(ranges, new Point(5, 5), HilbertSpace.N),
            "north neighbour (5,4) is in the range");
        assertFalse(HilbertSpace.inRange(ranges, new Point(5, 5), HilbertSpace.S),
            "south neighbour (5,6) is not in the range");
        // From (4,4): the E neighbour is (5,4) -> in range; W is (3,4) -> not.
        assertTrue(HilbertSpace.inRange(ranges, new Point(4, 4), HilbertSpace.E));
        assertFalse(HilbertSpace.inRange(ranges, new Point(4, 4), HilbertSpace.W));
        // Diagonals: from (6,5), NW is (5,4) -> in range.
        assertTrue(HilbertSpace.inRange(ranges, new Point(6, 5), HilbertSpace.NW));
    }
}
