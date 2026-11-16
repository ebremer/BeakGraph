package com.ebremer.halcyon.hilbert;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Random;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKTReader;

/**
 * Every pyramid level PolygonScaler writes is on the integer grid, including
 * a polygon that had to be repaired. The repair (JTS buffer(0)) nodes a
 * self-crossing ring where its edges cross, which is rarely a whole pixel:
 * Halcyon's tile renderer, which read only integers, threw on WKT such as
 * {@code POLYGON ((5838.666666666667 3535.5, 5842 3533, ...))} and dropped the
 * rest of the tile's grid cell (TCGA COAD feature files, 2026-11-16).
 *
 * @author erich
 */
class PolygonScalerIntegerGridTest {

    /** Edges (0 0)-(100 31) and (100 0)-(0 41) cross at (56.94..., 17.65...):
     *  left lobe about 1167 px, right lobe about 667 px. */
    private static final String BOWTIE = "POLYGON ((0 0, 100 31, 100 0, 0 41, 0 0))";

    private static boolean onGrid(Geometry g) {
        for (Coordinate c : g.getCoordinates()) {
            if (c.x != Math.rint(c.x) || c.y != Math.rint(c.y)) {
                return false;
            }
        }
        return true;
    }

    @Test
    @DisplayName("the repair alone leaves a fractional vertex")
    void repairIsOffTheGrid() throws Exception {
        Polygon bowtie = (Polygon) new WKTReader().read(BOWTIE);
        assertFalse(bowtie.isValid());
        Polygon repaired = PolygonScaler.fixPolygon(bowtie);
        assertTrue(repaired.isValid());
        assertFalse(onGrid(repaired), repaired.toText());
    }

    @Test
    @DisplayName("a repaired polygon is written on the integer grid, at every level")
    void repairedPyramidIsOnTheGrid() throws Exception {
        Polygon[] pyramid = PolygonScaler.toPolygons((Polygon) new WKTReader().read(BOWTIE));
        assertNotNull(pyramid);
        assertTrue(pyramid.length > 1, "a pyramid, not only the base");
        // The largest lobe is the one kept, as before.
        assertEquals(1167, pyramid[0].getArea(), 15, pyramid[0].toText());
        for (Polygon level : pyramid) {
            assertTrue(level.isValid(), level.toText());
            assertTrue(onGrid(level), level.toText());
        }
        for (String wkt : PolygonScaler.toWKT(pyramid)) {
            assertFalse(wkt.contains("."), wkt);
        }
        // The String entry point too.
        for (String wkt : PolygonScaler.toWKT(PolygonScaler.toPolygons(BOWTIE))) {
            assertFalse(wkt.contains("."), wkt);
        }
    }

    @Test
    @DisplayName("a polygon already on the grid is returned as it is")
    void onGridIsIdentity() throws Exception {
        Polygon square = (Polygon) new WKTReader().read("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
        assertSame(square, PolygonScaler.onIntegerGrid(square));
    }

    @Test
    @DisplayName("random self-crossing polygons: every level valid and on the grid")
    void fuzz() {
        GeometryFactory gf = new GeometryFactory();
        Random random = new Random(20261116L);
        int repaired = 0;
        for (int n = 0; n < 1000; n++) {
            int vertices = 4 + random.nextInt(16);
            Coordinate[] ring = new Coordinate[vertices + 1];
            for (int v = 0; v < vertices; v++) {
                ring[v] = new Coordinate(random.nextInt(1000), random.nextInt(1000));
            }
            ring[vertices] = new Coordinate(ring[0]);
            Polygon polygon;
            try {
                polygon = gf.createPolygon(ring);
            } catch (IllegalArgumentException degenerate) {
                continue;
            }
            if (!polygon.isValid()) {
                repaired++;
            }
            Polygon[] pyramid = PolygonScaler.toPolygons(polygon);
            if (pyramid == null) {
                continue;
            }
            for (Polygon level : pyramid) {
                assertTrue(level.isValid(), level.toText());
                assertTrue(onGrid(level), polygon.toText() + " -> " + level.toText());
            }
        }
        assertTrue(repaired > 500, "the repair path ran " + repaired + " times");
    }
}
