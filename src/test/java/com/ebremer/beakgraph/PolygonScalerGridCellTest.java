package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ebremer.halcyon.hilbert.GridCell;
import com.ebremer.halcyon.hilbert.PolygonScaler;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

/**
 * PolygonScaler.getGridCells must agree with the tile grid the writer
 * persists: every scale level's tiles are GRIDTILESIZE units on a side in that
 * level's own (already divided) coordinates - generateGridURNs stamps the
 * stored per-tile graph URNs with exactly that formula. The old code
 * multiplied the tile size by 2^scale ON TOP of the already-scaled polygon, so
 * for scale >= 1 every computed cell disagreed with every stored tile.
 */
class PolygonScalerGridCellTest {

    @Test
    void gridCellsMatchThePersistedTileGrid() {
        GeometryFactory gf = new GeometryFactory();
        // Coordinates are scale-1 coordinates (base 2048..2060 halved).
        Polygon atScale1 = (Polygon) gf.toGeometry(new Envelope(1024, 1030, 0, 10));
        List<GridCell> cells = PolygonScaler.getGridCells(new ArrayList<>(), atScale1, (short) 1);
        assertEquals(1, cells.size());
        assertEquals(1, cells.get(0).scale);
        assertEquals(2, cells.get(0).x, "x = floor(1024 / 512), the writer's tile column");
        assertEquals(0, cells.get(0).y);
    }

    @Test
    void scaleZeroCellsAreUnchanged() {
        GeometryFactory gf = new GeometryFactory();
        Polygon atScale0 = (Polygon) gf.toGeometry(new Envelope(600, 610, 0, 10));
        List<GridCell> cells = PolygonScaler.getGridCells(new ArrayList<>(), atScale0, (short) 0);
        assertEquals(1, cells.size());
        assertEquals(0, cells.get(0).scale);
        assertEquals(1, cells.get(0).x, "x = floor(600 / 512)");
        assertEquals(0, cells.get(0).y);
    }
}
