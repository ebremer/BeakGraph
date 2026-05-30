package com.ebremer.beakgraph;

import com.ebremer.beakgraph.features.ShapeAnalysis;
import java.awt.image.BufferedImage;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKTReader;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code ShapeAnalysis.getBufferedImage} sizes the raster to the polygon's bounding box and
 * must translate the polygon to that box's origin before rasterizing. If the translation is
 * dropped, a polygon far from (0,0) is drawn entirely off-canvas, the image stays blank, and
 * every pixel-derived shape feature (PixelSurface, the PCA axis lengths/elongation) collapses
 * to garbage. These tests fail if the translation result is discarded.
 */
class ShapeAnalysisRenderTest {

    private static Polygon square(int x0, int y0, int side) throws Exception {
        String wkt = String.format("POLYGON ((%d %d, %d %d, %d %d, %d %d, %d %d))",
            x0, y0, x0 + side, y0, x0 + side, y0 + side, x0, y0 + side, x0, y0);
        return (Polygon) new WKTReader().read(wkt);
    }

    @Test
    void offOriginPolygonIsRenderedInsideTheImage() throws Exception {
        Polygon far = square(10_000, 7_000, 120);
        BufferedImage bi = ShapeAnalysis.getBufferedImage(far);
        assertEquals(120, bi.getWidth());
        assertEquals(120, bi.getHeight());
        assertTrue(ShapeAnalysis.Area(bi) > 0,
            "off-origin polygon was drawn outside the image - translation to origin was lost");
    }

    @Test
    void rasterizedRegionIsFilledNotJustOutlined() throws Exception {
        int side = 150;
        int lit = ShapeAnalysis.Area(ShapeAnalysis.getBufferedImage(square(0, 0, side)));
        // A filled side x side square lights ~side^2 pixels (the region); a stroked outline would
        // light only ~perimeter (~4*side). The half-area floor cleanly separates the two.
        assertTrue(lit > side * side / 2,
            "expected a filled region (~" + (side * side) + " px) but got " + lit + " - shape was only outlined");
    }

    @Test
    void rasterizationIsPositionIndependent() throws Exception {
        // The same shape at two very different locations must rasterize identically once it is
        // translated to the origin. At (0,0) the dropped translation is a no-op (identity), so
        // only the far square exposes the bug - making the two pixel counts diverge.
        int atOrigin = ShapeAnalysis.Area(ShapeAnalysis.getBufferedImage(square(0, 0, 150)));
        int faraway  = ShapeAnalysis.Area(ShapeAnalysis.getBufferedImage(square(50_000, 30_000, 150)));
        assertTrue(atOrigin > 0, "baseline polygon produced a blank image");
        assertEquals(atOrigin, faraway,
            "rasterized pixel count depends on absolute position - polygon is not translated to origin");
    }
}
