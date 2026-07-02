package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ebremer.beakgraph.features.ShapeAnalysis;
import com.ebremer.halcyon.geometry.Point;
import com.ebremer.halcyon.geometry.Vector2D;
import java.awt.image.BufferedImage;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for feature-math defects:
 * <ul>
 *   <li>Vector2D.Magnitude(Point) computed sqrt(x*x + y+y) - addition instead
 *       of multiplication - and overflowed int for coordinates above ~46340.</li>
 *   <li>ShapeAnalysis.isEdge read the four neighbours without bounds checks, so
 *       a filled pixel on the image border threw ArrayIndexOutOfBoundsException
 *       out of Circumference.</li>
 * </ul>
 */
class FeatureMathTest {

    @Test
    void magnitudeUsesSquaresNotSums() {
        assertEquals(5.0, Vector2D.Magnitude(new Point(3, 4)), 1e-9);
        assertEquals(13.0, Vector2D.Magnitude(new Point(5, 12)), 1e-9);
    }

    @Test
    void magnitudeSurvivesLargeCoordinates() {
        // 50000^2 overflows int; slide-scale coordinates routinely exceed 46340.
        double expected = Math.sqrt(2.0 * 50000.0 * 50000.0);
        assertEquals(expected, Vector2D.Magnitude(new Point(50000, 50000)), 1e-3);
    }

    @Test
    void circumferenceHandlesShapesTouchingTheBorder() {
        // Every pixel filled: all are border/edge pixels. Previously isEdge read
        // (x-1,y) etc. without bounds checks and threw on the first border pixel.
        BufferedImage bi = new BufferedImage(3, 3, BufferedImage.TYPE_INT_ARGB);
        for (int x = 0; x < 3; x++) {
            for (int y = 0; y < 3; y++) {
                bi.setRGB(x, y, 0xFFFFFFFF);
            }
        }
        assertEquals(8, ShapeAnalysis.Circumference(bi),
            "all pixels except the centre are edge pixels of the 3x3 block");
    }
}
