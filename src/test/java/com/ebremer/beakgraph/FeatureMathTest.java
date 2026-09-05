package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ebremer.beakgraph.features.ShapeAnalysis;
import java.awt.image.BufferedImage;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for feature-math defects:
 * <ul>
 *   <li>ShapeAnalysis.isEdge read the four neighbours without bounds checks, so
 *       a filled pixel on the image border threw ArrayIndexOutOfBoundsException
 *       out of Circumference.</li>
 * </ul>
 */
class FeatureMathTest {

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
