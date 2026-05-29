package com.ebremer.beakgraph.turbo;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;

public class SineWaveGenerator {

    private static final GeometryFactory geomFactory = new GeometryFactory();

    /**
     * Generates a JTS LineString representing a Sine Wave with an offset.
     *
     * @param A         Amplitude (Height from center to peak). Total vertical span is 2*A.
     * @param W         Total Width (length along X axis).
     * @param n         Number of full cycles to fit within W.
     * @param numPoints Resolution: How many points to use to draw the wave.
     * @param offX      X offset (shifts the start of the wave horizontally).
     * @param offY      Y offset (shifts the center of the wave vertically).
     * @return          A JTS LineString.
     */
    public static LineString createSinePolyline(double A, double W, double n, int numPoints, double offX, double offY) {
        Coordinate[] coords = new Coordinate[numPoints + 1];
        
        // Calculate the angular frequency
        double k = (2 * Math.PI * n) / W;

        for (int i = 0; i <= numPoints; i++) {
            // 1. Calculate the local x (0 to W) used for the shape math
            double xLocal = (double) i / numPoints * W;
            
            // 2. Calculate the local y based on the sine function
            double yLocal = A * Math.sin(k * xLocal);
            
            // 3. Apply the offsets to get the final world coordinates
            coords[i] = new Coordinate(xLocal + offX, yLocal + offY);
        }

        return geomFactory.createLineString(coords);
    }
}