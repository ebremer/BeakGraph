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

    public static void main(String[] args) {
        // Wave Parameters
        double amplitude = 20000.0;
        double width = 100000.0;
        double cycles = 5.0;
        int smoothness = 1000;

        // Shift Parameters
        double offX = 20000.0;  // Shift right
        double offY = 20000.0; // Shift up

        // 1. Generate the Sine Wave with Offset
        LineString sineWave = createSinePolyline(amplitude, width, cycles, smoothness, offX, offY);
        
        // 2. Apply the buffer
        double bufferDistance = 2.0;
        var envelope = sineWave.buffer(bufferDistance);

        System.out.println("Sine Wave WKT: " + sineWave.toText());
        // System.out.println("Envelope WKT: " + envelope.toText()); 
    }
}