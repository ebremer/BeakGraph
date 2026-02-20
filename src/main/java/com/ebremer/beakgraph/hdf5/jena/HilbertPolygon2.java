package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.halcyon.hilbert.HilbertSpace;
import static com.ebremer.halcyon.hilbert.HilbertSpace.hc;
import org.davidmoten.hilbert.Range;
import org.locationtech.jts.geom.*;
import java.util.ArrayList;

public class HilbertPolygon2 {
    private static final GeometryFactory gf = new GeometryFactory();

    public static ArrayList<Range> Polygon2Hilbert(String wkt) {
        return Polygon2Hilbert(HilbertSpace.fromWkt(wkt), 0);
    }

    public static ArrayList<Range> Polygon2Hilbert(String wkt, int scale) {
        return Polygon2Hilbert(HilbertSpace.fromWkt(wkt), scale);
    }

    public static ArrayList<Range> Polygon2Hilbert(Polygon poly, int scale) {
        // Automatically determine the necessary bit depth based on the polygon's location
        // Defaulting to 24 bits covers roughly 16 million units (plenty for WSI/Geo data)
        int maxBits = 24; 
        
        ArrayList<Range> ranges = new ArrayList<>();
        
        // Optimization: Get the bounding box of the polygon
        //Envelope env = poly.getEnvelopeInternal();
        
        // Find the smallest power-of-2 square that contains the bounding box
        // to minimize the starting depth of the recursion.
        fold(poly, 0, 0, maxBits, scale, ranges);
        
        return mergeRanges(ranges);
    }

    private static void fold(Polygon poly, long x, long y, int currentBits, int targetScale, ArrayList<Range> ranges) {
        long side = 1L << currentBits;
        Envelope cellEnv = new Envelope(x, x + side, y, y + side);
        
        // Check 1: Quick bounding box intersection (Fast)
        if (!poly.getEnvelopeInternal().intersects(cellEnv)) {
            return;
        }
        
        // Check 2: Actual geometry intersection (More expensive)
        Geometry cellRect = gf.toGeometry(cellEnv);
        if (!poly.intersects(cellRect)) {
            return;
        }

        // Base Case
        if (currentBits <= targetScale) {
            long idx = hc.index(x >> targetScale, y >> targetScale);
            ranges.add(new Range(idx, idx));
            return;
        }

        int nextBits = currentBits - 1;
        long half = 1L << nextBits;

        // Sub-quadrant coordinates
        long[][] coords = {
            {x, y}, {x, y + half}, {x + half, y}, {x + half, y + half}
        };

        // Sort quadrants by Hilbert index to ensure 'ranges' is populated in order
        java.util.Arrays.sort(coords, (a, b) -> {
            long idxA = hc.index(a[0] >> targetScale, a[1] >> targetScale);
            long idxB = hc.index(b[0] >> targetScale, b[1] >> targetScale);
            return Long.compare(idxA, idxB);
        });

        for (long[] child : coords) {
            fold(poly, child[0], child[1], nextBits, targetScale, ranges);
        }
    }

    private static ArrayList<Range> mergeRanges(ArrayList<Range> sortedRanges) {
        if (sortedRanges.isEmpty()) return sortedRanges;
        ArrayList<Range> merged = new ArrayList<>();
        Range current = sortedRanges.get(0);
        for (int i = 1; i < sortedRanges.size(); i++) {
            Range next = sortedRanges.get(i);
            if (next.low() <= current.high() + 1) {
                current = new Range(current.low(), Math.max(current.high(), next.high()));
            } else {
                merged.add(current);
                current = next;
            }
        }
        merged.add(current);
        return merged;
    }
}
