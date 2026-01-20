package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.halcyon.hilbert.HilbertSpace;
import static com.ebremer.halcyon.hilbert.HilbertSpace.hc;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.davidmoten.hilbert.Range;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.Coordinate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HilbertPolygon {
    private static final Logger logger = LoggerFactory.getLogger(HilbertPolygon.class);
    private static final GeometryFactory gf = new GeometryFactory();

    public static ArrayList<Range> Polygon2Hilbert(String wkt) {
        return Polygon2Hilbert(HilbertSpace.fromWkt(wkt), 0);
    }

    public static ArrayList<Range> Polygon2Hilbert(String wkt, int scale) {
        return Polygon2Hilbert(HilbertSpace.fromWkt(wkt), scale);
    }

    public static ArrayList<Range> Polygon2Hilbert(Polygon poly) {
        return Polygon2Hilbert(poly, 0);
    }

    /**
     * Converts a JTS Polygon to a list of Hilbert Ranges.
     * @param poly
     * @param scale
     * @return 
     */
    public static ArrayList<Range> Polygon2Hilbert(Polygon poly, int scale) {
        Envelope env = poly.getEnvelopeInternal();

        // 1. Calculate scaled integer bounds
        long minX = (long) Math.floor(env.getMinX()) >> scale;
        long maxX = (long) Math.floor(env.getMaxX()) >> scale;
        long minY = (long) Math.floor(env.getMinY()) >> scale;
        long maxY = (long) Math.floor(env.getMaxY()) >> scale;

        // 2. Execute query. We use .stream() to get the ranges.
        // This is the most common API for davidmoten's hilbert-curve query builder.
        List<Range> candidateRanges = hc.query(new long[]{minX, minY}, new long[]{maxX, maxY})
                                       .stream()
                                       .collect(Collectors.toList());

        ArrayList<Range> filteredRanges = new ArrayList<>();
        for (Range r : candidateRanges) {
            if (isRangeRelevant(r, poly, scale)) {
                filteredRanges.add(r);
            }
        }

        // 3. Compact the results
        return compact(filteredRanges);
    }

    private static boolean isRangeRelevant(Range range, Polygon poly, int scale) {
        // Check the start point of the range
        long[] startCoord = hc.point(range.low());
        if (poly.covers(gf.createPoint(new Coordinate(startCoord[0] << scale, startCoord[1] << scale)))) {
            return true;
        }

        // Check the end point of the range
        long[] endCoord = hc.point(range.high());
        return poly.covers(gf.createPoint(new Coordinate(endCoord[0] << scale, endCoord[1] << scale)));
    }

    /**
     * Merges adjacent ranges to minimize the number of ranges.
     */
    private static ArrayList<Range> compact(List<Range> ranges) {
        if (ranges == null || ranges.isEmpty()) return new ArrayList<>();
        
        // Sorting isn't strictly necessary if the stream is ordered, but safe to do
        ranges.sort((a, b) -> Long.compare(a.low(), b.low()));
        
        ArrayList<Range> result = new ArrayList<>();
        Range last = null;

        for (Range current : ranges) {
            if (last == null) {
                last = current;
            } else if (current.low() <= last.high() + 1) {
                // Merge overlapping or adjacent ranges
                last = new Range(last.low(), Math.max(last.high(), current.high()));
            } else {
                result.add(last);
                last = current;
            }
        }
        if (last != null) result.add(last);
        return result;
    }
}
