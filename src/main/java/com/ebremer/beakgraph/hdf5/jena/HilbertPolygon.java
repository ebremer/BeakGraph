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

        // 1. Calculate scaled integer bounds, clamped into the curve's domain
        // (negative or >= 2^31 coordinates would otherwise alias via bit masking).
        long minX = HilbertSpace.clampToDomain((long) Math.floor(env.getMinX())) >> scale;
        long maxX = HilbertSpace.clampToDomain((long) Math.floor(env.getMaxX())) >> scale;
        long minY = HilbertSpace.clampToDomain((long) Math.floor(env.getMinY())) >> scale;
        long maxY = HilbertSpace.clampToDomain((long) Math.floor(env.getMaxY())) >> scale;

        // 2. Execute query. We use .stream() to get the ranges.
        // This is the most common API for davidmoten's hilbert-curve query builder.
        List<Range> candidateRanges = hc.query(new long[]{minX, minY}, new long[]{maxX, maxY})
                                       .stream()
                                       .collect(Collectors.toList());

        // When clamping moved an edge, the candidate cells lie at the domain
        // border while the polygon lies outside it: the per-cell relevance test
        // (the unclamped polygon against each clamped cell) rejected every cell
        // and returned an EMPTY cover for a geometry the writers index at exactly
        // those border cells (BG-72). Keep the whole clamped cover instead - the
        // same superset the index holds; the sfIntersects verification removes
        // the false positives.
        double cellSize = (double) (1L << scale);
        Envelope clamped = new Envelope((double) (minX << scale), (double) (maxX << scale) + cellSize,
                                        (double) (minY << scale), (double) (maxY << scale) + cellSize);
        boolean clampingMovedAnEdge = Math.floor(env.getMinX()) < 0 || Math.floor(env.getMinY()) < 0
                || Math.floor(env.getMaxX()) > HilbertSpace.clampToDomain(Long.MAX_VALUE)
                || Math.floor(env.getMaxY()) > HilbertSpace.clampToDomain(Long.MAX_VALUE);
        if (clampingMovedAnEdge && !clamped.intersects(env)) {
            return compact(candidateRanges);
        }

        ArrayList<Range> filteredRanges = new ArrayList<>();
        for (Range r : candidateRanges) {
            if (isRangeRelevant(r, poly, scale)) {
                filteredRanges.add(r);
            }
        }

        // 3. Compact the results
        return compact(filteredRanges);
    }

    /** Cap on per-range cell tests; longer ranges are kept outright (superset-safe). */
    private static final int MAX_RELEVANCE_CELLS = 64;

    /**
     * Whether any WHOLE CELL of the range intersects the polygon. The old test
     * asked whether the polygon covers the range's two endpoint cell-corner
     * POINTS, which dropped ranges the geometry genuinely intersects - any
     * polygon smaller than a cell returned an EMPTY cover, and diagonal strips
     * lost their interior cells: silent false negatives for every consumer of
     * this Polygon-to-Hilbert API. Keeping a range can only add false
     * positives, never lose a match, so ranges longer than the cap are kept
     * without examination.
     */
    private static boolean isRangeRelevant(Range range, Polygon poly, int scale) {
        if (range.high() - range.low() + 1 > MAX_RELEVANCE_CELLS) {
            return true;
        }
        double cellSize = (double) (1L << scale);
        for (long d = range.low(); d <= range.high(); d++) {
            long[] p = hc.point(d);
            double x = (double) (p[0] << scale);
            double y = (double) (p[1] << scale);
            if (poly.intersects(gf.toGeometry(new Envelope(x, x + cellSize, y, y + cellSize)))) {
                return true;
            }
        }
        return false;
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
