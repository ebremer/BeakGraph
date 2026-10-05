package com.ebremer.halcyon.hilbert;

import com.ebremer.beakgraph.utils.ImageTools;
import com.ebremer.halcyon.geometry.Point;
import java.awt.Polygon;
import java.util.ArrayList;
import java.util.Iterator;
import org.davidmoten.hilbert.HilbertCurve;
import org.davidmoten.hilbert.Range;
import org.davidmoten.hilbert.Ranges;
import org.davidmoten.hilbert.SmallHilbertCurve;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hilbert-curve helpers. The store itself uses only {@link #hc},
 * {@link #MAX_COORD}, {@link #clampToDomain}, {@link #blocks} and
 * {@link #fromWkt} (spatial index writer and query). The range-membership,
 * bbox-corner and boundary-polygon helpers below have no caller in this
 * repository; they are kept as the API the Halcyon viewer consumes, made
 * consistent with the stored cover (floor snapping, domain clamping) and
 * bounded ({@link #MAX_WALK_CELLS}) rather than removed.
 * <p>
 * Trimmed to those members; the legacy viewer rendering and rasterization
 * utilities that accumulated here (image generation, JSON export,
 * alternative Polygon2Hilbert variants, range merging) were unused - several
 * demonstrably broken - and were removed in the dead-code sweep.
 *
 * @author erich
 */
public final class HilbertSpace {
    private static final Logger logger = LoggerFactory.getLogger(HilbertSpace.class);
    public static final SmallHilbertCurve hc = HilbertCurve.small().bits(31).dimensions(2);

    /** Largest coordinate the 31-bit curve can address. */
    public static final long MAX_COORD = (1L << 31) - 1;

    public static final byte N = 0;
    public static final byte NE = 1;
    public static final byte E = 2;
    public static final byte SE = 3;
    public static final byte S = 4;
    public static final byte SW = 5;
    public static final byte W = 6;
    public static final byte NW = 7;

    private HilbertSpace() {}

    /**
     * Clamps a coordinate into the curve's domain [0, 2^31). The davidmoten
     * curve silently MASKS coordinates to their low 31 bits, so out-of-domain
     * values alias onto unrelated cells - written and queried covers then
     * disagree and matches are silently lost. Clamping is a monotone
     * projection: applied identically on the write and query sides, two
     * overlapping bboxes still overlap after it, so recall is preserved
     * (out-of-domain geometry indexes coarsely at the domain-edge cells and
     * the exact JTS verification removes the false positives).
     */
    public static long clampToDomain(long v) {
        return Math.max(0, Math.min(MAX_COORD, v));
    }

    private static final SmallHilbertCurve[] BLOCK_CURVES = new SmallHilbertCurve[32];

    /**
     * The curve over the 2^k-aligned blocks of {@link #hc}. The Hilbert curve
     * is self-similar: block (x &gt;&gt; k, y &gt;&gt; k) on this (31-k)-bit curve
     * has index {@code hc.index(x, y) >> 2k}, so a block with coarse index
     * {@code i} covers exactly the {@link #hc} indices
     * {@code [i << 2k, ((i + 1) << 2k) - 1]}. Covering a large box on the
     * block curve therefore yields an exact superset of its cell cover at a
     * cost bounded by the block grid rather than the cell grid.
     */
    public static SmallHilbertCurve blocks(int k) {
        if (k < 0 || k > 30) {
            throw new IllegalArgumentException("block shift out of range: " + k);
        }
        if (k == 0) {
            return hc;
        }
        SmallHilbertCurve c = BLOCK_CURVES[k];
        if (c == null) {
            c = HilbertCurve.small().bits(31 - k).dimensions(2);
            BLOCK_CURVES[k] = c; // idempotent; a racing double build is harmless
        }
        return c;
    }

    public static boolean inRange(ArrayList<Range> rr, Point p, Byte neighbor) {
        // Return the membership result - the old switch *statement* computed
        // contains(...) and discarded it, so this always answered false and the
        // getPolygon boundary walk never advanced.
        return switch (neighbor) {
            case N -> contains(rr,p.x,p.y-1);
            case NE -> contains(rr,p.x+1,p.y-1);
            case E -> contains(rr,p.x+1,p.y);
            case SE -> contains(rr,p.x+1,p.y+1);
            case S -> contains(rr,p.x,p.y+1);
            case SW -> contains(rr,p.x-1,p.y+1);
            case W -> contains(rr,p.x-1,p.y);
            case NW -> contains(rr,p.x-1,p.y-1);
            default -> false;
        };
    }

    public static boolean inRange(Ranges rr, Point p, Byte neighbor) {
        return switch (neighbor) {
            case N -> contains(rr,p.x,p.y-1);
            case NE -> contains(rr,p.x+1,p.y-1);
            case E -> contains(rr,p.x+1,p.y);
            case SE -> contains(rr,p.x+1,p.y+1);
            case S -> contains(rr,p.x,p.y+1);
            case SW -> contains(rr,p.x-1,p.y+1);
            case W -> contains(rr,p.x-1,p.y);
            case NW -> contains(rr,p.x-1,p.y-1);
            default -> false;
        };
    }

    public static Polygon getSkinnyPoint(int a, int b) {
        int[] x = new int[4];
        int[] y = new int[4];
        x[0] = a;
        y[0] = b;
        x[1] = a;
        y[1] = b;
        x[2] = a;
        y[2] = b;
        x[3] = a;
        y[3] = b;
        return new Polygon(x,y,4);
    }

    public static Polygon getSkinnyPoint(long p) {
        long[] c = hc.point(p);
        return getSkinnyPoint((int) c[0], (int) c[1]);
    }

    /** The four corner cells of the polygon's bbox at scale 0; see {@link #getBoundingBoxHilbertIndices(org.locationtech.jts.geom.Polygon, int)}. */
    public static long[] getBoundingBoxHilbertIndices(org.locationtech.jts.geom.Polygon polygon) {
        return getBoundingBoxHilbertIndices(polygon, 0);
    }

    /**
     * Hilbert indices of the bbox's corner cells {bottom-left, top-left,
     * top-right, bottom-right} at {@code scale} (cells of 2^scale units), using
     * the SAME floor snapping and domain clamping as the stored cell cover
     * (PositionalDictionaryWriterBuilder / SpatialIndexIterator). The former
     * round-and-cast version fed unclamped coordinates to the curve, which
     * bit-masks negative values onto unrelated cells, and could differ from the
     * stored corners by one cell.
     */
    public static long[] getBoundingBoxHilbertIndices(org.locationtech.jts.geom.Polygon polygon, int scale) {
        if (polygon == null || polygon.isEmpty()) {
            throw new IllegalArgumentException("Polygon cannot be null or empty");
        }
        if (scale < 0 || scale > 30) {
            throw new IllegalArgumentException("scale out of range: " + scale);
        }
        org.locationtech.jts.geom.Envelope env = polygon.getEnvelopeInternal();
        long cell = 1L << scale;
        long minX = Math.floorDiv(clampToDomain((long) Math.floor(env.getMinX())), cell);
        long maxX = Math.floorDiv(clampToDomain((long) Math.floor(env.getMaxX())), cell);
        long minY = Math.floorDiv(clampToDomain((long) Math.floor(env.getMinY())), cell);
        long maxY = Math.floorDiv(clampToDomain((long) Math.floor(env.getMaxY())), cell);
        long bl = hc.index(new long[] {minX, minY});
        long tl = hc.index(new long[] {minX, maxY});
        long tr = hc.index(new long[] {maxX, maxY});
        long br = hc.index(new long[] {maxX, minY});
        return new long[] {bl, tl, tr, br};
    }

    public static Point NextPoint(Point p, Byte neighbor) {
        switch (neighbor) {
            case N -> { return new Point(p.x,p.y-1); }
            case NE -> { return new Point(p.x+1,p.y-1); }
            case E -> { return new Point(p.x+1,p.y); }
            case SE -> { return new Point(p.x+1,p.y+1); }
            case S -> { return new Point(p.x,p.y+1); }
            case SW -> { return new Point(p.x-1,p.y+1); }
            case W -> { return new Point(p.x-1,p.y); }
            case NW -> { return new Point(p.x-1,p.y-1); }
        }
        return null;
    }

    public static boolean contains(ArrayList<Range> rr, int x, int y) {
        // A neighbour off the domain edge (x or y = -1 next to the axis) is in
        // no cover; the curve would otherwise bit-mask it onto the far side.
        if (x < 0 || y < 0 || x > MAX_COORD || y > MAX_COORD) {
            return false;
        }
        long target = hc.index(new long[] {x,y});
        for (Range r : rr) {
            if ((target>=r.low())&&(target<=r.high())) {
                return true;
            }
        }
        return false;
    }

    public static boolean contains(Ranges rr, int x, int y) {
        if (x < 0 || y < 0 || x > MAX_COORD || y > MAX_COORD) {
            return false;
        }
        long target = hc.index(new long[] {x,y});
        for (Range r : rr) {
            if ((target>=r.low())&&(target<=r.high())) {
                return true;
            }
        }
        return false;
    }

    /** Most cells the boundary-walk helpers will enumerate; tunable via beakgraph.hilbert.maxWalkCells. */
    public static final long MAX_WALK_CELLS = Long.getLong("beakgraph.hilbert.maxWalkCells", 1L << 24);

    /**
     * Guards the cell-enumerating viewer helpers: they visit every index of
     * every range, and one range over a modest bbox on the 31-bit curve can
     * span billions of indices. Beyond the budget the call is refused rather
     * than left to run for hours.
     */
    private static void checkWalkBudget(Ranges rr) {
        long cells = 0;
        for (Range r : rr) {
            cells += r.high() - r.low() + 1;
            if (cells > MAX_WALK_CELLS) {
                throw new IllegalArgumentException("Hilbert ranges cover more than " + MAX_WALK_CELLS
                        + " cells; the boundary walk is meant for tile-sized covers (raise beakgraph.hilbert.maxWalkCells to override)");
            }
        }
    }

    public static Point GetUpperLeft(Ranges rr) {
        checkWalkBudget(rr);
        Iterator<Range> i = rr.iterator();
        Point top = new Point(Integer.MAX_VALUE,Integer.MAX_VALUE);
        while (i.hasNext()) {
            Range r = i.next();
            for (long c = r.low(); c<=r.high();c++) {
                long[] p = hc.point(c);
                if (p[1]<top.y) {
                    top.x = (int) p[0];
                    top.y = (int) p[1];
                } else if (p[1]==top.y) {
                    if (p[0]<top.x) {
                        top.x = (int) p[0];
                        top.y = (int) p[1];
                    }
                }
            }
        }
        return top;
    }

    public static Polygon getPolygon(Ranges rr) {
        if (rr.size()==1) {
            Iterator<Range> ri = rr.iterator();
            Range r = ri.next();
            if ((r.high()-r.low())==0) {
                return getSkinnyPoint(r.low());
            }
        }
        Ranges big = rr;
        Point sp = GetUpperLeft(big);
        Polygon p = new Polygon();
        Point cp = sp.clone();
        Visits v = new Visits();
        p.addPoint(sp.x, sp.y);
        v.visited(cp);
        Point lp = sp.clone();
        byte cd = -1;
        byte ld;
        do {
            boolean jumped;
            do {
                jumped = true;
            if        (inRange(big, cp, N) &&!inRange(big,cp, NW) && (v.getNumVisits(cp)>v.getNumVisits(NextPoint(cp, N)))) {
                ld = cd; lp.x = cp.x; lp.y = cp.y;
                cp.y--; cd = N;
            } else if (inRange(big, cp, NE)&&!inRange(big, cp, N) && (v.getNumVisits(cp)>v.getNumVisits(NextPoint(cp, NE)))) {
                ld = cd; lp.x = cp.x; lp.y = cp.y;
                cp.x++; cp.y--; cd = NE;
            } else if (inRange(big, cp, E) &&!inRange(big, cp, NE)&& (v.getNumVisits(cp)>v.getNumVisits(NextPoint(cp, E)))) {
                ld = cd; lp.x = cp.x; lp.y = cp.y;
                cp.x++; cd = E;
            } else if (inRange(big, cp, SE)&&!inRange(big, cp, E) && (v.getNumVisits(cp)>v.getNumVisits(NextPoint(cp, SE)))) {
                ld = cd; lp.x = cp.x; lp.y = cp.y;
                cp.x++; cp.y++; cd = SE;
            } else if (inRange(big, cp, S) &&!inRange(big, cp, SE)&& (v.getNumVisits(cp)>v.getNumVisits(NextPoint(cp, S)))) {
                ld = cd; lp.x = cp.x; lp.y = cp.y;
                        cp.y++; cd = S;
            } else if (inRange(big, cp, SW)&&!inRange(big, cp, S) && (v.getNumVisits(cp)>v.getNumVisits(NextPoint(cp, SW)))) {
                ld = cd; lp.x = cp.x; lp.y = cp.y;
                cp.x--; cp.y++; cd = SW;
            } else if (inRange(big, cp, W) &&!inRange(big, cp, SW)&& (v.getNumVisits(cp)>v.getNumVisits(NextPoint(cp, W)))) {
                ld = cd; lp.x = cp.x; lp.y = cp.y;
                cp.x--; cd = W;
            } else if (inRange(big, cp, NW)&&!inRange(big, cp, W) && (v.getNumVisits(cp)>v.getNumVisits(NextPoint(cp, NW)))) {
                ld = cd; lp.x = cp.x; lp.y = cp.y;
                cp.x--; cp.y--; cd = NW;
            } else {
                if (v.getNumVisits(cp)>10) {
                    logger.warn("Absurd issue failout at {}", cp);
                    return getSkinnyPoint(cp.x,cp.y);
                }
                jumped = false;
                ld = -1;
            }
            v.visited(cp);
            } while (!jumped);
            if ((cd>=0)&&(cd!=ld)) {
                p.addPoint(cp.x, cp.y);
            }
        } while (!Point.isSamePoint(sp, cp));
        p.addPoint(cp.x, cp.y);
        return p;
    }

    public static org.locationtech.jts.geom.Polygon fromWkt(String wkt) {
        WKTReader reader = new WKTReader();
        Geometry geom;
        // GeoSPARQL wktLiterals may carry a "<crs-uri> WKT" prefix; JTS does not
        // accept it. Failures raise a catchable exception (with the offending
        // input), never a bare java.lang.Error.
        String clean = ImageTools.stripCrs(wkt);
        try {
            geom = reader.read(clean);
            if (!(geom instanceof org.locationtech.jts.geom.Polygon)) {
                logger.error("WKT is not a Polygon: {}", geom.getGeometryType());
                throw new IllegalArgumentException("WKT is not a Polygon: " + geom.getGeometryType());
            }
            return (org.locationtech.jts.geom.Polygon) geom;
        } catch (ParseException ex) {
            throw new IllegalArgumentException("Invalid WKT: "
                    + (clean.length() > 100 ? clean.substring(0, 100) + "..." : clean), ex);
        }
    }
}
