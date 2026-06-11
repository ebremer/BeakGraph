package com.ebremer.halcyon.hilbert;

import com.ebremer.beakgraph.utils.ImageTools;
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.locationtech.jts.io.WKTWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.locationtech.jts.operation.polygonize.Polygonizer;
import static com.ebremer.beakgraph.Params.GRIDTILESIZE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PolygonScaler {
    private static final Logger logger = LoggerFactory.getLogger(PolygonScaler.class);
    private static final GeometryFactory gf = new GeometryFactory();
    private static final AffineTransformation half = AffineTransformation.scaleInstance(0.5, 0.5);

    /**
     * Parses WKT and generates a sequence of progressively quarter-area scaled
     * polygons. Returns null (skip this geometry) for any input that cannot be
     * turned into a usable polygon - unparseable WKT, a geometry that collapses
     * under integer snapping, or one the fixer cannot repair. The policy is
     * uniform: one bad geometry is skipped with a warning, never silently and
     * never by aborting the (possibly multi-hour) build that contains it.
     * @param wkt The Well-Known Text string.
     * @return Array of Polygons, or null to skip.
     */
    public static Polygon[] toPolygons(String wkt) {
        Polygon original;
        try {
            original = ImageTools.wktToPolygon(wkt);
            if (original == null) {
                logger.warn("No polygon in WKT; skipping spatial indexing: {}", abbrev(wkt));
                return null;
            }
            original.apply(new IntSnapFilter());
            original = snapAndSimplify(original);
        } catch (Exception ex) {
            logger.warn("Failed to parse WKT as Polygon; skipping spatial indexing: {} ({})",
                    abbrev(wkt), ex.getMessage());
            return null;
        }
        if (original == null) {
            logger.warn("Polygon collapsed during integer snapping; skipping spatial indexing: {}",
                    abbrev(wkt));
            return null;
        }
        return toPolygons(original);
    }

    private static String abbrev(String wkt) {
        return (wkt != null && wkt.length() > 200) ? wkt.substring(0, 200) + "..." : wkt;
    }

    public static Polygon fixPolygon(Polygon polygon) {
        // Check if already valid
        if (polygon.isValid()) {
            return polygon;
        }
        // First try simple buffer(0) trick
        Geometry fixed = polygon.buffer(0);
        if (fixed instanceof Polygon && fixed.isValid()) {
            return (Polygon) fixed;
        }
        // If result is MultiPolygon, pick largest polygon
        if (fixed instanceof MultiPolygon mp) {
            Polygon largest = null;
            double maxArea = 0;
            for (int i = 0; i < mp.getNumGeometries(); i++) {
                Polygon p = (Polygon) mp.getGeometryN(i);
                if (p.getArea() > maxArea) {
                    maxArea = p.getArea();
                    largest = p;
                }
            }
            if (largest != null && largest.isValid()) {
                return largest;
            }
        }
        // Last resort: use Polygonizer to reconstruct from edges
        Polygonizer polygonizer = new Polygonizer();
        polygonizer.add(polygon); // adds lines of polygon
        @SuppressWarnings("unchecked")
        Collection<Polygon> polys = polygonizer.getPolygons();
        if (!polys.isEmpty()) {
            // pick largest polygon
            Polygon largest = null;
            double maxArea = 0;
            for (Polygon p : polys) {
                if (p.getArea() > maxArea) {
                    maxArea = p.getArea();
                    largest = p;
                }
            }
            if (largest != null && largest.isValid()) {
                return largest;
            }
        }
        // Describe the polygon by size and extent - embedding the full WKT of a
        // possibly-100k-vertex polygon in an exception message helps nobody.
        throw new IllegalStateException("Unable to fix invalid polygon ("
                + polygon.getNumPoints() + " points, envelope " + polygon.getEnvelopeInternal() + ")");
    }

    /**
     * Generates a sequence of progressively quarter-area scaled polygons.
     * (Geometry itself is scaled down).
     * @param original The original high-res polygon.
     * @return Array of Polygons (Levels).
     */
    public static Polygon[] toPolygons(Polygon original) {
        if (original == null || original.isEmpty()) {
            return null;
        }
        // Snap/clean here (not only in the String entry) so callers passing raw
        // JTS parts - e.g. individual MULTIPOLYGON members - get the same
        // integer-grid treatment.
        original.apply(new IntSnapFilter());
        original = snapAndSimplify(original);
        if (original == null) {
            logger.warn("Polygon collapsed during integer snapping; skipping spatial indexing");
            return null;
        }
        List<Polygon> scaledPolygons = new ArrayList<>();
        Polygon current;
        try {
            current = fixPolygon(original);
        } catch (RuntimeException ex) {
            // One unfixable geometry must not abort the whole build: skip its
            // spatial indexing - the source quad itself is still stored.
            logger.warn("Skipping spatial indexing of unfixable polygon: {}", ex.getMessage());
            return null;
        }
        if (current == null || !current.isValid()) {
            return new Polygon[0];
        }
        int maxIterations = 20; // Safety limit to prevent infinite loops
        int iterations = 0;        
        while (iterations < maxIterations) {
            scaledPolygons.add(current);
            // Scale geometry down by 0.5 (area becomes 0.25)
            Geometry scaled = half.transform(current);            
            // Verify the result is still a Polygon
            if (!(scaled instanceof Polygon)) {
                break;
            }            
            scaled.apply(new IntSnapFilter());            
            current = snapAndSimplify((Polygon) scaled);            
            if (current == null || current.getNumPoints() < 4 || !current.isValid()) {
                break;
            }
            double area = current.getArea();                          
            // Stop if polygon is too small
            if (area < 4.0) {
                break;
            }
            iterations++;
        }        
        return scaledPolygons.toArray(new Polygon[0]);
    }

    /**
     * Converts an array of JTS Polygons into an array of WKT Strings.
     * @param polygons Array of polygons to convert
     * @return Array of WKT strings
     */
    public static String[] toWKT(Polygon[] polygons) {
        if (polygons == null) {
            return new String[0];
        }
        WKTWriter wktWriter = new WKTWriter();
        String[] wktStrings = new String[polygons.length];        
        for (int i = 0; i < polygons.length; i++) {
            if (polygons[i] != null) {
                Polygon pp = polygons[i];
                pp.apply(new IntSnapFilter());
                wktStrings[i] = wktWriter.write(pp);
            } else {
                wktStrings[i] = "POLYGON EMPTY";
            }
        }
        return wktStrings;
    }
    
    /**
     * Gets grid cells for a polygon across multiple scale levels using the scaled polygon pyramid.
     * @param poly The input polygon
     * @param numscales Number of scale levels to process
     * @return List of grid cells across all scales
     */
    public static List<GridCell> getGridCells(Polygon poly, int numscales) {
        List<GridCell> list = new ArrayList<>();
        Polygon[] scaledPolygons = toPolygons(poly);
        if (scaledPolygons == null) {
            return list;
        }
        for (short s = 0; s < Math.min(numscales, scaledPolygons.length); s++) {
            getGridCells(list, scaledPolygons[s], s);
        }
        return list;
    }

    /**
     * Determines which grid cells the polygon intersects at a specific resolution scale.
     * <p>
     * The polygon is expected in the coordinates of ITS OWN scale level (already
     * divided by 2^scale, as produced by {@link #toPolygons}), and every level's
     * tiles are {@code GRIDTILESIZE} units on a side in those level coordinates -
     * exactly the grid the writer persists (see generateGridURNs). The old
     * formula multiplied the tile size by 2^scale ON TOP of the already-scaled
     * coordinates, so for scale &gt;= 1 the computed cells disagreed with every
     * stored tile graph URN.
     *
     * @param cells List to accumulate intersecting grid cells
     * @param poly The input JTS Polygon, in scale-level coordinates
     * @param scale The pyramid scale level (0, 1, 2, 3...)
     * @return List of GridCell indices at the requested scale
     */
    public static List<GridCell> getGridCells(List<GridCell> cells, Polygon poly, short scale) {
        if (poly == null || poly.isEmpty() || scale < 0) {
            return cells;
        }

        double effectiveTileSize = GRIDTILESIZE;
        Envelope env = poly.getEnvelopeInternal();
        
        int minGridX = (int) Math.floor(env.getMinX() / effectiveTileSize);
        int maxGridX = (int) Math.floor(env.getMaxX() / effectiveTileSize);
        int minGridY = (int) Math.floor(env.getMinY() / effectiveTileSize);
        int maxGridY = (int) Math.floor(env.getMaxY() / effectiveTileSize);
        
        for (int x = minGridX; x <= maxGridX; x++) {
            for (int y = minGridY; y <= maxGridY; y++) {
                double tileMinX = x * effectiveTileSize;
                double tileMaxX = (x + 1) * effectiveTileSize;
                double tileMinY = y * effectiveTileSize;
                double tileMaxY = (y + 1) * effectiveTileSize;                                
                Envelope tileEnv = new Envelope(tileMinX, tileMaxX, tileMinY, tileMaxY);
                if (poly.intersects(gf.toGeometry(tileEnv))) {
                    cells.add(new GridCell(scale, x, y));
                }
            }
        }
        return cells;
    }

    // ==========================================
    // GEOMETRY CLEANUP HELPERS
    // ==========================================

    private static Polygon snapAndSimplify(Polygon poly) {
        poly.apply(new IntSnapFilter());
        poly = removeDuplicateAndCollinearVertices(poly);
        if (poly == null || poly.getNumPoints() < 4) {
            return null;
        }
        Geometry cleaned = poly.buffer(0);
        return (cleaned instanceof Polygon) ? (Polygon) cleaned : null;
    }

    private static class IntSnapFilter implements CoordinateSequenceFilter {
        @Override
        public void filter(CoordinateSequence seq, int i) {
            seq.setOrdinate(i, 0, Math.round(seq.getOrdinate(i, 0)));
            seq.setOrdinate(i, 1, Math.round(seq.getOrdinate(i, 1)));
        }

        @Override 
        public boolean isDone() { 
            return false; 
        }
        
        @Override 
        public boolean isGeometryChanged() { 
            return true; 
        }
    }

    private static Polygon removeDuplicateAndCollinearVertices(Polygon poly) {
        // Clean every ring, not just the shell: interior rings (holes - lumens in
        // pathology annotations) must survive cleanup. A hole that collapses under
        // snapping is dropped while the outer shape survives.
        Coordinate[] shellCoords = cleanRing(poly.getExteriorRing().getCoordinates());
        if (shellCoords == null) {
            return null;
        }
        List<LinearRing> holes = new ArrayList<>();
        for (int h = 0; h < poly.getNumInteriorRing(); h++) {
            Coordinate[] ring = cleanRing(poly.getInteriorRingN(h).getCoordinates());
            if (ring != null) {
                try {
                    holes.add(gf.createLinearRing(ring));
                } catch (Exception e) {
                    // collapsed/invalid hole: drop it, keep the polygon
                }
            }
        }
        try {
            LinearRing shell = gf.createLinearRing(shellCoords);
            return gf.createPolygon(shell, holes.toArray(LinearRing[]::new));
        } catch (Exception e) {
            return null;
        }
    }

    /** De-duplicates and de-collinearizes one ring; null when it collapses (&lt;4 points). */
    private static Coordinate[] cleanRing(Coordinate[] coords) {
        List<Coordinate> cleaned = new ArrayList<>();
        // Remove duplicate consecutive vertices
        for (Coordinate coord : coords) {
            if (cleaned.isEmpty() || !coord.equals2D(cleaned.get(cleaned.size() - 1))) {
                cleaned.add(coord);
            }
        }
        // Remove collinear points
        int i = 0;
        while (i < cleaned.size() - 2) {
            Coordinate a = cleaned.get(i);
            Coordinate b = cleaned.get(i + 1);
            Coordinate c = cleaned.get(i + 2);

            // Check if b is collinear with a and c
            if (Orientation.index(a, b, c) == 0) {
                cleaned.remove(i + 1);
            } else {
                i++;
            }
        }
        // Ensure ring closure
        if (!cleaned.isEmpty() && !cleaned.get(0).equals2D(cleaned.get(cleaned.size() - 1))) {
            cleaned.add(new Coordinate(cleaned.get(0)));
        }
        if (cleaned.size() < 4) {
            return null;
        }
        return cleaned.toArray(new Coordinate[0]);
    }
}
