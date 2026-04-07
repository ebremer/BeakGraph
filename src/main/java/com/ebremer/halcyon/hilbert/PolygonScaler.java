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
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PolygonScaler {
    private static final Logger logger = LoggerFactory.getLogger(PolygonScaler.class);
    private static final GeometryFactory gf = new GeometryFactory();
    private static final AffineTransformation half = AffineTransformation.scaleInstance(0.5, 0.5);

    /**
     * Parses WKT and generates a sequence of progressively quarter-area scaled polygons.
     * @param wkt The Well-Known Text string.
     * @return Array of Polygons.
     */
    public static Polygon[] toPolygons(String wkt) {
        Polygon original;
        try {
            original = ImageTools.wktToPolygon(wkt);
            original.apply(new IntSnapFilter());
            original = snapAndSimplify(original);
        } catch (Exception ex) {
            logger.error("Failed to parse input WKT as Polygon {} {}", wkt, ex.getMessage());
            original = gf.createPolygon();
        }
        return toPolygons(original);
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
        throw new IllegalStateException("Unable to fix invalid polygon: " + polygon);
    }

    /**
     * Generates a sequence of progressively quarter-area scaled polygons.
     * (Geometry itself is scaled down).
     * @param original The original high-res polygon.
     * @return Array of Polygons (Levels).
     */
    public static Polygon[] toPolygons(Polygon original) {
        if (original == null) {
            return null;
        }
        List<Polygon> scaledPolygons = new ArrayList<>();
        Polygon current = fixPolygon(original);
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
     * Logic:
     * Scale 0: Tile covers 256 coordinates.
     * Scale 1: Tile covers 512 coordinates (1/2 resolution).
     * Scale 2: Tile covers 1024 coordinates (1/4 resolution).
     * Formula: EffectiveSize = 256 * 2^scale
     * 
     * @param cells List to accumulate intersecting grid cells
     * @param poly The input JTS Polygon (in base scale 0 coordinates)
     * @param scale The pyramid scale level (0, 1, 2, 3...)
     * @return List of GridCell indices at the requested scale
     */
    public static List<GridCell> getGridCells(List<GridCell> cells, Polygon poly, short scale) {
        if (poly == null || poly.isEmpty() || scale < 0) {
            return cells;
        }
        
        double effectiveTileSize = GRIDTILESIZE * (1 << scale);
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
        Coordinate[] coords = poly.getExteriorRing().getCoordinates();
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
        try {
            LinearRing shell = gf.createLinearRing(cleaned.toArray(new Coordinate[0]));
            return gf.createPolygon(shell);
        } catch (Exception e) {
            return null;
        }
    }
}
