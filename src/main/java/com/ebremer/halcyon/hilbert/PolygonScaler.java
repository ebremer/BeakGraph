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

public class PolygonScaler {

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
        } catch (Exception ex) {
            System.err.println(String.format("Failed to parse input WKT as Polygon %s %s", wkt, ex.getMessage()));
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
        throw new Error("COULD NOT FIX ");
    }

    /**
     * Generates a sequence of progressively quarter-area scaled polygons.
     * (Geometry itself is scaled down).
     * @param original The original high-res polygon.
     * @return Array of Polygons (Levels).
     */
    public static Polygon[] toPolygons(Polygon original) {
        List<Polygon> scaledPolygons = new ArrayList<>();
        Polygon current = fixPolygon(original);
        if (current == null || !current.isValid()) {
            return new Polygon[0];
        }        
        while (true) {
            double area = current.getArea();
            scaledPolygons.add(current);   
            // Stop if polygon is too small
            if (area < 4.0) {
                break;
            }            
            // Scale geometry down by 0.5 (area becomes 0.25)
            Geometry scaled = half.transform(current);
            scaled.apply(new IntSnapFilter());
            scaled.geometryChanged();            
            current = snapAndSimplify((Polygon) scaled);            
            if (current == null || current.getNumPoints() < 4 || !current.isValid()) {
                break;
            }
        }
        return scaledPolygons.toArray(new Polygon[0]);
    }

    /**
     * Converts an array of JTS Polygons into an array of WKT Strings.
     * @param polygons
     * @return 
     */
    public static String[] toWKT(Polygon[] polygons) {
        if (polygons == null) {
            return new String[0];
        }
        WKTWriter wktWriter = new WKTWriter();
        String[] wktStrings = new String[polygons.length];        
        for (int i = 0; i < polygons.length; i++) {
            if (polygons[i] != null) {
                wktStrings[i] = wktWriter.write(polygons[i]);
            } else {
                wktStrings[i] = "POLYGON EMPTY";
            }
        }
        return wktStrings;
    }
    
    public static List<GridCell> getGridCells(Polygon poly, int numscales) {
        List<GridCell> list = new ArrayList<>();
        for (short s=0; s<numscales; s++ ) {
            getGridCells(list,poly,s);
        }
        return list;
    }

    /**
     * Determines which grid cells the polygon intersects at a specific resolution scale.
     * * Logic:
     * Scale 0: Tile covers 256 coordinates.
     * Scale 1: Tile covers 512 coordinates (1/2 resolution).
     * Scale 2: Tile covers 1024 coordinates (1/4 resolution).
     * Formula: EffectiveSize = 256 * 2^scale
     * * @param poly The input JTS Polygon (in base scale 0 coordinates)
     * @param intersectingCells
     * @param poly
     * @param scale The pyramid scale level (0, 1, 2, 3...)
     * @return List of GridCell indices {x, y} at the requested scale
     */
    public static List<GridCell> getGridCells(List<GridCell> intersectingCells, Polygon poly, short scale) {
        if (poly == null || poly.isEmpty() || scale < 0) {
            return intersectingCells;
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
                    intersectingCells.add(new GridCell(scale, x, y));
                }
            }
        }
        return intersectingCells;
    }

    // ==========================================
    // GEOMETRY CLEANUP HELPERS
    // ==========================================

    private static Polygon snapAndSimplify(Polygon poly) {
        poly.apply(new IntSnapFilter());
        poly.geometryChanged();

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

        @Override public boolean isDone() { return false; }
        @Override public boolean isGeometryChanged() { return true; }
    }

    private static Polygon removeDuplicateAndCollinearVertices(Polygon poly) {
        Coordinate[] coords = poly.getExteriorRing().getCoordinates();
        List<Coordinate> cleaned = new ArrayList<>();
        for (Coordinate coord : coords) {
            if (cleaned.isEmpty() || !coord.equals2D(cleaned.get(cleaned.size() - 1))) {
                cleaned.add(coord);
            }
        }
        // Remove collinear points
        for (int i = 0; i < cleaned.size() - 2; ) {
            Coordinate a = cleaned.get(i);
            Coordinate b = cleaned.get(i + 1);
            Coordinate c = cleaned.get(i + 2);
            if (Orientation.isCCW(new Coordinate[]{a, b, c})) {
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
    
    // ==========================================
    // MAIN / TEST
    // ==========================================
    
    public static void main(String[] args) throws Exception {       
        String wkt = "POLYGON ((84100 19091, 84099 19092, 84097 19092, 84096 19093, 84095 19093, 84095 19094, 84094 19095, 84094 19099, 84095 19100, 84095 19102, 84103 19110, 84103 19111, 84109 19117, 84111 19117, 84113 19115, 84114 19115, 84115 19114, 84116 19114, 84117 19113, 84118 19113, 84121 19110, 84121 19108, 84120 19107, 84120 19106, 84119 19105, 84119 19104, 84118 19103, 84117 19103, 84114 19100, 84114 19099, 84111 19096, 84110 19096, 84109 19095, 84108 19095, 84105 19092, 84104 19092, 84100 19091))";
        Polygon poly = ImageTools.wktToPolygon(wkt);
        WKTWriter wktWriter = new WKTWriter();
        System.out.println("Original Polygon Bounds: " + poly.getEnvelopeInternal());        
        System.out.println("\n--- Geometry Scaling (Vector Pyramid) ---");
        Polygon[] scaledPolygons = toPolygons(wktWriter.write(poly));
        System.out.println("Generated " + scaledPolygons.length + " vector pyramid levels.");
        System.out.println("\n--- Grid Cell Intersection (Tile Pyramid) ---");
        for (int scale = 0; scale < 5; scale++) {
            List<GridCell> cells = getGridCells(poly, scale);
            double currentTileSize = GRIDTILESIZE * (1 << scale);
            System.out.printf("Scale %d (Tile Size: %.0f) -> Touches %d tiles%n", 
            scale, currentTileSize, cells.size());
            if (!cells.isEmpty()) {
                System.out.print("   Sample: ");
                for (int i=0; i<Math.min(5, cells.size()); i++) {
                    System.out.print(cells.get(i) + " ");
                }
                if (cells.size() > 5) System.out.print("...");
                System.out.println();
            }
        }
        Polygon[] p = toPolygons(wkt);
    }
}
