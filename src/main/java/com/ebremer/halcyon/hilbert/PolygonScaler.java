package com.ebremer.halcyon.hilbert;

import com.ebremer.beakgraph.core.lib.GEO;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.Quad;
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import org.locationtech.jts.util.GeometricShapeFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PolygonScaler {

    private static final String BASE_PREDICATE = "https://halcyon.is/ns/asHilbert";
    private static final RDFDatatype WKT_TYPE = TypeMapper.getInstance().getSafeTypeByName(GEO.wktLiteral.toString());

    /**
     * Generates a sequence of progressively quarter-area scaled polygons from a geo:wktLiteral string.
     * The returned array contains:
     *   result[0] → polygon scaled by 0.25  (area = original × 0.25)
     *   result[1] → polygon scaled by 0.0625 (area = original × 0.0625)
     *   result[n] → further quarter-area reductions
     * Scaling stops when the area becomes ≤ 1.0 or the geometry becomes invalid/degenerate.
     *
     * @param wkt Well-Known Text representation of a Polygon (geo:wktLiteral)
     * @return Array of scaled Polygon objects; never null (may be empty if input is invalid)
     */
    public static Polygon[] toHilbert(String wkt) {
        GeometryFactory gf = new GeometryFactory();
        WKTReader reader = new WKTReader(gf);

        Polygon original;
        try {
            Geometry geom = reader.read(wkt);
            if (!(geom instanceof Polygon)) {
                throw new IllegalArgumentException("Provided WKT must represent a Polygon "+wkt);
            }
            original = (Polygon) geom;
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse input WKT as Polygon "+wkt, e);
        }

        List<Polygon> scaledPolygons = new ArrayList<>();

        Polygon current = snapAndSimplify(original, gf);
        if (current == null || !current.isValid()) {
            return new Polygon[0];
        }

        while (true) {
            double area = current.getArea();

            // The first scaled polygon (¼ area) goes into index 0, etc.
            scaledPolygons.add(current);

            if (area <= 1.0) {
                break;
            }

            // Scale 50% around centroid → new area = previous area × 0.25
            Coordinate centroid = current.getCentroid().getCoordinate();
            AffineTransformation scaleTrans = AffineTransformation.scaleInstance(0.5, 0.5, centroid.x, centroid.y);
            Geometry scaled = scaleTrans.transform(current);

            // Snap to integer grid
            scaled.apply(new IntSnapFilter());
            scaled.geometryChanged();

            // Clean up and simplify
            current = snapAndSimplify((Polygon) scaled, gf);

            if (current == null || current.getNumPoints() < 4 || !current.isValid()) {
                break;
            }
        }

        return scaledPolygons.toArray(new Polygon[0]);
    }

    // ---------- SNAP + SIMPLIFY ----------
    private static Polygon snapAndSimplify(Polygon poly, GeometryFactory gf) {
        poly.apply(new IntSnapFilter());
        poly.geometryChanged();

        poly = removeDuplicateAndCollinearVertices(poly, gf);
        if (poly == null || poly.getNumPoints() < 4) {
            return null;
        }

        Geometry cleaned = poly.buffer(0);
        return (cleaned instanceof Polygon) ? (Polygon) cleaned : null;
    }

    // ---------- SNAP FILTER ----------
    static class IntSnapFilter implements CoordinateSequenceFilter {
        @Override
        public void filter(CoordinateSequence seq, int i) {
            seq.setOrdinate(i, 0, Math.round(seq.getOrdinate(i, 0)));
            seq.setOrdinate(i, 1, Math.round(seq.getOrdinate(i, 1)));
        }

        @Override public boolean isDone() { return false; }
        @Override public boolean isGeometryChanged() { return true; }
    }

    // ---------- REMOVE DUPLICATE & COLLINEAR ----------
    private static Polygon removeDuplicateAndCollinearVertices(Polygon poly, GeometryFactory gf) {
        Coordinate[] coords = poly.getExteriorRing().getCoordinates();
        List<Coordinate> cleaned = new ArrayList<>();

        for (Coordinate coord : coords) {
            if (cleaned.isEmpty() || !coord.equals2D(cleaned.get(cleaned.size() - 1))) {
                cleaned.add(coord);
            }
        }

        // Remove collinear points (simple forward pass)
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
        if (cleaned.size() > 0 && !cleaned.get(0).equals2D(cleaned.get(cleaned.size() - 1))) {
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

    // -----------------------------------------------------------------
    // The original toHilbertQuads method has been removed as requested.
    // -----------------------------------------------------------------

    public static void main(String[] args) {
        GeometryFactory gf = new GeometryFactory();
        Random rnd = new Random();
        GeometricShapeFactory sf = new GeometricShapeFactory(gf);
        sf.setNumPoints(10);
        sf.setCentre(new Coordinate(rnd.nextDouble() * 100, rnd.nextDouble() * 100));
        sf.setSize(20 + rnd.nextDouble() * 30);
        Polygon poly = sf.createEllipse();

        WKTWriter wktWriter = new WKTWriter();
        System.out.println("Original polygon:\n" + wktWriter.write(poly));

        Polygon[] scaled = toHilbert(wktWriter.write(poly));
        for (int i = 0; i < scaled.length; i++) {
            System.out.printf("Level %d (area ≈ %.6f):%n%s%n%n",
                    i + 1, scaled[i].getArea(), wktWriter.write(scaled[i]));
        }
    }
}