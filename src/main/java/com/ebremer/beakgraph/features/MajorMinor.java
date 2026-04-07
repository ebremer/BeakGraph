package com.ebremer.beakgraph.features;
import com.ebremer.ns.GEO;
import com.ebremer.ns.HAL;
import java.util.ArrayList;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.Quad;
import org.ejml.simple.SimpleEVD;
import org.ejml.simple.SimpleMatrix;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
/**
 * Adds centroid, major axis and minor axis as WKT literals to a geo:Feature
 */
public class MajorMinor {
    public static void Add(Resource f, String wkt) {
        try {
            WKTReader reader = new WKTReader();
            Geometry geom = reader.read(wkt);
            if (!(geom instanceof Polygon)) return;
            Polygon poly = (Polygon) geom;
            Coordinate[] coords = poly.getCoordinates();
            // build point cloud
            int n = coords.length - 1; // close ring
            SimpleMatrix m = new SimpleMatrix(n, 2);
            for (int i = 0; i < n; i++) {
                m.set(i, 0, coords[i].x);
                m.set(i, 1, coords[i].y);
            }
            // centroid
            double mx = m.extractVector(false, 0).elementSum() / n;
            double my = m.extractVector(false, 1).elementSum() / n;
            // PCA
            SimpleMatrix ones = SimpleMatrix.ones(n, 1);
            SimpleMatrix meanVec = new SimpleMatrix(1, 2, true, mx, my);
            SimpleMatrix centered = m.minus(ones.mult(meanVec));
            SimpleMatrix cov = centered.transpose().mult(centered).divide(n - 1.0);
            SimpleEVD evd = cov.eig();
            double lambda0 = evd.getEigenvalue(0).getReal();
            double lambda1 = evd.getEigenvalue(1).getReal();
            SimpleMatrix v0 = (SimpleMatrix) evd.getEigenVector(0).copy();
            SimpleMatrix v1 = (SimpleMatrix) evd.getEigenVector(1).copy();
            if (lambda0 < lambda1) {
                double t = lambda0; lambda0 = lambda1; lambda1 = t;
                SimpleMatrix tv = v0; v0 = v1; v1 = tv;
            }
            if (v0.get(0) < 0) v0 = v0.scale(-1.0);
            if (v1.get(0) < 0) v1 = v1.scale(-1.0);
            double majorlen = 2 * Math.sqrt(lambda0);
            double minorlen = 2 * Math.sqrt(lambda1);
            // half-axis vectors
            double ax = majorlen / 2 * v0.get(0);
            double ay = majorlen / 2 * v0.get(1);
            double bx = minorlen / 2 * v1.get(0);
            double by = minorlen / 2 * v1.get(1);
            // WKT strings
            String centroidWKT = String.format("POINT(%.4f %.4f)", mx, my);
            String majorWKT = String.format("LINESTRING(%.4f %.4f, %.4f %.4f)",
                    mx - ax, my - ay, mx + ax, my + ay);
            String minorWKT = String.format("LINESTRING(%.4f %.4f, %.4f %.4f)",
                    mx - bx, my - by, mx + bx, my + by);
            // add to the feature
            f.addProperty(HAL.centroid, f.getModel().createTypedLiteral(centroidWKT, GEO.wktLiteral.getURI()));
            f.addProperty(HAL.majorAxis, f.getModel().createTypedLiteral(majorWKT, GEO.wktLiteral.getURI()));
            f.addProperty(HAL.minorAxis, f.getModel().createTypedLiteral(minorWKT, GEO.wktLiteral.getURI()));
        } catch (ParseException ignored) {}
    }
    
    public static void Add(ArrayList<Quad> quads, Node f, String wkt) {
        try {
            WKTReader reader = new WKTReader();
            Geometry geom = reader.read(wkt);
            if (!(geom instanceof Polygon)) return;
            Polygon poly = (Polygon) geom;
            Coordinate[] coords = poly.getCoordinates();
            int n = coords.length - 1;
            SimpleMatrix m = new SimpleMatrix(n, 2);
            for (int i = 0; i < n; i++) {
                m.set(i, 0, coords[i].x);
                m.set(i, 1, coords[i].y);
            }
            double mx = m.extractVector(false, 0).elementSum() / n;
            double my = m.extractVector(false, 1).elementSum() / n;
            SimpleMatrix ones = SimpleMatrix.ones(n, 1);
            SimpleMatrix meanVec = new SimpleMatrix(1, 2, true, mx, my);
            SimpleMatrix centered = m.minus(ones.mult(meanVec));
            SimpleMatrix cov = centered.transpose().mult(centered).divide(n - 1.0);
            SimpleEVD evd = cov.eig();
            double lambda0 = evd.getEigenvalue(0).getReal();
            double lambda1 = evd.getEigenvalue(1).getReal();
            SimpleMatrix v0 = (SimpleMatrix) evd.getEigenVector(0).copy();
            SimpleMatrix v1 = (SimpleMatrix) evd.getEigenVector(1).copy();
            if (lambda0 < lambda1) {
                double t = lambda0; lambda0 = lambda1; lambda1 = t;
                SimpleMatrix tv = v0; v0 = v1; v1 = tv;
            }
            if (v0.get(0) < 0) v0 = v0.scale(-1.0);
            if (v1.get(0) < 0) v1 = v1.scale(-1.0);
            double majorlen = 2 * Math.sqrt(lambda0);
            double minorlen = 2 * Math.sqrt(lambda1);
            double ax = majorlen / 2 * v0.get(0);
            double ay = majorlen / 2 * v0.get(1);
            double bx = minorlen / 2 * v1.get(0);
            double by = minorlen / 2 * v1.get(1);
            String centroidWKT = String.format("POINT(%.4f %.4f)", mx, my);
            String majorWKT = String.format("LINESTRING(%.4f %.4f, %.4f %.4f)", mx - ax, my - ay, mx + ax, my + ay);
            String minorWKT = String.format("LINESTRING(%.4f %.4f, %.4f %.4f)", mx - bx, my - by, mx + bx, my + by);
            Node graph = Quad.defaultGraphIRI;
            RDFDatatype wktDT = NodeFactory.getType(GEO.wktLiteral.getURI());
            quads.add(Quad.create(graph, f, HAL.centroid.asNode(), NodeFactory.createLiteralDT(centroidWKT, wktDT)));
            quads.add(Quad.create(graph, f, HAL.majorAxis.asNode(), NodeFactory.createLiteralDT(majorWKT, wktDT)));
            quads.add(Quad.create(graph, f, HAL.minorAxis.asNode(), NodeFactory.createLiteralDT(minorWKT, wktDT)));
        } catch (ParseException ignored) {}
    }
}
