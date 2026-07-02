package com.ebremer.beakgraph.features.pyradiomics;
import com.ebremer.ns.PYR;
import com.ebremer.beakgraph.features.ShapeAnalysis;
import java.util.ArrayList;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.sparql.core.Quad;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKTReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Gen2DFeatures {
    private static final Logger logger = LoggerFactory.getLogger(Gen2DFeatures.class);
    public static void generate(Resource geo, String wktPolygon) {
        try {
            GeometryFactory gf = new GeometryFactory();
            Polygon p = (Polygon) new WKTReader(gf).read(wktPolygon);
            java.awt.image.BufferedImage bi = ShapeAnalysis.getBufferedImage(p);
            org.ejml.simple.SimpleMatrix pts = ShapeAnalysis.BufferedImage2INDArray(bi);
            addFinite(geo, PYR.MeshSurface, ShapeAnalysis.getMeshSurfaceFeatureValue(p));
            addFinite(geo, PYR.PixelSurface, ShapeAnalysis.getPixelSurfaceFeatureValue(bi));
            addFinite(geo, PYR.Perimeter, ShapeAnalysis.getPerimeter(p));
            addFinite(geo, PYR.PerimeterSurfaceRatio, ShapeAnalysis.getPerimeterSurfaceRatioFeatureValue(p));
            addFinite(geo, PYR.Sphericity, ShapeAnalysis.getSphericityFeatureValue(p));
            addFinite(geo, PYR.SphericalDisproportion, ShapeAnalysis.getSphericalDisproportionFeatureValue(p));
            addFinite(geo, PYR.Maximum2DDiameter, ShapeAnalysis.getMaximum2DDiameterFeatureValue(p));
            addFinite(geo, PYR.MajorAxisLength, ShapeAnalysis.getMajorAxisLengthFeatureValue(pts));
            addFinite(geo, PYR.MinorAxisLength, ShapeAnalysis.getMinorAxisLengthFeatureValue(pts));
            addFinite(geo, PYR.Elongation, ShapeAnalysis.getElongationFeatureValue(pts));
        } catch (Exception e) {
            // A geometry whose features cannot be computed is skipped - but never
            // silently: silently-missing features read as "no data" downstream.
            logger.warn("Failed to generate 2D shape features for {}: {}", geo, e.toString());
        }
    }

    public static void generate(ArrayList<Quad> quads, Node geo, String wkt) {
        try {
            GeometryFactory gf = new GeometryFactory();
            Polygon p = (Polygon) new WKTReader(gf).read(wkt);
            java.awt.image.BufferedImage bi = ShapeAnalysis.getBufferedImage(p);
            org.ejml.simple.SimpleMatrix pts = ShapeAnalysis.BufferedImage2INDArray(bi);
            Node graph = Quad.defaultGraphIRI;
            addFinite(quads, graph, geo, PYR.MeshSurface.asNode(), ShapeAnalysis.getMeshSurfaceFeatureValue(p));
            addFinite(quads, graph, geo, PYR.PixelSurface.asNode(), ShapeAnalysis.getPixelSurfaceFeatureValue(bi));
            addFinite(quads, graph, geo, PYR.Perimeter.asNode(), ShapeAnalysis.getPerimeter(p));
            addFinite(quads, graph, geo, PYR.PerimeterSurfaceRatio.asNode(), ShapeAnalysis.getPerimeterSurfaceRatioFeatureValue(p));
            addFinite(quads, graph, geo, PYR.Sphericity.asNode(), ShapeAnalysis.getSphericityFeatureValue(p));
            addFinite(quads, graph, geo, PYR.SphericalDisproportion.asNode(), ShapeAnalysis.getSphericalDisproportionFeatureValue(p));
            addFinite(quads, graph, geo, PYR.Maximum2DDiameter.asNode(), ShapeAnalysis.getMaximum2DDiameterFeatureValue(p));
            addFinite(quads, graph, geo, PYR.MajorAxisLength.asNode(), ShapeAnalysis.getMajorAxisLengthFeatureValue(pts));
            addFinite(quads, graph, geo, PYR.MinorAxisLength.asNode(), ShapeAnalysis.getMinorAxisLengthFeatureValue(pts));
            addFinite(quads, graph, geo, PYR.Elongation.asNode(), ShapeAnalysis.getElongationFeatureValue(pts));
        } catch (Exception e) {
            logger.warn("Failed to generate 2D shape features for {}: {}", geo, e.toString());
        }
    }

    /**
     * Emit a feature only when its value is a real number. Degenerate geometry
     * (a zero-area bowtie, say) drives the ratio features to Infinity/NaN, and
     * an "INF"^^xsd:double literal stored as a real feature value poisons every
     * numeric consumer downstream; the finite features of the same geometry are
     * still emitted.
     */
    private static void addFinite(Resource geo, org.apache.jena.rdf.model.Property feature, double value) {
        if (Double.isFinite(value)) {
            geo.addProperty(feature, ResourceFactory.createTypedLiteral(value));
        } else {
            logger.warn("Skipping non-finite feature {} for {}", feature.getLocalName(), geo);
        }
    }

    private static void addFinite(ArrayList<Quad> quads, Node graph, Node geo, Node feature, double value) {
        if (Double.isFinite(value)) {
            quads.add(Quad.create(graph, geo, feature, ResourceFactory.createTypedLiteral(value).asNode()));
        } else {
            logger.warn("Skipping non-finite feature {} for {}", feature, geo);
        }
    }
}
