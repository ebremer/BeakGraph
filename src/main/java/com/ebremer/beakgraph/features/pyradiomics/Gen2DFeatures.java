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

public class Gen2DFeatures {
    public static void Generate(Resource geo, String wktPolygon) {
        try {
            GeometryFactory gf = new GeometryFactory();
            Polygon p = (Polygon) new WKTReader(gf).read(wktPolygon);
            java.awt.image.BufferedImage bi = ShapeAnalysis.getBufferedImage(p);
            org.ejml.simple.SimpleMatrix pts = ShapeAnalysis.BufferedImage2INDArray(bi);
            geo.addProperty(PYR.MeshSurface, ResourceFactory.createTypedLiteral(ShapeAnalysis.getMeshSurfaceFeatureValue(p)));
            geo.addProperty(PYR.PixelSurface, ResourceFactory.createTypedLiteral(ShapeAnalysis.getPixelSurfaceFeatureValue(bi)));
            geo.addProperty(PYR.Perimeter, ResourceFactory.createTypedLiteral(ShapeAnalysis.getPerimeter(p)));
            geo.addProperty(PYR.PerimeterSurfaceRatio, ResourceFactory.createTypedLiteral(ShapeAnalysis.getPerimeterSurfaceRatioFeatureValue(p)));
            geo.addProperty(PYR.Sphericity, ResourceFactory.createTypedLiteral(ShapeAnalysis.getSphericityFeatureValue(p)));
            geo.addProperty(PYR.SphericalDisproportion, ResourceFactory.createTypedLiteral(ShapeAnalysis.getSphericalDisproportionFeatureValue(p)));
            geo.addProperty(PYR.Maximum2DDiameter, ResourceFactory.createTypedLiteral(ShapeAnalysis.getMaximum2DDiameterFeatureValue(p)));
            geo.addProperty(PYR.MajorAxisLength, ResourceFactory.createTypedLiteral(ShapeAnalysis.getMajorAxisLengthFeatureValue(pts)));
            geo.addProperty(PYR.MinorAxisLength, ResourceFactory.createTypedLiteral(ShapeAnalysis.getMinorAxisLengthFeatureValue(pts)));
            geo.addProperty(PYR.Elongation, ResourceFactory.createTypedLiteral(ShapeAnalysis.getElongationFeatureValue(pts)));
        } catch (Exception e) {}
    }
    
    public static void Generate(ArrayList<Quad> quads, Node geo, String wkt) {
        try {
            GeometryFactory gf = new GeometryFactory();
            Polygon p = (Polygon) new WKTReader(gf).read(wkt);
            java.awt.image.BufferedImage bi = ShapeAnalysis.getBufferedImage(p);
            org.ejml.simple.SimpleMatrix pts = ShapeAnalysis.BufferedImage2INDArray(bi);
            Node graph = Quad.defaultGraphIRI;
            quads.add(Quad.create(graph, geo, PYR.MeshSurface.asNode(), ResourceFactory.createTypedLiteral(ShapeAnalysis.getMeshSurfaceFeatureValue(p)).asNode()));
            quads.add(Quad.create(graph, geo, PYR.PixelSurface.asNode(), ResourceFactory.createTypedLiteral(ShapeAnalysis.getPixelSurfaceFeatureValue(bi)).asNode()));
            quads.add(Quad.create(graph, geo, PYR.Perimeter.asNode(), ResourceFactory.createTypedLiteral(ShapeAnalysis.getPerimeter(p)).asNode()));
            quads.add(Quad.create(graph, geo, PYR.PerimeterSurfaceRatio.asNode(), ResourceFactory.createTypedLiteral(ShapeAnalysis.getPerimeterSurfaceRatioFeatureValue(p)).asNode()));
            quads.add(Quad.create(graph, geo, PYR.Sphericity.asNode(), ResourceFactory.createTypedLiteral(ShapeAnalysis.getSphericityFeatureValue(p)).asNode()));
            quads.add(Quad.create(graph, geo, PYR.SphericalDisproportion.asNode(), ResourceFactory.createTypedLiteral(ShapeAnalysis.getSphericalDisproportionFeatureValue(p)).asNode()));
            quads.add(Quad.create(graph, geo, PYR.Maximum2DDiameter.asNode(), ResourceFactory.createTypedLiteral(ShapeAnalysis.getMaximum2DDiameterFeatureValue(p)).asNode()));
            quads.add(Quad.create(graph, geo, PYR.MajorAxisLength.asNode(), ResourceFactory.createTypedLiteral(ShapeAnalysis.getMajorAxisLengthFeatureValue(pts)).asNode()));
            quads.add(Quad.create(graph, geo, PYR.MinorAxisLength.asNode(), ResourceFactory.createTypedLiteral(ShapeAnalysis.getMinorAxisLengthFeatureValue(pts)).asNode()));
            quads.add(Quad.create(graph, geo, PYR.Elongation.asNode(), ResourceFactory.createTypedLiteral(ShapeAnalysis.getElongationFeatureValue(pts)).asNode()));
        } catch (Exception e) {}
    }
}
