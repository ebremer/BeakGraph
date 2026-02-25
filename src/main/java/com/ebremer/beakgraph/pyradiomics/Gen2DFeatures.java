package com.ebremer.beakgraph.pyradiomics;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKTReader;

public class Gen2DFeatures {
    private static final String NS = "https://pyradiomics.org/ns/shape2d#";
    private static final Property MeshSurface = ResourceFactory.createProperty(NS, "MeshSurface");
    private static final Property PixelSurface = ResourceFactory.createProperty(NS, "PixelSurface");
    private static final Property Perimeter = ResourceFactory.createProperty(NS, "Perimeter");
    private static final Property PerimeterSurfaceRatio = ResourceFactory.createProperty(NS, "PerimeterSurfaceRatio");
    private static final Property Sphericity = ResourceFactory.createProperty(NS, "Sphericity");
    private static final Property SphericalDisproportion = ResourceFactory.createProperty(NS, "SphericalDisproportion");
    private static final Property Maximum2DDiameter = ResourceFactory.createProperty(NS, "Maximum2DDiameter");
    private static final Property MajorAxisLength = ResourceFactory.createProperty(NS, "MajorAxisLength");
    private static final Property MinorAxisLength = ResourceFactory.createProperty(NS, "MinorAxisLength");
    private static final Property Elongation = ResourceFactory.createProperty(NS, "Elongation");

    public static void Generate(Resource g, String wktPolygon) {
        try {
            GeometryFactory gf = new GeometryFactory();
            Polygon p = (Polygon) new WKTReader(gf).read(wktPolygon);
            java.awt.image.BufferedImage bi = ShapeAnalysis.getBufferedImage(p);
            org.ejml.simple.SimpleMatrix pts = ShapeAnalysis.BufferedImage2INDArray(bi);
            g.addProperty(MeshSurface, ResourceFactory.createTypedLiteral(ShapeAnalysis.getMeshSurfaceFeatureValue(p)));
            g.addProperty(PixelSurface, ResourceFactory.createTypedLiteral(ShapeAnalysis.getPixelSurfaceFeatureValue(bi)));
            g.addProperty(Perimeter, ResourceFactory.createTypedLiteral(ShapeAnalysis.getPerimeter(p)));
            g.addProperty(PerimeterSurfaceRatio, ResourceFactory.createTypedLiteral(ShapeAnalysis.getPerimeterSurfaceRatioFeatureValue(p)));
            g.addProperty(Sphericity, ResourceFactory.createTypedLiteral(ShapeAnalysis.getSphericityFeatureValue(p)));
            g.addProperty(SphericalDisproportion, ResourceFactory.createTypedLiteral(ShapeAnalysis.getSphericalDisproportionFeatureValue(p)));
            g.addProperty(Maximum2DDiameter, ResourceFactory.createTypedLiteral(ShapeAnalysis.getMaximum2DDiameterFeatureValue(p)));
            g.addProperty(MajorAxisLength, ResourceFactory.createTypedLiteral(ShapeAnalysis.getMajorAxisLengthFeatureValue(pts)));
            g.addProperty(MinorAxisLength, ResourceFactory.createTypedLiteral(ShapeAnalysis.getMinorAxisLengthFeatureValue(pts)));
            g.addProperty(Elongation, ResourceFactory.createTypedLiteral(ShapeAnalysis.getElongationFeatureValue(pts)));
        } catch (Exception e) {}
    }
}
