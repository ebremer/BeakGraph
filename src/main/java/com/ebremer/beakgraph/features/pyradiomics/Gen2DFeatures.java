package com.ebremer.beakgraph.features.pyradiomics;
import com.ebremer.ns.PYR;
import com.ebremer.beakgraph.features.ShapeAnalysis;
import java.util.ArrayList;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.sparql.core.Quad;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Gen2DFeatures {
    private static final Logger logger = LoggerFactory.getLogger(Gen2DFeatures.class);

    /**
     * Emits the pyradiomics 2D shape features of {@code wkt} as quads. Any
     * polygonal geometry is accepted (a MULTIPOLYGON's features describe the
     * union of its parts); other geometry types are skipped with a warning
     * rather than reported as a failure.
     */
    public static void generate(ArrayList<Quad> quads, Node geo, String wkt) {
        try {
            GeometryFactory gf = new GeometryFactory();
            Geometry p = new WKTReader(gf).read(wkt);
            if (!(p instanceof org.locationtech.jts.geom.Polygonal)) {
                logger.warn("Skipping 2D shape features for {}: unsupported geometry type {}", geo, p.getGeometryType());
                return;
            }
            ShapeAnalysis.Raster raster = ShapeAnalysis.getRaster(p);
            java.awt.image.BufferedImage bi = raster.image();
            double scale = raster.scale();
            org.ejml.simple.SimpleMatrix pts = ShapeAnalysis.BufferedImage2INDArray(bi);
            Node graph = Quad.defaultGraphIRI;
            addFinite(quads, graph, geo, PYR.MeshSurface.asNode(), ShapeAnalysis.getMeshSurfaceFeatureValue(p));
            // Pixel-derived features are measured on the (possibly down-scaled)
            // raster and mapped back to geometry units.
            addFinite(quads, graph, geo, PYR.PixelSurface.asNode(), ShapeAnalysis.getPixelSurfaceFeatureValue(bi) / (scale * scale));
            addFinite(quads, graph, geo, PYR.Perimeter.asNode(), ShapeAnalysis.getPerimeter(p));
            addFinite(quads, graph, geo, PYR.PerimeterSurfaceRatio.asNode(), ShapeAnalysis.getPerimeterSurfaceRatioFeatureValue(p));
            addFinite(quads, graph, geo, PYR.Sphericity.asNode(), ShapeAnalysis.getSphericityFeatureValue(p));
            addFinite(quads, graph, geo, PYR.SphericalDisproportion.asNode(), ShapeAnalysis.getSphericalDisproportionFeatureValue(p));
            addFinite(quads, graph, geo, PYR.Maximum2DDiameter.asNode(), ShapeAnalysis.getMaximum2DDiameterFeatureValue(p));
            addFinite(quads, graph, geo, PYR.MajorAxisLength.asNode(), ShapeAnalysis.getMajorAxisLengthFeatureValue(pts) / scale);
            addFinite(quads, graph, geo, PYR.MinorAxisLength.asNode(), ShapeAnalysis.getMinorAxisLengthFeatureValue(pts) / scale);
            addFinite(quads, graph, geo, PYR.Elongation.asNode(), ShapeAnalysis.getElongationFeatureValue(pts));
        } catch (Exception e) {
            // A geometry whose features cannot be computed is skipped - but never
            // silently: silently-missing features read as "no data" downstream.
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

    private static void addFinite(ArrayList<Quad> quads, Node graph, Node geo, Node feature, double value) {
        if (Double.isFinite(value)) {
            quads.add(Quad.create(graph, geo, feature, ResourceFactory.createTypedLiteral(value).asNode()));
        } else {
            logger.warn("Skipping non-finite feature {} for {}", feature, geo);
        }
    }
}
