package com.ebremer.beakgraph.features;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.features.pyradiomics.Gen2DFeatures;
import com.ebremer.ns.HAL;
import com.ebremer.ns.PYR;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.Quad;
import org.junit.jupiter.api.Test;

/**
 * Feature-math edge cases:
 * <ul>
 *   <li>a valid polygon thinner than ~0.5 units must still get features (the
 *       raster used to round to a 0-sized image and BufferedImage threw,
 *       skipping even the purely polygon-based features);</li>
 *   <li>zero-area geometry must not emit "INF"^^xsd:double literals;</li>
 *   <li>MajorMinor values come from exact AREA moments: holed polygons get an
 *       unbiased centroid (the vertex cloud double-weighted ring closures) and
 *       axis lengths follow the pyradiomics 4*sqrt(lambda) convention that
 *       PYR.MajorAxisLength uses.</li>
 * </ul>
 */
class FeatureEdgeCasesTest {

    private static final Node F = NodeFactory.createURI("http://ex.org/f");

    private static Map<Node, String> byPredicate(ArrayList<Quad> quads) {
        return quads.stream().collect(Collectors.toMap(Quad::getPredicate,
                q -> q.getObject().getLiteralLexicalForm()));
    }

    @Test
    void thinPolygonStillGetsAllFeatures() {
        ArrayList<Quad> quads = new ArrayList<>();
        Gen2DFeatures.generate(quads, F, "POLYGON((0 0,100 0,100 0.4,0 0.4,0 0))");
        Map<Node, String> features = byPredicate(quads);
        assertTrue(features.containsKey(PYR.MeshSurface.asNode()),
                "polygon-based features must survive a sub-pixel raster height");
        assertEquals(40.0, Double.parseDouble(features.get(PYR.MeshSurface.asNode())), 1e-9);
        assertTrue(features.containsKey(PYR.PixelSurface.asNode()),
                "the raster features must be computed on the clamped 1px image");
    }

    @Test
    void zeroAreaGeometryEmitsNoNonFiniteFeatureValues() {
        // The bowtie's shoelace area is exactly 0: the ratio features divide by
        // it and used to store "INF"^^xsd:double as real feature values.
        ArrayList<Quad> quads = new ArrayList<>();
        Gen2DFeatures.generate(quads, F, "POLYGON((0 0,10 10,10 0,0 10,0 0))");
        Map<Node, String> features = byPredicate(quads);
        assertTrue(features.containsKey(PYR.Perimeter.asNode()), "finite features are still emitted");
        assertFalse(features.containsKey(PYR.PerimeterSurfaceRatio.asNode()),
                "perimeter/area is infinite for zero area and must be skipped");
        assertFalse(features.containsKey(PYR.SphericalDisproportion.asNode()));
        for (Map.Entry<Node, String> e : features.entrySet()) {
            assertTrue(Double.isFinite(Double.parseDouble(e.getValue())),
                    "non-finite value emitted for " + e.getKey() + ": " + e.getValue());
        }
    }

    @Test
    void holedPolygonCentroidIsTheAreaCentroid() {
        // Square shell with a centered square hole: the area centroid is exactly
        // the center. The vertex cloud kept the shell's ring-closure vertex, so
        // the old PCA centroid was biased toward it (~1.78 instead of 2).
        ArrayList<Quad> quads = new ArrayList<>();
        MajorMinor.add(quads, F, "POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1,3 1,3 3,1 3,1 1))");
        Map<Node, String> features = byPredicate(quads);
        assertEquals("POINT(2.0000 2.0000)", features.get(HAL.centroid.asNode()));
    }

    @Test
    void axisLengthsFollowThePyradiomicsAreaConvention() throws Exception {
        // Rectangle 20x10: region variance along x is 20^2/12, so the pyradiomics
        // major axis is 4*sqrt(400/12) = 23.094 - the SAME definition
        // PYR.MajorAxisLength uses on the raster. The old vertex-cloud 2*sqrt
        // drawn axis was mutually inconsistent with it.
        ArrayList<Quad> quads = new ArrayList<>();
        MajorMinor.add(quads, F, "POLYGON((0 0,20 0,20 10,0 10,0 0))");
        Map<Node, String> features = byPredicate(quads);

        org.locationtech.jts.geom.Geometry major = new org.locationtech.jts.io.WKTReader()
                .read(features.get(HAL.majorAxis.asNode()));
        org.locationtech.jts.geom.Geometry minor = new org.locationtech.jts.io.WKTReader()
                .read(features.get(HAL.minorAxis.asNode()));
        assertEquals(4 * Math.sqrt(400.0 / 12.0), major.getLength(), 1e-3);
        assertEquals(4 * Math.sqrt(100.0 / 12.0), minor.getLength(), 1e-3);
        // The major axis runs along x, centered on the area centroid.
        org.locationtech.jts.geom.Coordinate[] c = major.getCoordinates();
        assertEquals(5.0, c[0].y, 1e-6);
        assertEquals(5.0, c[1].y, 1e-6);
    }
}
