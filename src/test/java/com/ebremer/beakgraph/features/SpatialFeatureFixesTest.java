package com.ebremer.beakgraph.features;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.features.pyradiomics.Gen2DFeatures;
import com.ebremer.halcyon.hilbert.HilbertSpace;
import com.ebremer.halcyon.hilbert.PolygonScaler;
import com.ebremer.halcyon.hilbert.WKTDatatype;
import com.ebremer.ns.PYR;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.Quad;
import org.davidmoten.hilbert.Range;
import org.davidmoten.hilbert.Ranges;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKTReader;

/**
 * Regression tests for the spatial/feature findings BG-146, BG-151, BG-163,
 * BG-149, BG-150/377, BG-373, BG-152, BG-154, BG-153, BG-148 and BG-364.
 */
class SpatialFeatureFixesTest {

    private static final Node F = NodeFactory.createURI("http://ex.org/f");

    private static Map<String, Double> features(String wkt) {
        ArrayList<Quad> quads = new ArrayList<>();
        Gen2DFeatures.generate(quads, F, wkt);
        Map<String, Double> out = new HashMap<>();
        for (Quad q : quads) {
            out.put(q.getPredicate().getLocalName(), Double.parseDouble(q.getObject().getLiteralLexicalForm()));
        }
        return out;
    }

    private static Map<String, String> axes(String wkt) {
        ArrayList<Quad> quads = new ArrayList<>();
        MajorMinor.add(quads, F, wkt);
        Map<String, String> out = new HashMap<>();
        for (Quad q : quads) out.put(q.getPredicate().getLocalName(), q.getObject().getLiteralLexicalForm());
        return out;
    }

    private static String rect(double x0, double y0, double w, double h) {
        return String.format(Locale.ROOT, "POLYGON((%s %s,%s %s,%s %s,%s %s,%s %s))",
                x0, y0, x0 + w, y0, x0 + w, y0 + h, x0, y0 + h, x0, y0);
    }

    // --- BG-146: raster budget ---------------------------------------------

    @Test
    void oversizedPolygonIsRasterizedUnderBudgetAndFeaturesAreRescaled() throws Exception {
        // 40000 x 20000 = 8e8 pixels: 3.2 GB as an INT_RGB image before the fix.
        String big = rect(0, 0, 40_000, 20_000);
        Polygon p = (Polygon) new WKTReader().read(big);
        ShapeAnalysis.Raster r = ShapeAnalysis.getRaster(p);
        long pixels = (long) r.image().getWidth() * r.image().getHeight();
        assertTrue(pixels <= ShapeAnalysis.MAX_RASTER_PIXELS, "raster must respect the pixel budget: " + pixels);
        assertTrue(r.scale() < 1.0);
        Map<String, Double> f = features(big);
        assertEquals(8e8, f.get(PYR.PixelSurface.getLocalName()), 8e8 * 0.01, "pixel surface mapped back to geometry units");
        // Axis lengths of a rectangle: 4 * sqrt(w^2/12) and 4 * sqrt(h^2/12), scale-corrected.
        assertEquals(4 * Math.sqrt(40_000.0 * 40_000.0 / 12), f.get(PYR.MajorAxisLength.getLocalName()), 40_000 * 0.02);
        assertEquals(4 * Math.sqrt(20_000.0 * 20_000.0 / 12), f.get(PYR.MinorAxisLength.getLocalName()), 20_000 * 0.02);
        assertEquals(2.0, f.get(PYR.MajorAxisLength.getLocalName()) / f.get(PYR.MinorAxisLength.getLocalName()), 0.05);
        // A small polygon is rasterized unscaled, as before.
        assertEquals(1.0, ShapeAnalysis.getRaster((Polygon) new WKTReader().read(rect(0, 0, 100, 50))).scale());
    }

    // --- BG-151: MULTIPOLYGON gets features -------------------------------------

    @Test
    void multiPolygonGetsShapeAndAxisFeatures() {
        String multi = "MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0)),((20 0,30 0,30 10,20 10,20 0)))";
        Map<String, Double> f = features(multi);
        assertFalse(f.isEmpty(), "a MULTIPOLYGON must not be skipped as a ClassCastException");
        assertEquals(200.0, f.get(PYR.MeshSurface.getLocalName()), 1e-9);
        Map<String, String> ax = axes(multi);
        assertTrue(ax.get("centroid").startsWith("POINT"), ax.toString());
        assertTrue(ax.get("centroid").contains("15"), "centroid of the union sits between the parts: " + ax.get("centroid"));
        // Non-polygonal input is skipped (warned), not failed.
        assertTrue(features("LINESTRING(0 0,10 10)").isEmpty());
        assertTrue(axes("POINT(1 1)").isEmpty());
    }

    // --- BG-163: near-isotropic regions keep finite axes ---------------------------

    @Test
    void nearIsotropicRegionHasFiniteAxes() {
        // A large disc: catastrophic cancellation in trace^2 - 4 det used to make
        // the discriminant slightly negative and every axis feature NaN.
        StringBuilder disc = new StringBuilder("POLYGON((");
        int n = 720;
        for (int i = 0; i <= n; i++) {
            double a = 2 * Math.PI * (i % n) / n;
            disc.append(String.format(Locale.ROOT, "%.3f %.3f", 5000 + 1500 * Math.cos(a), 5000 + 1500 * Math.sin(a)));
            if (i < n) disc.append(',');
        }
        disc.append("))");
        Map<String, Double> f = features(disc.toString());
        assertNotNull(f.get(PYR.MajorAxisLength.getLocalName()), "axes must be emitted, not dropped as non-finite");
        assertNotNull(f.get(PYR.Elongation.getLocalName()));
        assertEquals(1.0, f.get(PYR.Elongation.getLocalName()), 0.05);
    }

    // --- BG-149 / BG-150 / BG-377 / BG-373: PolygonScaler ------------------------------

    @Test
    void toPolygonsDoesNotMutateTheCallersGeometry() throws Exception {
        Polygon p = (Polygon) new WKTReader().read("POLYGON((0.4 0.4,10.6 0.4,10.6 10.6,0.4 10.6,0.4 0.4))");
        Coordinate[] before = p.getCoordinates().clone();
        Polygon[] pyramid = PolygonScaler.toPolygons(p);
        assertNotNull(pyramid);
        assertTrue(pyramid.length > 0);
        Coordinate[] after = p.getCoordinates();
        for (int i = 0; i < before.length; i++) {
            assertEquals(before[i].x, after[i].x, 0.0, "x of vertex " + i + " must be untouched (no integer snapping of the input)");
            assertEquals(before[i].y, after[i].y, 0.0);
        }
        assertTrue(pyramid[0].getCoordinates()[0].x == Math.rint(pyramid[0].getCoordinates()[0].x), "the pyramid itself is snapped");
    }

    @Test
    void bowtieIsRepairedToItsLargestLobeNotReportedAsCollapsed() throws Exception {
        // Self-intersecting figure-eight: left lobe 20x20, right lobe 10x10.
        Polygon bowtie = (Polygon) new WKTReader().read("POLYGON((0 0,20 20,30 20,30 10,20 20,0 20,0 0))");
        assertFalse(bowtie.isValid());
        Polygon[] pyramid = PolygonScaler.toPolygons(bowtie);
        assertNotNull(pyramid, "a repairable bowtie must not be dropped");
        assertTrue(pyramid.length > 0);
        assertTrue(pyramid[0].isValid());
        assertTrue(pyramid[0].getArea() >= 150, "the largest lobe survives: area " + pyramid[0].getArea());
    }

    @Test
    void unrepairableGeometryIsSkippedWithoutThrowing() throws Exception {
        // Fewer than four distinct points after snapping: collapsed, null, no exception.
        Polygon sliver = (Polygon) new WKTReader().read("POLYGON((0 0,0.1 0.1,0.2 0.2,0 0))");
        assertNull(PolygonScaler.toPolygons(sliver));
        assertNull(PolygonScaler.toPolygons("POLYGON((0 0,0.1 0.1,0.2 0.2,0 0))"));
        assertNull(PolygonScaler.toPolygons("NOT WKT"));
    }

    // --- BG-152 / BG-154 / BG-153: Hilbert helpers ---------------------------------------

    @Test
    void bboxCornerIndicesUseFloorAndClamp() throws Exception {
        Polygon p = (Polygon) new WKTReader().read("POLYGON((-5.7 -5.2,10.6 -5.2,10.6 8.4,-5.7 8.4,-5.7 -5.2))");
        long[] c = HilbertSpace.getBoundingBoxHilbertIndices(p);
        assertEquals(HilbertSpace.hc.index(0, 0), c[0], "negative coordinates clamp to the domain edge, not bit-mask");
        assertEquals(HilbertSpace.hc.index(0, 8), c[1]);
        assertEquals(HilbertSpace.hc.index(10, 8), c[2]);
        assertEquals(HilbertSpace.hc.index(10, 0), c[3]);
        long[] s2 = HilbertSpace.getBoundingBoxHilbertIndices(p, 2);
        assertEquals(HilbertSpace.hc.index(2, 2), s2[2], "scale 2: cells of 4 units");
    }

    @Test
    void neighbourOffTheDomainEdgeIsNeverInRange() {
        ArrayList<Range> ranges = new ArrayList<>();
        long far = HilbertSpace.hc.index(HilbertSpace.MAX_COORD, 5);
        ranges.add(new Range(far, far));
        // (0,5)'s west neighbour is x = -1, which the curve masked onto x = MAX_COORD.
        assertFalse(HilbertSpace.inRange(ranges, new com.ebremer.halcyon.geometry.Point(0, 5), HilbertSpace.W));
        assertFalse(HilbertSpace.contains(ranges, -1, 5));
        assertFalse(HilbertSpace.contains(ranges, 0, -1));
        assertTrue(HilbertSpace.contains(ranges, (int) HilbertSpace.MAX_COORD, 5));
    }

    @Test
    void boundaryWalkRefusesUnboundedCovers() {
        Ranges huge = new Ranges(100);
        huge.add(new Range(0, HilbertSpace.MAX_WALK_CELLS + 10));
        assertThrows(IllegalArgumentException.class, () -> HilbertSpace.GetUpperLeft(huge));
        Ranges small = new Ranges(100);
        long c = HilbertSpace.hc.index(3, 4);
        small.add(new Range(c, c));
        assertEquals(3, HilbertSpace.GetUpperLeft(small).x);
        assertEquals(4, HilbertSpace.GetUpperLeft(small).y);
    }

    // --- BG-148: datatype registration and cheap validation ----------------------------

    @Test
    void wktDatatypeIsRegisteredDeterministicallyAndValidatesCheaply() {
        WKTDatatype.register();
        assertTrue(org.apache.jena.datatypes.TypeMapper.getInstance().getTypeByName(WKTDatatype.URI) instanceof WKTDatatype);
        assertTrue(WKTDatatype.INSTANCE.isValid("POLYGON((0 0,1 0,1 1,0 1,0 0))"));
        assertTrue(WKTDatatype.INSTANCE.isValid("<http://www.opengis.net/def/crs/EPSG/0/4326> POINT(1 2)"));
        assertTrue(WKTDatatype.INSTANCE.isValid("multipolygon(((0 0,1 0,1 1,0 0)))"));
        assertFalse(WKTDatatype.INSTANCE.isValid(""));
        assertFalse(WKTDatatype.INSTANCE.isValid("not wkt"));
        assertFalse(WKTDatatype.INSTANCE.isValid("POLYGON((0 0,1 0,1 1,0 0)"), "unbalanced parentheses");
        assertFalse(WKTDatatype.INSTANCE.isValid("<http://crs POINT(1 2)"), "unterminated CRS prefix");
    }

    // --- BG-364: GridCell key -----------------------------------------------------------

    @Test
    void gridCellKeyIsAsciiInEveryLocale() {
        Locale saved = Locale.getDefault(Locale.Category.FORMAT);
        Locale.setDefault(Locale.Category.FORMAT, Locale.forLanguageTag("ar-EG"));
        try {
            assertEquals("3/12/7/", new com.ebremer.halcyon.hilbert.GridCell((short) 3, 12, 7).toString());
        } finally {
            Locale.setDefault(Locale.Category.FORMAT, saved);
        }
    }
}
