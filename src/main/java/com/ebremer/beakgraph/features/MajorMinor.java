package com.ebremer.beakgraph.features;
import com.ebremer.ns.GEO;
import com.ebremer.ns.HAL;
import java.util.ArrayList;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.Quad;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
/**
 * Adds centroid, major axis and minor axis as WKT literals to a geo:Feature.
 * <p>
 * All values come from the polygon's exact AREA moments (Green's theorem over
 * the rings, shell positive and holes negative), not from a vertex point
 * cloud: the vertex PCA double-weighted ring-closure vertices on holed
 * polygons and measured boundary spread rather than the region, so the drawn
 * axis disagreed with {@code PYR.MajorAxisLength} (raster-region based) for
 * the same geometry. Axis length is the pyradiomics convention, 4*sqrt(λ) of
 * the region's covariance - identical in definition to the raster feature.
 */
public class MajorMinor {
    private static final Logger logger = LoggerFactory.getLogger(MajorMinor.class);


    public static void add(ArrayList<Quad> quads, Node f, String wkt) {
        try {
            Geometry geom = new WKTReader().read(wkt);
            if (!(geom instanceof org.locationtech.jts.geom.Polygonal)) {
                logger.warn("Skipping centroid/axis features for {}: unsupported geometry type {}", f, geom.getGeometryType());
                return;
            }
            double[] g = axes(geom);
            if (g == null) {
                logger.warn("Skipping centroid/axis features for {}: zero-area geometry", f);
                return;
            }
            Node graph = Quad.defaultGraphIRI;
            RDFDatatype wktDT = NodeFactory.getType(GEO.wktLiteral.getURI());
            quads.add(Quad.create(graph, f, HAL.centroid.asNode(), NodeFactory.createLiteralDT(centroidWkt(g), wktDT)));
            quads.add(Quad.create(graph, f, HAL.majorAxis.asNode(), NodeFactory.createLiteralDT(majorWkt(g), wktDT)));
            quads.add(Quad.create(graph, f, HAL.minorAxis.asNode(), NodeFactory.createLiteralDT(minorWkt(g), wktDT)));
        } catch (ParseException | RuntimeException e) {
            // See the Resource overload: skip-with-warning, never abort the build.
            logger.warn("Skipping centroid/axis features for {}: {}", f, e.toString());
        }
    }

    private static String centroidWkt(double[] g) {
        return String.format(Locale.ROOT, "POINT(%.4f %.4f)", g[0], g[1]);
    }

    private static String majorWkt(double[] g) {
        return String.format(Locale.ROOT, "LINESTRING(%.4f %.4f, %.4f %.4f)",
                g[0] - g[2], g[1] - g[3], g[0] + g[2], g[1] + g[3]);
    }

    private static String minorWkt(double[] g) {
        return String.format(Locale.ROOT, "LINESTRING(%.4f %.4f, %.4f %.4f)",
                g[0] - g[4], g[1] - g[5], g[0] + g[4], g[1] + g[5]);
    }

    /**
     * {cx, cy, ax, ay, bx, by}: area centroid and the major/minor HALF-axis
     * vectors, or null when the net area is zero/degenerate. Exact closed-form
     * moments over each ring's edges; ring contributions are sign-normalized so
     * the shell adds and every hole subtracts regardless of winding order in
     * the source WKT.
     */
    /**
     * Exact area moments of any polygonal geometry: every Polygon part
     * contributes its shell positively and its holes negatively, so a
     * MULTIPOLYGON's centroid and axes describe the union of its parts (it
     * used to be skipped without a word).
     */
    private static double[] axes(Geometry geom) {
        double area = 0, sumX = 0, sumY = 0, sumX2 = 0, sumY2 = 0, sumXY = 0;
        for (int part = 0; part < geom.getNumGeometries(); part++) {
          if (!(geom.getGeometryN(part) instanceof Polygon poly)) continue;
          for (int r = -1; r < poly.getNumInteriorRing(); r++) {
            Coordinate[] ring = (r < 0 ? poly.getExteriorRing() : poly.getInteriorRingN(r)).getCoordinates();
            double a = 0, sx = 0, sy = 0, x2 = 0, y2 = 0, xy = 0;
            for (int i = 0; i < ring.length - 1; i++) {
                double x0 = ring[i].x, y0 = ring[i].y;
                double x1 = ring[i + 1].x, y1 = ring[i + 1].y;
                double cross = x0 * y1 - x1 * y0;
                a  += cross;
                sx += (x0 + x1) * cross;
                sy += (y0 + y1) * cross;
                x2 += (x0 * x0 + x0 * x1 + x1 * x1) * cross;
                y2 += (y0 * y0 + y0 * y1 + y1 * y1) * cross;
                xy += (x0 * y1 + 2 * x0 * y0 + 2 * x1 * y1 + x1 * y0) * cross;
            }
            double sign = ((r < 0) == (a >= 0)) ? 1 : -1;
            area  += sign * a / 2;
            sumX  += sign * sx / 6;
            sumY  += sign * sy / 6;
            sumX2 += sign * x2 / 12;
            sumY2 += sign * y2 / 12;
            sumXY += sign * xy / 24;
          }
        }
        if (!(area > 0) || !Double.isFinite(area)) {
            return null;
        }
        double cx = sumX / area;
        double cy = sumY / area;
        // Central second moments per unit area: the region's covariance matrix.
        double varX = sumX2 / area - cx * cx;
        double varY = sumY2 / area - cy * cy;
        double cov  = sumXY / area - cx * cy;
        double trace = varX + varY;
        double disc = Math.hypot(varX - varY, 2 * cov);   // sqrt(trace^2 - 4 det) without cancellation
        double l0 = Math.max(0, (trace + disc) / 2);
        double l1 = Math.max(0, (trace - disc) / 2);
        double v0x, v0y, v1x, v1y;
        if (Math.abs(cov) < 1e-12) {
            if (varX >= varY) { v0x = 1; v0y = 0; v1x = 0; v1y = 1; }
            else              { v0x = 0; v0y = 1; v1x = 1; v1y = 0; }
        } else {
            v0x = cov; v0y = l0 - varX;
            double n0 = Math.hypot(v0x, v0y);
            v0x /= n0; v0y /= n0;
            v1x = cov; v1y = l1 - varX;
            double n1 = Math.hypot(v1x, v1y);
            v1x /= n1; v1y /= n1;
        }
        if (v0x < 0) { v0x = -v0x; v0y = -v0y; }
        if (v1x < 0) { v1x = -v1x; v1y = -v1y; }
        // Half-axis = (4*sqrt(λ)) / 2, the pyradiomics full axis length halved.
        double ax = 2 * Math.sqrt(l0) * v0x;
        double ay = 2 * Math.sqrt(l0) * v0y;
        double bx = 2 * Math.sqrt(l1) * v1x;
        double by = 2 * Math.sqrt(l1) * v1y;
        return new double[]{cx, cy, ax, ay, bx, by};
    }
}
