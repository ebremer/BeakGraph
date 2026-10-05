package com.ebremer.beakgraph.utils;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.io.WKTReader;
import java.awt.*;
import java.awt.geom.Path2D;
import java.awt.image.BufferedImage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Utility class for rendering JTS Polygon geometries (in image pixel coordinates)
 * onto BufferedImage and converting WKT strings to JTS Polygon(s).
 *
 * Coordinates are assumed to be in pixel space:
 * - Origin: top-left
 * - Y-axis: positive downward
 *
 * @author erich
 */
public class ImageTools {
    // Safe to share across threads: JTS WKTReader.read() (>= 1.19) creates its
    // StreamTokenizer locally and passes it through as a method parameter, and only reads
    // its construction-time fields - so concurrent reads (e.g. parallel addSpatial tasks)
    // don't interfere. Do not call configuration setters on it after construction, and note
    // that JTS < 1.19 kept the tokenizer in an instance field and was NOT thread-safe.
    private static final WKTReader WKT_READER = new WKTReader();



    public static Polygon wktToPolygon(String wkt) throws Exception {
        List<Polygon> polygons = wktToPolygons(wkt);
        return polygons.isEmpty() ? null : polygons.get(0);
    }

    /**
     * Strips the optional CRS/SRS URI prefix from a GeoSPARQL wktLiteral
     * ("&lt;http://...crs...&gt; POLYGON(...)" -&gt; "POLYGON(...)"). The GeoSPARQL
     * spec allows the prefix and data in the wild commonly carries it; JTS's
     * WKTReader does not accept it.
     */
    public static String stripCrs(String wkt) {
        String trimmed = wkt.trim();
        if (trimmed.startsWith("<")) {
            int end = trimmed.indexOf('>');
            if (end != -1) {
                return trimmed.substring(end + 1).trim();
            }
        }
        return trimmed;
    }

    public static List<Polygon> wktToPolygons(String wkt) throws Exception {
        if (wkt == null || wkt.trim().isEmpty()) {
            return Collections.emptyList();
        }
        return toPolygons(WKT_READER.read(stripCrs(wkt)));
    }

    /**
     * The polygonal parts of an already-parsed geometry at ANY depth (see
     * {@link #wktToPolygons}): a MULTIPOLYGON nested inside a
     * GEOMETRYCOLLECTION, or a collection inside a collection, contributes
     * every polygon it holds. The one-level walk this replaced saw a nested
     * MultiPolygon as neither a Polygon nor a leaf and dropped it (BG-371).
     */
    public static List<Polygon> toPolygons(Geometry geom) {
        if (geom.isEmpty()) {
            return Collections.emptyList();
        }
        List<Polygon> result = new ArrayList<>();
        collectPolygons(geom, result);
        return result;
    }

    private static void collectPolygons(Geometry g, List<Polygon> out) {
        if (g instanceof Polygon p) {
            // POLYGON EMPTY as a MULTIPOLYGON / GEOMETRYCOLLECTION member: no
            // rings, a null envelope - nothing to render or index (BG-375).
            if (!p.isEmpty()) {
                out.add(p);
            }
            return;
        }
        for (int i = 0; i < g.getNumGeometries(); i++) {
            collectPolygons(g.getGeometryN(i), out);
        }
    }

    /**
     * Everything the spatial index must cover for a geometry, as polygons:
     * every polygon leaf (any depth), plus the expanded envelope of every
     * NON-polygonal leaf (POINT, LINESTRING, ... - again at any depth, so the
     * members of a MULTIPOINT or of a nested collection count individually).
     * Empty leaves contribute nothing. THE walk both the RAM builder and the
     * disk pipeline index through ({@code SpatialAugmenter.addSpatial}), so
     * the engines cannot drift (BG-371).
     */
    public static List<Polygon> spatialParts(Geometry geom) {
        List<Polygon> parts = new ArrayList<>();
        if (geom.isEmpty()) {
            return parts;
        }
        collectSpatialParts(geom, parts, new GeometryFactory());
        return parts;
    }

    private static void collectSpatialParts(Geometry g, List<Polygon> out, GeometryFactory gf) {
        if (g instanceof Polygon p) {
            // An empty member's null envelope floors to Long.MIN_VALUE, clamps
            // to 0 and used to emit a spurious hilbertCell0 = 0 entry (BG-375).
            if (!p.isEmpty()) {
                out.add(p);
            }
            return;
        }
        if (g instanceof GeometryCollection) {
            for (int i = 0; i < g.getNumGeometries(); i++) {
                collectSpatialParts(g.getGeometryN(i), out, gf);
            }
            return;
        }
        if (!g.isEmpty()) {
            Envelope env = g.getEnvelopeInternal();
            env.expandBy(0.5);
            out.add((Polygon) gf.toGeometry(env));
        }
    }

}