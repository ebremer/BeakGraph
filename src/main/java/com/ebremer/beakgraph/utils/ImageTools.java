package com.ebremer.beakgraph.utils;

import org.locationtech.jts.geom.Geometry;
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

    /** The polygonal parts of an already-parsed geometry (see {@link #wktToPolygons}). */
    public static List<Polygon> toPolygons(Geometry geom) {
        if (geom.isEmpty()) {
            return Collections.emptyList();
        }
        List<Polygon> result = new ArrayList<>();
        if (geom instanceof Polygon polygon) {
            result.add(polygon);
        } else {
            for (int i = 0; i < geom.getNumGeometries(); i++) {
                Geometry part = geom.getGeometryN(i);
                if (part instanceof Polygon p) {
                    result.add(p);
                }
            }
        }
        return result;
    }

}