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

    public static void drawPolygonsOnImage(List<Polygon> polygons, BufferedImage image, Color strokeColor, int offX, int offY) {
        if (polygons == null || polygons.isEmpty() || image == null) {
            return;
        }
        Graphics2D g2d = image.createGraphics();
        try {
            if (strokeColor != null) {
                g2d.setColor(strokeColor);
                g2d.setStroke(new BasicStroke(1f));
            }
            for (Polygon polygon : polygons) {
                if (polygon == null || polygon.isEmpty()) {
                    continue;
                }
                Shape exterior = coordinateSequenceToShape(polygon.getExteriorRing().getCoordinateSequence(), offX, offY);
                Path2D.Double path = new Path2D.Double();
                path.append(exterior, false);
                for (int h = 0; h < polygon.getNumInteriorRing(); h++) {
                    Shape hole = coordinateSequenceToShape(polygon.getInteriorRingN(h).getCoordinateSequence(), offX, offY);
                    path.append(hole, false);
                }
                if (strokeColor != null) {
                    g2d.draw(path);
                }
            }
        } finally {
            g2d.dispose();
        }
    }

    public static void drawWktPolygonsOnImage(List<String> wktList, BufferedImage image, Color strokeColor, int offX, int offY) throws Exception {
        if (wktList == null || wktList.isEmpty() || image == null) {
            return;
        }
        Graphics2D g2d = image.createGraphics();
        try {
            g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
            g2d.setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY);
            if (strokeColor != null) {
                g2d.setColor(strokeColor);
                g2d.setStroke(new BasicStroke(2f));
            }
            for (String wkt : wktList) {
                List<Polygon> polygons = wktToPolygons(wkt);
                for (Polygon p : polygons) {
                    Shape exterior = coordinateSequenceToShape(p.getExteriorRing().getCoordinateSequence(), offX, offY);
                    Path2D.Double path = new Path2D.Double();
                    path.append(exterior, false);
                    for (int h = 0; h < p.getNumInteriorRing(); h++) {
                        path.append(coordinateSequenceToShape(p.getInteriorRingN(h).getCoordinateSequence(), offX, offY), false);
                    }
                    if (strokeColor != null) {
                        g2d.draw(path);
                    }
                }
            }
        } finally {
            g2d.dispose();
        }
    }

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
        Geometry geom = WKT_READER.read(stripCrs(wkt));
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

    private static Shape coordinateSequenceToShape(CoordinateSequence seq, int offX, int offY) {
        Path2D.Double path = new Path2D.Double();
        if (seq.size() == 0) {
            return path;
        }
        org.locationtech.jts.geom.Coordinate c = seq.getCoordinate(0);
        path.moveTo(c.x - offX, c.y - offY);
        for (int i = 1; i < seq.size(); i++) {
            c = seq.getCoordinate(i);
            path.lineTo(c.x - offX, c.y - offY);
        }
        path.closePath();
        return path;
    }
}