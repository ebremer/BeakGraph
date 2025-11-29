package com.ebremer.beakgraph.utils;

import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import java.awt.*;
import java.awt.geom.Path2D;
import java.awt.image.BufferedImage;
import java.util.ArrayList;

/**
 * Utility class for rendering JTS geometries (WKT polygons) that are already expressed
 * in image pixel coordinates onto a BufferedImage.
 *
 * @author erich
 */
public class ImageTools {

    /**
     * Draws a collection of WKT polygons (Polygon or MultiPolygon) directly onto a BufferedImage.
     * Coordinates in the WKT strings are treated as pixel coordinates (origin top-left).
     *
     * @param wktList     List of WKT strings (Polygon or MultiPolygon)
     * @param image       The BufferedImage to draw on (modified in-place)
     * @param fillColor   Fill color (with alpha for transparency); null = no fill
     * @param strokeColor Outline color; null = no outline
     * @throws Exception if any WKT cannot be parsed
     */
    public static void drawWktPolygonsOnImage(ArrayList<String> wktList,
                                              BufferedImage image,
                                              Color fillColor,
                                              Color strokeColor) throws Exception {

        if (wktList == null || wktList.isEmpty() || image == null) {
            return;
        }

        Graphics2D g2d = image.createGraphics();
        try {
            g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
            g2d.setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY);

            WKTReader reader = new WKTReader();

            for (String wkt : wktList) {
                if (wkt == null || wkt.trim().isEmpty()) {
                    continue;
                }

                Geometry geom = reader.read(wkt.trim());

                // Support Polygon and MultiPolygon
                for (int i = 0; i < geom.getNumGeometries(); i++) {
                    Geometry part = geom.getGeometryN(i);

                    if (!(part instanceof org.locationtech.jts.geom.Polygon polygon)) {
                        continue; // skip non-polygon geometries
                    }

                    // Exterior ring
                    Shape exterior = coordinateSequenceToShape(polygon.getExteriorRing().getCoordinateSequence());

                    // Build path including holes
                    Path2D.Double path = new Path2D.Double();
                    path.append(exterior, false);

                    for (int h = 0; h < polygon.getNumInteriorRing(); h++) {
                        Shape hole = coordinateSequenceToShape(polygon.getInteriorRingN(h).getCoordinateSequence());
                        path.append(hole, false);
                    }

                    // Fill
                    if (fillColor != null) {
                        g2d.setColor(fillColor);
                        g2d.fill(path);
                    }

                    // Outline
                    if (strokeColor != null) {
                        g2d.setColor(strokeColor);
                        g2d.setStroke(new BasicStroke(2f));
                        g2d.draw(path);
                    }
                }
            }
        } finally {
            g2d.dispose();
        }
    }

    /**
     * Converts a JTS CoordinateSequence directly to an AWT Shape.
     * Coordinates are used as-is (origin top-left, Y positive downward).
     */
    private static Shape coordinateSequenceToShape(CoordinateSequence seq) {
        Path2D.Double path = new Path2D.Double();

        if (seq.size() == 0) {
            return path;
        }

        // First point
        org.locationtech.jts.geom.Coordinate c = seq.getCoordinate(0);
        path.moveTo(c.x, c.y);

        // Remaining points
        for (int i = 1; i < seq.size(); i++) {
            c = seq.getCoordinate(i);
            path.lineTo(c.x, c.y);
        }

        path.closePath();
        return path;
    }
}