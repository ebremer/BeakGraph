package com.ebremer.beakgraph.utils;

import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import java.awt.*;
import java.awt.geom.Path2D;
import java.awt.image.BufferedImage;
import java.util.ArrayList;

/**
 * Utility class for rendering JTS geometries (WKT polygons) onto BufferedImage.
 *
 * @author erich
 */
public class ImageTools {

    /**
     * Draws a collection of WKT polygons (Polygon or MultiPolygon) onto a BufferedImage.
     *
     * @param wktList     List of WKT strings (Polygon or MultiPolygon)
     * @param image       The BufferedImage to draw on (modified in-place)
     * @param envelope    Real-world envelope mapped to the entire image
     * @param fillColor   Fill color (with alpha for transparency); null = no fill
     * @param strokeColor Outline color; null = no outline
     * @throws Exception if any WKT cannot be parsed
     */
    public static void drawWktPolygonsOnImage(ArrayList<String> wktList,
                                              BufferedImage image,
                                              Envelope envelope,
                                              Color fillColor,
                                              Color strokeColor) throws Exception {

        if (wktList == null || wktList.isEmpty() || image == null || envelope == null) {
            return;
        }

        Graphics2D g2d = image.createGraphics();
        try {
            g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
            g2d.setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY);

            int width  = image.getWidth();
            int height = image.getHeight();

            double scaleX  = width  / envelope.getWidth();
            double scaleY  = height / envelope.getHeight();
            double offsetX = -envelope.getMinX() * scaleX;
            double offsetY =  envelope.getMaxY() * scaleY;   // Y flipped

            WKTReader reader = new WKTReader();

            for (String wkt : wktList) {
                if (wkt == null || wkt.trim().isEmpty()) {
                    continue;
                }

                Geometry geom = reader.read(wkt.trim());

                // Support Polygon and MultiPolygon
                for (int i = 0; i < geom.getNumGeometries(); i++) {
                    Geometry part = geom.getGeometryN(i);

                    // Explicitly refer to JTS Polygon to avoid ambiguity with java.awt.Polygon
                    if (!(part instanceof org.locationtech.jts.geom.Polygon jtsPolygon)) {
                        continue;   // skip non-polygon parts
                    }

                    // Exterior ring
                    Shape exterior = coordinateSequenceToShape(
                            jtsPolygon.getExteriorRing().getCoordinateSequence(),
                            scaleX, scaleY, offsetX, offsetY);

                    // Build path with holes
                    Path2D.Double path = new Path2D.Double();
                    path.append(exterior, false);

                    for (int h = 0; h < jtsPolygon.getNumInteriorRing(); h++) {
                        Shape hole = coordinateSequenceToShape(
                                jtsPolygon.getInteriorRingN(h).getCoordinateSequence(),
                                scaleX, scaleY, offsetX, offsetY);
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
     * Converts a JTS CoordinateSequence to an AWT Shape using pixel coordinates.
     */
    private static Shape coordinateSequenceToShape(CoordinateSequence seq,
                                                   double scaleX, double scaleY,
                                                   double offsetX, double offsetY) {
        Path2D.Double path = new Path2D.Double();
        if (seq.size() == 0) {
            return path;
        }

        // First point
        org.locationtech.jts.geom.Coordinate c = seq.getCoordinate(0);
        double x = c.x * scaleX + offsetX;
        double y = offsetY - c.y * scaleY;   // flip Y
        path.moveTo(x, y);

        // Remaining points
        for (int i = 1; i < seq.size(); i++) {
            c = seq.getCoordinate(i);
            x = c.x * scaleX + offsetX;
            y = offsetY - c.y * scaleY;
            path.lineTo(x, y);
        }

        path.closePath();
        return path;
    }
}