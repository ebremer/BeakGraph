package com.ebremer.beakgraph;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.geom.Path2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A utility class to parse WKT Polygons and render them to an image file.
 */
public class WktPolygonDrawer {

    public static void main(String[] args) {
        // 1. Setup Sample Data
        ArrayList<String> wktList = new ArrayList<>();
        // A square
        wktList.add("POLYGON ((0 0, 0 100, 100 100, 100 0, 0 0))");
        // A triangle inside
        wktList.add("POLYGON ((20 20, 50 80, 80 20, 20 20))");
        // An overlapping shape
        wktList.add("POLYGON ((80 80, 80 120, 120 120, 120 80, 80 80))");

        // 2. Define Output configuration
        String outputPath = "output_polygons.png";
        int width = 800;
        int height = 600;
        int padding = 50; // Pixels of padding around the shapes

        try {
            System.out.println("Processing " + wktList.size() + " polygons...");
            drawWktList(wktList, width, height, padding, outputPath);
            System.out.println("Success! Image saved to: " + new File(outputPath).getAbsolutePath());
        } catch (Exception e) {
            System.err.println("Error generating image: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * core method to parse WKT, calculate bounds, and draw to image.
     * @param wktList
     * @param imgWidth
     * @param imgHeight
     * @param padding
     * @param filePath
     * @throws org.locationtech.jts.io.ParseException
     */
    public static void drawWktList(List<String> wktList, int imgWidth, int imgHeight, int padding, String filePath) throws ParseException, IOException {
        WKTReader reader = new WKTReader();
        List<Geometry> geometries = new ArrayList<>();
        Envelope globalBounds = new Envelope();

        // 1. Parse all WKTs and calculate the global bounding box (Envelope)
        for (String wkt : wktList) {
            Geometry geom = reader.read(wkt);
            geometries.add(geom);
            // Expand global bounds to include this geometry
            globalBounds.expandToInclude(geom.getEnvelopeInternal());
        }

        // Handle empty list case
        if (geometries.isEmpty()) {
            throw new IllegalArgumentException("WKT List is empty");
        }

        // 2. Create Buffered Image
        BufferedImage image = new BufferedImage(imgWidth, imgHeight, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g2d = image.createGraphics();

        // Enable Anti-aliasing for smooth lines
        g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);

        // Fill background (White)
        g2d.setColor(Color.WHITE);
        g2d.fillRect(0, 0, imgWidth, imgHeight);

        // 3. Calculate Scale and Transformation
        // We need to map World Coordinates (WKT) to Screen Coordinates (Pixels)
        double worldWidth = globalBounds.getWidth();
        double worldHeight = globalBounds.getHeight();
        
        // Avoid division by zero if all points are identical
        if(worldWidth == 0) worldWidth = 1;
        if(worldHeight == 0) worldHeight = 1;

        // Calculate available drawing area (accounting for padding)
        double drawWidth = imgWidth - (2 * padding);
        double drawHeight = imgHeight - (2 * padding);

        // Determine scale (pixels per map unit) - keep aspect ratio
        double scaleX = drawWidth / worldWidth;
        double scaleY = drawHeight / worldHeight;
        double scale = Math.min(scaleX, scaleY);

        // 4. Draw Each Geometry
        // Set stroke style
        g2d.setStroke(new BasicStroke(2));

        for (int i = 0; i < geometries.size(); i++) {
            Geometry geom = geometries.get(i);
            
            // Convert JTS Geometry to Java AWT Shape (Path2D)
            Path2D path = new Path2D.Double();
            Coordinate[] coords = geom.getCoordinates();

            for (int k = 0; k < coords.length; k++) {
                Coordinate c = coords[k];

                // Transform logic:
                // 1. Translate point relative to minX/minY
                // 2. Multiply by scale
                // 3. Add padding offset
                // 4. INVERT Y-AXIS (Computer graphics Y starts at top, Map Y starts at bottom)
                
                double pixelX = padding + ((c.x - globalBounds.getMinX()) * scale);
                double pixelY = (imgHeight - padding) - ((c.y - globalBounds.getMinY()) * scale);

                if (k == 0) {
                    path.moveTo(pixelX, pixelY);
                } else {
                    path.lineTo(pixelX, pixelY);
                }
            }
            
            // Close the path if it's a polygon
            if (geom instanceof Polygon) {
                path.closePath();
            }

            // Draw Fill (Optional - Transparent Blue)
            g2d.setColor(new Color(0, 100, 255, 50)); 
            g2d.fill(path);

            // Draw Border (Solid Blue)
            g2d.setColor(Color.BLUE);
            g2d.draw(path);
        }

        g2d.dispose();

        // 5. Write to File
        ImageIO.write(image, "png", new File(filePath));
    }
}
