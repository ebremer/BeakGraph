package com.ebremer.beakgraph.features;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Shape;
import java.awt.geom.Ellipse2D;
import java.awt.image.BufferedImage;
import org.locationtech.jts.awt.ShapeWriter;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.ejml.simple.SimpleMatrix;
/**
 *
 * @author erich
 */
public class ShapeAnalysis {
    private static final GeometryFactory gf = new GeometryFactory();
  
  
    public static int Area(BufferedImage bi) {
        int count = 0;
        for (int i=0; i<bi.getWidth(); i++) {
            for (int j=0; j<bi.getHeight(); j++) {
                int c = bi.getRGB(i, j) & 0xFF;
                if (c>0) count++;
            }
        }
        return count;
    }
  
    public static boolean isEdge(BufferedImage bi, int a, int b) {
        int c = bi.getRGB(a, b) & 0xFF;
        if (c>0) {
            // Out-of-bounds neighbours count as background, so a filled pixel on
            // the image border is an edge instead of an ArrayIndexOutOfBounds.
            int n = filled(bi, a+1, b) + filled(bi, a-1, b) + filled(bi, a, b+1) + filled(bi, a, b-1);
            return (n!=4);
        }
        return false;
    }

    private static int filled(BufferedImage bi, int x, int y) {
        if (x < 0 || y < 0 || x >= bi.getWidth() || y >= bi.getHeight()) {
            return 0;
        }
        return ((bi.getRGB(x, y) & 0xFF) > 0) ? 1 : 0;
    }
  
    public static int Circumference(BufferedImage bi) {
        int count = 0;
        for (int i=0; i<bi.getWidth(); i++) {
            for (int j=0; j<bi.getHeight(); j++) {
                if (isEdge(bi,i,j)) count++;
            }
        }
        return count;
    }
  
    public static SimpleMatrix BufferedImage2INDArray(BufferedImage bi) {
        int area = Area(bi);
        SimpleMatrix m = new SimpleMatrix(area, 2);
        int h=0;
        for (int i=0; i<bi.getWidth(); i++) {
            for (int j=0; j<bi.getHeight(); j++) {
                int c = bi.getRGB(i, j) & 0xFF;
                if (c>0) {
                    m.set(h, 0, i);
                    m.set(h, 1, j);
                    h++;
                }
            }
        }
        return m;
    }
  
    public static double getArea(Geometry p) { return p.getArea(); }
  
    public static double getPerimeter(Geometry p) { return p.getLength(); }
  
    public static double getPerimeterSurfaceRatioFeatureValue(Geometry p) {
        return getPerimeter(p)/getArea(p);
    }
    public static double getSphericityFeatureValue(Geometry p) {
        return 2*Math.sqrt(Math.PI*getArea(p))/getPerimeter(p);
    }
    public static double getSphericalDisproportionFeatureValue(Geometry p) {
        return 1.0d/getSphericityFeatureValue(p);
    }
  
    public static double getMeshSurfaceFeatureValue(Geometry p) {
        return p.getArea();
    }
  
    public static double getMaximum2DDiameterFeatureValue(Geometry p) {
        Geometry ch = p.convexHull();
        Coordinate[] coords = ch.getCoordinates();
        double maxD = 0;
        for (int i=0; i<coords.length; i++) {
            for (int j=i+1; j<coords.length; j++) {
                double dx = coords[i].x - coords[j].x;
                double dy = coords[i].y - coords[j].y;
                double d = Math.sqrt(dx*dx + dy*dy);
                if (d > maxD) maxD = d;
            }
        }
        return maxD;
    }
  
    private static double[] getEigenvalues(SimpleMatrix points) {
        int n = points.getNumRows();
        if (n < 2) return new double[]{0,0};
        double meanX = 0, meanY = 0;
        for(int i=0; i<n; i++) {
            meanX += points.get(i,0);
            meanY += points.get(i,1);
        }
        meanX /= n;
        meanY /= n;
        double sxx = 0, sxy=0, syy=0;
        for(int i=0; i<n; i++) {
            double dx = points.get(i,0) - meanX;
            double dy = points.get(i,1) - meanY;
            sxx += dx*dx;
            sxy += dx*dy;
            syy += dy*dy;
        }
        double a = sxx / (n-1);
        double b = sxy / (n-1);
        double c = syy / (n-1);
        double trace = a + c;
        // hypot(a - c, 2b) is sqrt(trace^2 - 4 det) without the catastrophic
        // cancellation that turned a near-isotropic, large region's discriminant
        // slightly negative (NaN axes); the eigenvalues are clamped at zero too.
        double disc = Math.hypot(a - c, 2 * b);
        double l1 = Math.max(0, (trace + disc) / 2);
        double l2 = Math.max(0, (trace - disc) / 2);
        return l1 > l2 ? new double[]{l1, l2} : new double[]{l2, l1};
    }
  
    public static double getMajorAxisLengthFeatureValue(SimpleMatrix points) {
        double[] lambda = getEigenvalues(points);
        return 4 * Math.sqrt(lambda[0]);
    }
  
    public static double getMinorAxisLengthFeatureValue(SimpleMatrix points) {
        double[] lambda = getEigenvalues(points);
        return 4 * Math.sqrt(lambda[1]);
    }
  
    public static double getElongationFeatureValue(SimpleMatrix points) {
        double maj = getMajorAxisLengthFeatureValue(points);
        double min = getMinorAxisLengthFeatureValue(points);
        return maj > 0 ? min / maj : 0;
    }
  
    public static double getPixelSurfaceFeatureValue(BufferedImage bi) {
        return Area(bi);
    }
  
  
    /** Pixel budget for one geometry's raster; tunable via beakgraph.features.maxRasterPixels. */
    public static final long MAX_RASTER_PIXELS = Long.getLong("beakgraph.features.maxRasterPixels", 4_000_000L);

    /** A rasterized region and the factor its coordinates were scaled by (1 = unscaled). */
    public record Raster(BufferedImage image, double scale) {}

    /**
     * Rasterizes the polygon's region into an image no larger than
     * {@link #MAX_RASTER_PIXELS}. Whole-slide annotations run to tens of
     * thousands of pixels a side; an unbounded raster (3.6 GB for 30k x 30k,
     * plus 16 bytes per lit pixel for the point matrix) took the build down
     * with an OutOfMemoryError. Over-budget geometry is drawn at a uniform
     * scale factor, which pixel-derived features undo ({@code PixelSurface /
     * scale^2}, axis lengths {@code / scale}; Elongation is scale-invariant).
     */
    public static Raster getRaster(Geometry p) {
        // Dimensions come from the Envelope, not getEnvelope()'s coordinate array:
        // a degenerate (point/line) envelope has fewer than 3 coordinates (the old
        // c[2] access threw), and a valid polygon thinner than ~0.5 units rounded
        // to a 0-sized image (BufferedImage rejects it) - either way ALL features
        // were skipped, including the purely polygon-based ones. Clamping to 1px
        // keeps the raster features defined and the polygon features exact.
        org.locationtech.jts.geom.Envelope env = p.getEnvelopeInternal();
        double w = Math.max(1.0, env.getWidth());
        double h = Math.max(1.0, env.getHeight());
        double scale = Math.min(1.0, Math.sqrt(MAX_RASTER_PIXELS / (w * h)));
        int width = Math.max(1, (int) Math.round(w * scale));
        int height = Math.max(1, (int) Math.round(h * scale));
        AffineTransformation af = new AffineTransformation();
        af.setToTranslation(-env.getMinX(), -env.getMinY());
        if (scale < 1.0) {
            af.scale(scale, scale);
        }
        // transform() returns a translated copy (it does not mutate p, which the caller
        // still uses for its other feature calcs). Draw that copy, shifted so the polygon's
        // bounding-box corner sits at (0,0) and lands inside the width x height image -
        // drawing the original p would place it off-canvas, leaving the image blank.
        Geometry moved = af.transform(p);
        BufferedImage bi = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        ShapeWriter sw = new ShapeWriter();
        Shape s = sw.toShape(moved);
        Graphics2D g = bi.createGraphics();
        // Fill (not stroke) so the raster represents the polygon's REGION: PixelSurface counts lit
        // pixels as the area and the PCA axis features sample the filled region - stroking would
        // measure the perimeter instead. (createGraphics() defaults to a white foreground, so filled
        // pixels register as lit in Area().)
        g.fill(s);
        return new Raster(bi, scale);
    }

    /** The raster of {@link #getRaster}, without its scale; pixel counts are in raster pixels. */
    public static BufferedImage getBufferedImage(Geometry p) {
        return getRaster(p).image();
    }
}
