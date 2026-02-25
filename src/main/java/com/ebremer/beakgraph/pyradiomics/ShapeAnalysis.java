package com.ebremer.beakgraph.pyradiomics;
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
  
    public static BufferedImage getTestBI() {
        int size = 1000;
        BufferedImage bi = new BufferedImage(size,size, BufferedImage.TYPE_INT_RGB);
        int offset = 250;
        int rot = 25;
        int major = 300;
        int minor = 300;
        Graphics2D g = bi.createGraphics();
        g.setColor(Color.black);
        g.fillRect(0, 0, size, size);
        g.setColor(Color.BLUE);
        Ellipse2D.Double oval = new Ellipse2D.Double(offset, offset, minor, major);
        g.rotate(Math.toRadians(rot),offset+minor/2,offset+major/2);
        g.fill(oval);
        return bi;
    }
  
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
            c = ((bi.getRGB(a+1, b) & 0xFF)>0)?1:0;
            c = c + (((bi.getRGB(a-1, b) & 0xFF)>0)?1:0);
            c = c + (((bi.getRGB(a, b+1) & 0xFF)>0)?1:0);
            c = c + (((bi.getRGB(a, b-1) & 0xFF)>0)?1:0);
            return (c!=4);
        }
        return false;
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
  
    public static double getArea(Polygon p) { return p.getArea(); }
  
    public static double getPerimeter(Polygon p) { return p.getLength(); }
  
    public static double getPerimeterSurfaceRatioFeatureValue(Polygon p) {
        return getPerimeter(p)/getArea(p);
    }
    public static double getSphericityFeatureValue(Polygon p) {
        return 2*Math.sqrt(Math.PI*getArea(p))/getPerimeter(p);
    }
    public static double getSphericalDisproportionFeatureValue(Polygon p) {
        return 1.0d/getSphericityFeatureValue(p);
    }
  
    public static double getMeshSurfaceFeatureValue(Polygon p) {
        return p.getArea();
    }
  
    public static double getMaximum2DDiameterFeatureValue(Polygon p) {
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
        double det = a*c - b*b;
        double disc = Math.sqrt(trace*trace - 4*det);
        double l1 = (trace + disc)/2;
        double l2 = (trace - disc)/2;
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
  
    /**
     * Returns major/minor axis unit vectors and signed rotation (radians) of major axis
     * relative to reference vector (e.g. up = {0, -1}).
     * @param points
     * @param refX
     * @param refY
     * @return {majorX, majorY, minorX, minorY, rotationRadians}
     */
    public static double[] getPrincipalAxes(SimpleMatrix points, double refX, double refY) {
        int n = points.getNumRows();
        if (n < 2) return new double[]{0,0,0,0,0};
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
        double det = a*c - b*b;
        double disc = Math.sqrt(trace*trace - 4*det);
        double l1 = (trace + disc)/2;
        double l2 = (trace - disc)/2;
        if (l1 < l2) { double t = l1; l1 = l2; l2 = t; }
        double majX, majY, minX, minY;
        if (Math.abs(b) < 1e-12) {
            if (a > c) { majX=1; majY=0; minX=0; minY=1; }
            else { majX=0; majY=1; minX=1; minY=0; }
        } else {
            majX = b; majY = l1 - a;
            double n1 = Math.sqrt(majX*majX + majY*majY);
            majX /= n1; majY /= n1;
            minX = b; minY = l2 - a;
            double n2 = Math.sqrt(minX*minX + minY*minY);
            minX /= n2; minY /= n2;
        }
        double majAngle = Math.atan2(majY, majX);
        double refAngle = Math.atan2(refY, refX);
        double rotation = majAngle - refAngle;
        while (rotation > Math.PI) rotation -= 2*Math.PI;
        while (rotation < -Math.PI) rotation += 2*Math.PI;
        return new double[]{majX, majY, minX, minY, rotation};
    }
  
    public static BufferedImage getBufferedImage(Polygon p) {
        Geometry bb = p.getEnvelope();
        Coordinate[] c = bb.getCoordinates();
        int width = (int) Math.round(c[2].x-c[0].x);
        int height = (int) Math.round(c[2].y - c[0].y);
        AffineTransformation af = new AffineTransformation();
        af.setToTranslation(-c[0].x, -c[0].y);
        af.transform(p);
        BufferedImage bi = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        ShapeWriter sw = new ShapeWriter();
        Shape s = sw.toShape(p);
        Graphics2D g = bi.createGraphics();
        g.draw(s);
        return bi;
    }
}
