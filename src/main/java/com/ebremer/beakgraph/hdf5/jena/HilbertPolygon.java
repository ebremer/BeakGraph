package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.halcyon.hilbert.HilbertSpace;
import static com.ebremer.halcyon.hilbert.HilbertSpace.hc;
import java.util.ArrayList;
import org.davidmoten.hilbert.Range;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

public class HilbertPolygon {
    private static final GeometryFactory gf = new GeometryFactory();

    public static ArrayList<Range> Polygon2Hilbert(String wkt) {
        Polygon poly = HilbertSpace.fromWkt(wkt);
        return Polygon2Hilbert(poly, 0);
    }

    public static ArrayList<Range> Polygon2Hilbert(String wkt, int scale) {
        Polygon poly = HilbertSpace.fromWkt(wkt);
        return Polygon2Hilbert(poly, scale);
    }
    
    public static ArrayList<Range> Polygon2Hilbert(Polygon poly) {
        return Polygon2Hilbert(poly, 0);
    }

    /**
     * Converts a Polygon to a list of Hilbert Ranges at a specific scale.
     * @param poly The JTS Polygon
     * @param scale The downsampling scale. 0 = full size, 1 = 1/2 dim (1/4 area), 2 = 1/4 dim (1/16 area), etc.
     * @return Compressed ArrayList of Hilbert Ranges
     */
    public static ArrayList<Range> Polygon2Hilbert(Polygon poly, int scale) {
        ArrayList<Range> rah = new ArrayList<>();
        Envelope env = poly.getEnvelopeInternal();
        int minx = (int) env.getMinX() >> scale;
        int maxx = (int) env.getMaxX() >> scale;
        int miny = (int) env.getMinY() >> scale;
        int maxy = (int) env.getMaxY() >> scale;
        ArrayList<Long> pp = new ArrayList<>();
        for (int x = minx; x <= maxx; x++) {
            for (int y = miny; y <= maxy; y++) {
                double checkX = x << scale;
                double checkY = y << scale;                
                Point pt = gf.createPoint(new Coordinate(checkX, checkY));                
                if (poly.covers(pt)) {
                    pp.add(hc.index(x, y));
                }
            }   
        }
        pp.sort(null);
        if (pp.isEmpty()) {
            return rah;
        }
        long sv = pp.get(0);
        long ev = pp.get(0);
        for (int i = 1; i < pp.size(); i++) {
            long nv = pp.get(i);
            if ((nv - ev) == 1) {
                ev = nv;
            } else {
                rah.add(new Range(sv, ev));
                sv = nv;
                ev = nv;
            }
        }
        rah.add(new Range(sv, ev));
        return rah;
    }
}
