package com.ebremer.halcyon.hilbert;

import com.ebremer.halcyon.geometry.Point;
import java.util.HashMap;

/**
 *
 * @author erich
 */
public class Visits {
    private final HashMap<String,Integer> visits;
    
    public Visits() {
        visits = new HashMap<>();
    }
    
    public void visited(Point p) {
        String key = p.x+"#"+p.y;
        if (!visits.containsKey(key)) {
            visits.put(key, 1);
        } else {
            int count = visits.get(key);
            count++;
            visits.put(key, count);
        }
    }
    
    public int getNumVisits(Point p) {
        String key = p.x+"#"+p.y;
        if (!visits.containsKey(key)) {
            return 0;
        }
        return visits.get(key);
    }
    
    public static void main(String[] args) {
        Visits ha = new Visits();
        Point a = new Point(122,213);
        Point b = new Point(122,213);
        Point c = new Point(22,113);
        ha.visited(a);
        ha.visited(a);
        ha.visited(a);
        ha.visited(a);
        ha.visited(a);
        ha.visited(b);
        ha.visited(b);
        ha.visited(b);
        ha.visited(b);
        
    }
}
