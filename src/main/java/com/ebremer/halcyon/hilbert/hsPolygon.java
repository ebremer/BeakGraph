package com.ebremer.halcyon.hilbert;

import java.util.List;
import org.davidmoten.hilbert.Range;
import org.davidmoten.hilbert.Ranges;

/**
 *
 * @author erich
 */
public class hsPolygon {
    List<Range> ranges;
    private long min;
    private long max;
    
    public hsPolygon(Ranges r) {
        ranges = r.toList();
        //FindMixMax();
    }
    
    private long getMin() {
        return min;
    }

    private long getMax() {
        return max;
    }
    
    private void FindMixMax() {
        min = Long.MAX_VALUE;
        max = Long.MIN_VALUE;
        for (Range r : ranges) {
            min = Math.min(min,r.low());
            max = Math.max(max,r.high());
        }
    }
    
    public List<Range> getRanges() {
        return ranges;
    }
    
}
