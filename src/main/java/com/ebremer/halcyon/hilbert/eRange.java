package com.ebremer.halcyon.hilbert;

/**
 *
 * @author erich
 */
public class eRange implements Comparable<eRange> {
    private final long low;
    private final long high;

    public eRange(long low, long high) {
        this.low = Math.min(low, high);
        this.high = Math.max(low, high);
    }
    
    @Override
    public int compareTo(eRange candidate) {          
        return (this.low < candidate.low() ? -1 : 
            (this.low == candidate.low() ? 0 : 1));     
    }       
    
    public static eRange create(long low, long high) {
        return new eRange(low, high);
    }

    public static eRange create(long value) {
        return new eRange(value, value);
    }

    public long low() {
        return low;
    }

    public long high() {
        return high;
    }

    public boolean contains(long value) {
        return low <= value && value <= high;
    }

    @Override
    public String toString() {
        return "Range [low=" + low + ", high=" + high + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (high ^ (high >>> 32));
        result = prime * result + (int) (low ^ (low >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        eRange other = (eRange) obj;
        if (high != other.high)
            return false;
        return low == other.low;
    }

    public eRange join(eRange range) {
        return eRange.create(Math.min(low, range.low), Math.max(high, range.high));
    }
    
}
