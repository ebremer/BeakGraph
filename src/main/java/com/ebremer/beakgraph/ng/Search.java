package com.ebremer.beakgraph.ng;

import org.apache.arrow.vector.VarCharVector;

/**
 *
 * @author erich
 */
public class Search {
    
    public static int Find(VarCharVector varCharVector, String target) {
       // System.out.println("Find ("+target+") ====> "+varCharVector);
        int low = 0;
        int high = varCharVector.getValueCount() - 1;
        while (low <= high) {
            int mid = (low + high) / 2;
            String currentVal = new String(varCharVector.get(mid));
            if (currentVal.equals(target)) {
                return mid;
            }
            if (currentVal.compareTo(target) < 0) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return -1;
    }
}
