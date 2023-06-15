package com.ebremer.beakgraph.ng;

/**
 *
 * @author erich
 */
public class Statistics {
    private static Statistics statistics = null;
    
    private Statistics() {
        
    }
    
    public static Statistics getStatistics() {
        if (statistics==null) {
            statistics = new Statistics();
        }
        return statistics;
    }
    
}
