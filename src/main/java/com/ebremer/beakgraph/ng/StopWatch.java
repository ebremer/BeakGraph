package com.ebremer.beakgraph.ng;

/**
 *
 * @author erich
 */
public class StopWatch {
    private long begin;
    
    private StopWatch() {
        begin = System.nanoTime();
    }
    
    public static StopWatch getInstance() {
        return new StopWatch();
    }
    
    public StopWatch reset() {
        begin = System.nanoTime();
        return this;
    }
    
    public StopWatch Lapse(String msg) {
        double end = System.nanoTime() - begin;
        end = end / 1000000d;
        System.out.println(msg+" --> "+end);
        return this;
    }
}
