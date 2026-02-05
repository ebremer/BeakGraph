package com.ebremer.beakgraph.pool;

import com.ebremer.beakgraph.core.BeakGraph;
import java.time.Duration;

/**
 *
 * @author erich
 */
public class BeakGraphPool {   
    
    private BeakGraphPool() {}

    private static class Holder {
        private static final BeakGraphKeyedPool INSTANCE;
        static {
            BeakGraphKeyedPoolConfig<BeakGraph> config = new BeakGraphKeyedPoolConfig<>();
            config.setMaxTotalPerKey(8);
            config.setMaxTotal(20);
            config.setMinIdlePerKey(0);
            config.setTestOnBorrow(true);
            config.setMaxWait(Duration.ofMillis(60000));
            config.setBlockWhenExhausted(true);
            config.setMinEvictableIdleDuration(Duration.ofMinutes(5));
            config.setTimeBetweenEvictionRuns(Duration.ofMinutes(1));
            INSTANCE = new BeakGraphKeyedPool(new BeakGraphPoolFactory(), config);
        }
    }

    public static BeakGraphKeyedPool getPool() {
        return Holder.INSTANCE;
    }
}
