package com.ebremer.beakgraph.pool;

import com.ebremer.beakgraph.core.BeakGraph;
import java.net.URI;
import java.time.Duration;

/**
 *
 * @author erich
 */
public class BeakGraphPool {
    private static BeakGraphKeyedPool<URI,BeakGraph> pool;
    
    private BeakGraphPool() {}
    
    public static synchronized BeakGraphKeyedPool<URI, BeakGraph> getPool() {
        if (pool == null) {
            BeakGraphKeyedPoolConfig config = new BeakGraphKeyedPoolConfig<>();
            //config.setMaxTotalPerKey(Runtime.getRuntime().availableProcessors());
            config.setMaxTotalPerKey(100);
            config.setMinIdlePerKey(0);
            config.setMaxWait(Duration.ofMillis(60000));
            config.setBlockWhenExhausted(true);
            config.setMinEvictableIdleDuration(Duration.ofMillis(21000));
            config.setTimeBetweenEvictionRuns(Duration.ofMillis(21000));
            pool = new BeakGraphKeyedPool<>(new BeakGraphPoolFactory(),config);
        }
        return pool;
    }
}
