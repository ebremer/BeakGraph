package com.ebremer.beakgraph.pool;

import com.ebremer.beakgraph.core.BeakGraph;
import java.time.Duration;

/**
 * The JVM-wide pool of {@link BeakGraph} readers, keyed by store URI.
 * <p>
 * A reader is safe for concurrent use (absolute buffer reads throughout), so
 * pooling exists for lifecycle - validation on borrow, idle eviction,
 * invalidation on failure - not for exclusion. Sizing is therefore about
 * memory (each instance holds mapped buffers, a tiered node index and its
 * caches) against parallelism: the LWS SPARQL path holds one instance for the
 * whole of a request's result streaming, so the per-key cap is the number of
 * in-flight queries a store can answer at once. It used to be 2, with a
 * 60-second wait: the third concurrent query on a file stalled for a minute
 * and then failed with a 500. Tunable through system properties:
 * <ul>
 *   <li>{@code beakgraph.pool.perKey} (8): concurrent readers per store;</li>
 *   <li>{@code beakgraph.pool.maxTotal} (256): readers across all stores;</li>
 *   <li>{@code beakgraph.pool.maxWait.seconds} (5): how long a borrow waits
 *       for a free reader before failing fast (the endpoint answers 503);</li>
 *   <li>{@code beakgraph.pool.idle.seconds} (300): idle time before an
 *       instance is closed, releasing its file mapping.</li>
 * </ul>
 *
 * @author erich
 */
public class BeakGraphPool {

    private BeakGraphPool() {}

    private static class Holder {
        private static final BeakGraphKeyedPool INSTANCE;
        static {
            BeakGraphKeyedPoolConfig<BeakGraph> config = new BeakGraphKeyedPoolConfig<>();
            config.setMaxTotalPerKey(Integer.getInteger("beakgraph.pool.perKey", 8));
            config.setMaxTotal(Integer.getInteger("beakgraph.pool.maxTotal", 256));
            config.setMinIdlePerKey(0);
            config.setTestOnBorrow(true);
            config.setMaxWait(Duration.ofSeconds(Long.getLong("beakgraph.pool.maxWait.seconds", 5L)));
            config.setBlockWhenExhausted(true);
            config.setMinEvictableIdleDuration(Duration.ofSeconds(Long.getLong("beakgraph.pool.idle.seconds", 300L)));
            config.setTimeBetweenEvictionRuns(Duration.ofMinutes(1));
            INSTANCE = new BeakGraphKeyedPool(new BeakGraphPoolFactory(), config);
        }
    }

    public static BeakGraphKeyedPool getPool() {
        return Holder.INSTANCE;
    }
}
