package com.ebremer.beakgraph.pool;

import com.ebremer.beakgraph.core.BeakGraph;
import java.net.URI;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author erich
 */
public class BeakGraphKeyedPool extends GenericKeyedObjectPool<URI, BeakGraph> {
    private static final Logger logger = LoggerFactory.getLogger(BeakGraphKeyedPool.class);
    
    public BeakGraphKeyedPool(BeakGraphPoolFactory factory, BeakGraphKeyedPoolConfig<BeakGraph> config) {
        super(factory, config);
    }
    
    @Override
    public BeakGraph borrowObject(final URI key) throws Exception {
        // getStatus() takes five pool-lock metrics; only pay for it when TRACE is on.
        if (logger.isTraceEnabled()) {
            logger.trace("borrowObject {}\n{}", key, getStatus());
        }
        return super.borrowObject(key);
    }

    @Override
    public void returnObject(final URI key, final BeakGraph reader) {
        if (logger.isTraceEnabled()) {
            logger.trace("returnObject {}\n{}", key, getStatus());
        }
        super.returnObject(key, reader);
    }
    
    public String getStatus() {
        return String.format("""
               Active Objects  : %d
               Idle Objects    : %d
               Total Borrowed  : %d
               Created Count   : %d
               Destroyed Count : %d
               """,
                getNumActive(),
                getNumIdle(),
                getBorrowedCount(),
                getCreatedCount(),
                getDestroyedCount());
    }
}
