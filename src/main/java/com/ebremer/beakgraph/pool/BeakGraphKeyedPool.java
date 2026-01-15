package com.ebremer.beakgraph.pool;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

/**
 *
 * @author erich
 * @param <URI>
 * @param <BeakGraph>
 */
public class BeakGraphKeyedPool<URI, BeakGraph> extends GenericKeyedObjectPool<URI, BeakGraph> {
    
    public BeakGraphKeyedPool(BeakGraphPoolFactory factory, BeakGraphKeyedPoolConfig config) {
        super((BaseKeyedPooledObjectFactory)factory, config);
    }
    
    @Override
    public BeakGraph borrowObject(final URI key) {
        try {
            BeakGraph f = (BeakGraph) super.borrowObject(key);
            return f;
        } catch (Exception ex) {
            Logger.getLogger(BeakGraphKeyedPool.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }
    
    @Override
    public void returnObject(final URI key, final BeakGraph reader) {
        super.returnObject(key, reader);
    }
}
