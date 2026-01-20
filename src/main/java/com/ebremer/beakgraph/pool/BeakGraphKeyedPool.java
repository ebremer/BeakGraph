package com.ebremer.beakgraph.pool;

import com.ebremer.beakgraph.core.BeakGraph;
import java.net.URI;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

/**
 *
 * @author erich
 */
public class BeakGraphKeyedPool extends GenericKeyedObjectPool<URI, BeakGraph> {
    
    public BeakGraphKeyedPool(BeakGraphPoolFactory factory, BeakGraphKeyedPoolConfig<BeakGraph> config) {
        super(factory, config);
    }
    
    @Override
    public BeakGraph borrowObject(final URI key) throws Exception {     
        return super.borrowObject(key);
    }
    
    @Override
    public void returnObject(final URI key, final BeakGraph reader) {
        super.returnObject(key, reader);
    }
    
    public void printStatus() {
       System.out.println("--- Pool Status ---");
        System.out.println("Active Objects: " + getNumActive());
        System.out.println("Idle Objects:   " + getNumIdle());
        System.out.println("Total Borrowed: " + getBorrowedCount());
        System.out.println("Created Count:  " + getCreatedCount());
        System.out.println("Destroyed Count: " + getDestroyedCount());
        // listAllObjects()
}
}
