package com.ebremer.beakgraph.pool;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.DestroyMode;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author erich
 */
public class BeakGraphPoolFactory extends BaseKeyedPooledObjectFactory<URI, BeakGraph> {
    private final Map<URI, AtomicInteger> instanceCountMap = new ConcurrentHashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(BeakGraphPoolFactory.class);
    
    public BeakGraphPoolFactory() {}

    @Override
    public BeakGraph create(URI uri) throws Exception {
        logger.trace("Creating BeakGraph "+uri);
        //IO.println("Creating BeakGraph "+uri);
        instanceCountMap.computeIfAbsent(uri, k -> new AtomicInteger()).incrementAndGet();
        HDF5Reader reader = new HDF5Reader(new File(uri));
        BeakGraph bg = new BeakGraph( reader, uri, null );
        return bg;
    }
    
    @Override
    public PooledObject<BeakGraph> wrap(BeakGraph value) {
        return new DefaultPooledObject<>(value);
    }
    
    public int getInstanceCount(URI key) {
        AtomicInteger count = instanceCountMap.get(key);
        return count == null ? 0 : count.get();
    }

    @Override
    public void destroyObject(URI uri, PooledObject p, DestroyMode mode) throws Exception {
        logger.trace("Closing BeakGraph "+uri);
        instanceCountMap.get(uri).decrementAndGet();
        ((BeakGraph) p.getObject()).close();
        super.destroyObject(uri, p, mode);
               
    }  
}
