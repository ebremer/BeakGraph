package com.ebremer.beakgraph.pool;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import java.io.File;
import java.net.URI;
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
    private static final Logger logger = LoggerFactory.getLogger(BeakGraphPoolFactory.class);
    
    public BeakGraphPoolFactory() {}

    @Override
    public BeakGraph create(URI uri) throws Exception {
        logger.trace("Creating BeakGraph {}", uri);
        HDF5Reader reader = new HDF5Reader(new File(uri));
        return new BeakGraph(reader, uri, null);
    }
    
    @Override
    public PooledObject<BeakGraph> wrap(BeakGraph value) {
        return new DefaultPooledObject<>(value);
    }

    /**
     * Without this override, BaseKeyedPooledObjectFactory.validateObject always
     * returns true and setTestOnBorrow(true) validates nothing - an instance whose
     * reader was closed (poisoned) would be re-issued and fail on first use.
     */
    @Override
    public boolean validateObject(URI uri, PooledObject<BeakGraph> p) {
        return p.getObject().getReader().isOpen();
    }

    @Override
    public void destroyObject(URI uri, PooledObject<BeakGraph> p, DestroyMode mode) throws Exception {
        logger.trace("destroyObject {}", uri);        
        p.getObject().close();
        super.destroyObject(uri, p, mode);               
    }  
    
}
