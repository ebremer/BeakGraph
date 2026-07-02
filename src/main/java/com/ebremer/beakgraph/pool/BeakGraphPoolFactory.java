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
        try {
            return new BeakGraph(reader, uri, null);
        } catch (RuntimeException | Error e) {
            // Release the mapped file if wrapping fails - a leaked reader pins it.
            try { reader.close(); } catch (Exception ignore) {}
            throw e;
        }
    }
    
    @Override
    public PooledObject<BeakGraph> wrap(BeakGraph value) {
        return new DefaultPooledObject<>(value);
    }

    /**
     * Without this override, BaseKeyedPooledObjectFactory.validateObject always
     * returns true and setTestOnBorrow(true) validates nothing - an instance whose
     * reader was closed (poisoned) would be re-issued and fail on first use.
     * The probe additionally touches REAL mapped data, not just the open flag:
     * a reader whose backing file was replaced or truncated underneath stays
     * "open" but throws on access, and an isOpen-only check re-issued it to
     * every borrower forever.
     */
    @Override
    public boolean validateObject(URI uri, PooledObject<BeakGraph> p) {
        BeakGraph bg = p.getObject();
        if (!bg.getReader().isOpen()) {
            return false;
        }
        try {
            var predicates = bg.getReader().getDictionary().getPredicates();
            if (predicates.getNumberOfNodes() > 0) {
                predicates.extract(1);
            }
            return true;
        } catch (RuntimeException | Error e) {
            logger.warn("Pooled BeakGraph for {} failed data-access validation; discarding", uri, e);
            return false;
        }
    }

    @Override
    public void destroyObject(URI uri, PooledObject<BeakGraph> p, DestroyMode mode) throws Exception {
        logger.trace("destroyObject {}", uri);        
        p.getObject().close();
        super.destroyObject(uri, p, mode);               
    }  
    
}
