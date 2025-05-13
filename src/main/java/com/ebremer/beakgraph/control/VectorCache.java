package com.ebremer.beakgraph.control;

import com.ebremer.beakgraph.ng.DataType;
import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import org.apache.arrow.vector.complex.StructVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author erich
 */
public class VectorCache implements AutoCloseable {
    private volatile static VectorCache instance = null;
    private volatile ConcurrentHashMap<URI,ConcurrentHashMap<VectorRequest, Future<ConcurrentHashMap<DataType,StructVector>>>> cache;
    private static final Logger logger = LoggerFactory.getLogger(VectorCache.class);
    
    private VectorCache() {
        cache = new ConcurrentHashMap<>();
    }
    
    public ConcurrentHashMap<VectorRequest, Future<ConcurrentHashMap<DataType,StructVector>>> getCache(URI uri) {
        var chunk = cache.get(uri);
        if (chunk==null) {
            cache.putIfAbsent(uri, new ConcurrentHashMap<>());
            chunk = cache.get(uri);
        }
        return chunk;
    }
    
    public void removeFromCache(URI uri) {
        var buff = cache.remove(uri);
        if (buff!=null) {
            buff.forEach((vr,map)->{
                try {
                    var group = map.get();
                    group.forEach((dt,sv)->{
                        logger.trace("closing : "+sv.getName()+"   "+sv.getAllocator().getName());
                        try (sv) {
                            logger.trace("closed : "+sv.getName()+"   "+sv.getAllocator().getName());
                        }
                    });
                } catch (InterruptedException ex) {
                    java.util.logging.Logger.getLogger(VectorCache.class.getName()).log(Level.SEVERE, null, ex);
                } catch (ExecutionException ex) {
                    java.util.logging.Logger.getLogger(VectorCache.class.getName()).log(Level.SEVERE, null, ex);
                }
            });        
            buff.clear();
            AllocatorCore.getInstance().closeChildAllocators(uri);
        }
    }
        
    public ConcurrentHashMap<URI,ConcurrentHashMap<VectorRequest, Future<ConcurrentHashMap<DataType,StructVector>>>> getCache() {
        return cache;
    }
    
    public static VectorCache getInstance() {
        if (instance == null) {
            synchronized (VectorCache.class) {
                if (instance == null) {
                    instance = new VectorCache();
                }
            }
        }
        return instance;
    }
    
    @Override
    public void close() {
        cache.forEach((k,v)->{
            v.forEach((vr,fmap)->{
                try {
                    fmap.get().forEach((dt,sv)->{
                        try (sv) {
                            
                        }
                    });
                } catch (InterruptedException ex) {
                    logger.error(ex.toString());;
                } catch (ExecutionException ex) {
                    logger.error(ex.toString());
                }
            });
        });
    }
}
