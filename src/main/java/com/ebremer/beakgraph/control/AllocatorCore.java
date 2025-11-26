package com.ebremer.beakgraph.control;

import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.DefaultAllocationManagerOption;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.LoggerFactory;

/**
 *
 * @author erich
 */
public class AllocatorCore implements AutoCloseable {
    private static volatile AllocatorCore instance = null;
    private final RootAllocator root;
    private final ConcurrentHashMap<URI,BufferAllocator> children;
    private final ConcurrentHashMap<String,BufferAllocator> frags;
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(AllocatorCore.class);
    
    private AllocatorCore() {
        System.setProperty("arrow.memory.debug.allocator","false");
        System.setProperty(DefaultAllocationManagerOption.ALLOCATION_MANAGER_TYPE_PROPERTY_NAME,"Unsafe");        
        root = new RootAllocator();
        children = new ConcurrentHashMap<>();
        frags = new ConcurrentHashMap<>();
    }
    
    public static AllocatorCore getInstance() {
        if (instance == null) {
            synchronized (AllocatorCore.class) {
                if (instance == null) {
                    instance = new AllocatorCore();
                }
            }
        }
        return instance;
    }
    
    public synchronized void closeChildAllocators(URI uri) {
        logger.debug("Closing Allocators for : "+uri);
        BufferAllocator ba = children.remove(uri);
        if (ba!=null) {            
            ba.getChildAllocators().forEach(b->{
                logger.trace("Closing Child Allocator : "+b.getName());
                try(b) {
                    logger.trace("Closed Child Allocator : "+b.getName());
                } catch (IllegalStateException ex) {
                    logger.error("Error Closing Child Allocator for : "+b.getName());
                }
            });
            logger.trace("Closing Main Allocator for : "+uri);
            ba.getChildAllocators().forEach(b->{
                System.out.println("Main Allocator Child : "+b.getName());
            });
            try(ba) {               
                logger.trace("Closing Main Allocator : "+ba.getName());
            } catch (IllegalStateException ex) {
                logger.error("Error Closing Main Allocator for : "+ba.getName());
            }
        }        
    }
    
    public synchronized BufferAllocator getChildAllocator(URI uri) {
        BufferAllocator child = children.get(uri);
        if (child==null) {
            child = root.newChildAllocator(uri.toString(), 0, Long.MAX_VALUE);
            children.putIfAbsent(uri, child);            
        }
        return child;
    }
    
    public synchronized BufferAllocator getChildAllocator(URI uri, String name) {                
        String n = uri.toString()+"/"+name;
        BufferAllocator ba = frags.get(n);        
        if (ba==null) {
            ba = getChildAllocator(uri).newChildAllocator(n, 0, Long.MAX_VALUE);   
            frags.putIfAbsent(n, ba);
        }
        return ba;
    }
    
    @Override
    public void close() {
        logger.debug("Closing Root Allocator");
        try (root) {
            logger.debug("Closed Root Allocator");
        }
    }
}
