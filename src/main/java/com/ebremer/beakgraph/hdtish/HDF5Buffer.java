package com.ebremer.beakgraph.hdtish;

import java.nio.file.Path;
import java.util.Map;

/**
 *
 * @author Erich Bremer
 */
public interface HDF5Buffer {
    
    public Path getName();
    public byte[] getBuffer();
    public Map<String,Object> getProperties();
    
}
