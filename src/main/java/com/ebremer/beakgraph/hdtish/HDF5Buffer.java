package com.ebremer.beakgraph.hdtish;

import java.nio.file.Path;

/**
 *
 * @author Erich Bremer
 */
public interface HDF5Buffer {
    
    public Path getName();
    public byte[] getBuffer();
    
}
