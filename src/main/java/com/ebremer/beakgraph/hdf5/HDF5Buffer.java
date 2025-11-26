package com.ebremer.beakgraph.hdf5;

import io.jhdf.api.WritableGroup;
import java.nio.file.Path;

/**
 *
 * @author Erich Bremer
 */
public interface HDF5Buffer {
    public Path getName();
    public long getNumEntries();
    public void Add(WritableGroup group);
}
