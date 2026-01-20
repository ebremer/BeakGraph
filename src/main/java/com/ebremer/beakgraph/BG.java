package com.ebremer.beakgraph;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

/**
 *
 * @author Erich Bremer
 */
public class BG {
    
    public static HDF5Writer.Builder getBGWriterBuilder() { 
        return HDF5Writer.Builder();
    }
    
    public static BeakGraph getBeakGraph(File file) throws IOException {
        HDF5Reader reader = new HDF5Reader(file);
        return new BeakGraph(reader);
    }
    
    public static BeakGraph getBeakGraph(Path path) throws IOException {
        HDF5Reader reader = new HDF5Reader(path);       
        return new BeakGraph(reader);
    }
}
