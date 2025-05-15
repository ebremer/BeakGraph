package com.ebremer.beakgraph.core;

import java.io.File;
import org.apache.jena.query.Dataset;

// T refers to the concrete class (e.g., HDF5Writer.Builder)
public abstract class AbstractGraphBuilder<T extends AbstractGraphBuilder<T>> {
    
    protected File src;
    protected File dest;
    protected Dataset ds;
    protected boolean spatial;

    // Force the concrete class to return 'this'
    protected abstract T self();

    public T setSource(File file) {
        this.src = file;
        return self();
    }

    public T setDestination(File file) {
        this.dest = file;
        return self();
    }
    
    public T setSpatial(boolean flag) {
        this.spatial = flag;
        return self();
    }
    
    public boolean getSpatial() {
        return spatial;
    }

    public T setDataset(Dataset ds) {
        this.ds = ds;
        return self();
    }

    public File getSource() { return src; }
    public File getDestination() { return dest; }
    public Dataset getDataset() { return ds; }
    
    // All writers usually need a root group name
    public abstract String getName(); 
    
    // All builders must produce a Writer
    public abstract BeakGraphWriter build() throws java.io.IOException;
}
