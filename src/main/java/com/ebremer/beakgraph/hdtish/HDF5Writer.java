package com.ebremer.beakgraph.hdtish;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 *
 * @author Erich Bremer
 */
public class HDF5Writer {
    private File src = null;
    private File dest = null;
    
    private HDF5Writer() {}
    
    private HDF5Writer(Builder builder) throws IOException {
        src = builder.getSource();
        dest = builder.getDestination();
        FiveSectionDictionaryWriter.Builder db = new FiveSectionDictionaryWriter.Builder();
        FiveSectionDictionaryWriter w = db
            .setSource(src)
            .setDestination(dest)
            .build();
        w.close();
        
        
    }
    
    public static class Builder {
        private File src;
        private File dest;
        
        public File getSource() {
            return src;
        }
        
        public File getDestination() {
            return dest;
        }

        public Builder setSource(File file) {
            src = file;
            return this;
        }
        
        public Builder setDestination(File file) {
            dest = file;
            return this;
        }
        
        public HDF5Writer build() throws IOException {
            return new HDF5Writer(this);
        }
    }
    
    public static void main(String[] args) throws FileNotFoundException, IOException, Exception {
        File file = new File("/data/sorted.nq.gz");
        File dest = new File("/data/data.hdf5");
        
        Builder builder = new HDF5Writer.Builder();
        builder
            .setSource(file)
            .setDestination(dest)
            .build();
    }
    
}
