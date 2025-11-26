package com.ebremer.beakgraph;

import com.ebremer.beakgraph.core.BGDatasetGraph;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;

/**
 *
 * @author Erich Bremer
 */
public class BG {
    
    public static HDF5Writer.Builder getBGWriterBuilder() { 
        return HDF5Writer.Builder();
    }
    
    public static void main(String[] args) throws FileNotFoundException, IOException, Exception {
        File file = new File("/data/sorted.nq.gz");
        File dest = new File("/data/dX2.h5");
        file = new File("/data/beakgraph/src/compressed/TCGA-CM-5348-01Z-00-DX1.2ad0b8f6-684a-41a7-b568-26e97675cce9.ttl.gz");
        dest = new File("/data/beakgraph/dest/dXX.h5");
        dest.getParentFile().mkdirs();
        
        /*
        if (dest.exists()) dest.delete();
        HDF5Writer.Builder()
            .setSource(file)
            .setSpatial(true)
            .setDestination(dest)
            .build()
            .write();
        */
        try (HDF5Reader reader = new HDF5Reader(dest)) {
            BeakGraph bg = new BeakGraph( reader, null, null );
            BGDatasetGraph dsg = new BGDatasetGraph(bg);
            Dataset ds = DatasetFactory.wrap(dsg);
            ds.getDefaultModel().write(System.out, "NTRIPLE");
        }
    }
    
}
