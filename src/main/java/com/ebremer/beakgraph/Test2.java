package com.ebremer.beakgraph;

import com.ebremer.beakgraph.core.BGDatasetGraph;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;

public class Test2 {
    public static void main(String[] args) throws IOException {
        File src = new File("/beakgraph/dest/compressed/TCGA-CM-5348-01Z-00-DX1.2ad0b8f6-684a-41a7-b568-26e97675cce9.ttl.h5");
        if (!src.exists()) {
            System.err.println("File not found: " + src.getAbsolutePath());
            return;
        }
        try (HDF5Reader reader = new HDF5Reader(src)) {
            BeakGraph bg = new BeakGraph(reader, reader.getURI(), null);
            BGDatasetGraph dsg = new BGDatasetGraph(bg);
            Dataset ds = DatasetFactory.wrap(dsg);
            String outputName = src.getName().replace(".ttl.h5", ".trig");
            File out = new File(src.getParentFile(), outputName);
            try (FileOutputStream fos = new FileOutputStream(out)) {
                RDFDataMgr.write(fos, ds, RDFFormat.TRIG_PRETTY);
            }
        }
    }
}
