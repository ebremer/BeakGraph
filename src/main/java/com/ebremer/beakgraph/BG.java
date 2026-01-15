package com.ebremer.beakgraph;

import com.ebremer.beakgraph.core.BGDatasetGraph;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.EXIF;
import com.ebremer.beakgraph.core.lib.GEO;
import com.ebremer.beakgraph.core.lib.HAL;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;

/**
 *
 * @author Erich Bremer
 */
public class BG {
    
    public static HDF5Writer.Builder getBGWriterBuilder() { 
        return HDF5Writer.Builder();
    }
    
    public static void main(String[] args) throws FileNotFoundException, IOException, Exception {
        File file = new File("/beakgraph/sorted.nq.gz");
        File dest = new File("/beakgraph/dX2.h5");
        file = new File("/beakgraph/src/compressed/TCGA-CM-5348-01Z-00-DX1.2ad0b8f6-684a-41a7-b568-26e97675cce9.ttl.gz");
        dest = new File("/beakgraph/dest/dXX.h5");
        dest.getParentFile().mkdirs();
        
        
        if (dest.exists()) dest.delete();
        HDF5Writer.Builder()
            .setSource(file)
            .setSpatial(true)
            .setDestination(dest)
            .build()
            .write();
        
        /*
        //Dataset buffer = DatasetFactory.create();
        try (HDF5Reader reader = new HDF5Reader(dest)) {
            BeakGraph bg = new BeakGraph( reader, null, null );
            BGDatasetGraph dsg = new BGDatasetGraph(bg);
            Dataset ds = DatasetFactory.wrap(dsg);
           // buffer.getDefaultModel().add(ds.getDefaultModel());
            //buffer.addNamedModel(Params.SPATIALSTRING, ds.getNamedModel(Params.SPATIALSTRING));
            
            ds.getDefaultModel().setNsPrefix("geo", GEO.NS);
            ds.getDefaultModel().setNsPrefix("hal", HAL.NS);
            ds.getDefaultModel().setNsPrefix("exif", EXIF.NS);
            ds.getDefaultModel().setNsPrefix("xsd", XSD.NS);
            ds.getDefaultModel().setNsPrefix("rdfs", RDFS.getURI());
            ds.getDefaultModel().setNsPrefix("sno", "http://snomed.info/id/");
            try (FileOutputStream fos = new FileOutputStream(new File("/beakgraph/dest/dump2.trig"))) {
                RDFDataMgr.write(fos, ds, Lang.TRIG);
            }
            IO.println(ds.getDefaultModel().size());
            IO.println(ds.getNamedModel(Params.SPATIALSTRING).size());
            
        }*/
        

    }
}
