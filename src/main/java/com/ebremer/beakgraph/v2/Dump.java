package com.ebremer.beakgraph.v2;

import com.ebremer.beakgraph.ng.BGDatasetGraph;
import com.ebremer.beakgraph.ng.BeakGraph;
import com.ebremer.beakgraph.ng.BeakReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

/**
 *
 * @author Erich Bremer
 */
public class Dump {
    
    public static void main(String[] args) throws IOException {
        File file = new File("/HalcyonStorage/nuclearsegmentation2019/coad/TCGA-CM-5348-01Z-00-DX1.2ad0b8f6-684a-41a7-b568-26e97675cce9.zip");
        BeakGraph br = new BeakGraph(file.toURI());
        BGDatasetGraph ds = new BGDatasetGraph(br);
       
        Dataset dsx = DatasetFactory.wrap(ds);
        //ds.listGraphNodes().forEachRemaining(n->System.out.println(n));
        Dataset k = DatasetFactory.create();
        dsx.listModelNames().forEachRemaining(m->{
            System.out.println(m);
            k.addNamedModel(m.toString(), dsx.getNamedModel(m.toString()));
        });
        k.getDefaultModel().add(dsx.getDefaultModel());
       try(FileOutputStream fos = new FileOutputStream("/tcga/alpha.nq")){
          RDFDataMgr.write(fos, k, Lang.NQUADS);
      }
       // System.out.println(m.size());
    }
    
}
