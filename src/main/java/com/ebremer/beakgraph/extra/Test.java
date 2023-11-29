package com.ebremer.beakgraph.extra;

import com.ebremer.beakgraph.ng.BGDatasetGraph;
import com.ebremer.beakgraph.ng.BeakGraph;
import com.ebremer.beakgraph.ng.StopWatch;
import java.io.File;
import java.io.IOException;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;


/**
 *
 * @author erich
 */
public class Test {
    
    public static void main(String[] args) throws IOException {
        Configurator.setLevel("com.ebremer.beakgraph.ng", Level.ERROR);
        //Configurator.setRootLevel(Level.ALL);        
        StopWatch sw = StopWatch.getInstance();
        //File file = new File("D:\\HalcyonStorage\\nuclearsegmentation2019\\coad\\TCGA-CM-5348-01Z-00-DX1.2ad0b8f6-684a-41a7-b568-26e97675cce9.zip");
        File file = new File("D:\\tcga\\cvpr-data\\zip\\brca\\TCGA-E2-A1B1-01Z-00-DX1.7C8DF153-B09B-44C7-87B8-14591E319354.zip");
        BeakGraph bg = new BeakGraph(file.toURI());
        Dataset ds = DatasetFactory.wrap(new BGDatasetGraph(bg));
        /*
        Model ha = ds.getDefaultModel();
        ha.setNsPrefix("hal", "https://www.ebremer.com/halcyon/ns/");
        ha.setNsPrefix("exif", "http://www.w3.org/2003/12/exif/ns#");
        ha.setNsPrefix("geo", "http://www.opengis.net/ont/geosparql#");
        ha.setNsPrefix("xmls", "http://www.w3.org/2001/XMLSchema#");
        ha.setNsPrefix("prov", "http://www.w3.org/ns/prov#");
        ha.setNsPrefix("so", "https://schema.org/");
          */     
        sw.Lapse("Default Graph Loaded");
        ParameterizedSparqlString pss = new ParameterizedSparqlString(
            """
            select *
            where {
                #?s hal:hasClassification ?o
                graph ?g {
                    ?s geo:hasGeometry ?geometry .
                    ?geometry hal:asHilbert ?hilbert .
                    ?hilbert hal:hasRange ?range .
                    ?range hal:low ?low; hal:high ?high
                }
            } limit 10
            """
        );
        pss.setNsPrefix("hal", "https://www.ebremer.com/halcyon/ns/");
        pss.setNsPrefix("exif", "http://www.w3.org/2003/12/exif/ns#");
        pss.setNsPrefix("geo", "http://www.opengis.net/ont/geosparql#");
        pss.setNsPrefix("xmls", "http://www.w3.org/2001/XMLSchema#");
        pss.setNsPrefix("prov", "http://www.w3.org/ns/prov#");
        QueryExecution qe = QueryExecutionFactory.create(pss.toString(),ds);
        ResultSet rs = qe.execSelect();
        System.out.println(sw.Lapse("Here."));
        ResultSetFormatter.out(System.out, rs);
        System.out.println(sw.Lapse("Done."));
        //RDFDataMgr.write(System.out, ha, RDFFormat.TURTLE_PRETTY);
    }
    
}
