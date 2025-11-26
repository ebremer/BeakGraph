package com.ebremer.beakgraph;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.BGDatasetGraph;
import com.ebremer.beakgraph.core.EXIF;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import java.io.File;
import java.io.IOException;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.SchemaDO;

/**
 *
 * @author Erich Bremer
 */
public class Test {
    
    public static void main(String[] args) {
        File src = new File("D:\\data\\beakgraph\\dest\\dXX.h5");
        if (!src.exists()) {
            System.err.println("File not found: " + src.getAbsolutePath());
            return;
        }
        long start = System.nanoTime();
        try (HDF5Reader reader = new HDF5Reader(src)) {
            BeakGraph bg = new BeakGraph( reader, null, null );
            BGDatasetGraph dsg = new BGDatasetGraph(bg);
            Dataset ds = DatasetFactory.wrap(dsg);
            ds.listNames().forEachRemaining(ng->IO.println(ng));
            ParameterizedSparqlString  pss = new ParameterizedSparqlString(
                """
                select *
                where {                            
                    ?s hal:asHilbert1 ?hilbert .
                    ?hilbert hal:hasRange ?range .
                    ?range hal:low ?low; hal:high ?high .
                    
                    
                                        #filter ((?low >= 3391593900) && (?low <= 3391593999))
                                        #filter ((?low > 1847600570) && (?low <= 1847600572))
                                        #?range hal:high ?high
                                    
                                    #?member rdfs:member ?yay .
                                    #?yay a geo:Feature;
                                    #?s a geo:FeatureCollection;
                                    #    ?p ?o
                                    #?wow hal:classification ?classification
                                    #geo:hasGeometry [ geo:asWKT ?wkt ]
                                }
                                #order by ?low
                                limit 100
                """
            );
            ParameterizedSparqlString  pss2 = new ParameterizedSparqlString(
                """
                select *
                where {
                    ?image a sdo:ImageObject .
                    ?image exif:width ?width .                   
                    ?image exif:height ?height .
                }
                limit 100
                """
            );            
            pss = pss2;
            pss.setNsPrefix("hal", "https://halcyon.is/ns/");
            pss.setNsPrefix("rdfs", RDFS.getURI());
            pss.setNsPrefix("exif", EXIF.NS);
            pss.setNsPrefix("sdo", SchemaDO.NS);
            pss.setNsPrefix("geo", "http://www.opengis.net/ont/geosparql#");
            
            
            long end = System.nanoTime();
            double delta = end - start;
            delta = delta / 1000000d;
            IO.println("A : "+delta);
            
            IO.println("SPARQL:\n"+pss.toString());
            start = System.nanoTime();
            try (QueryExecution qexec = QueryExecutionFactory.create(pss.toString(), ds)) {
                ResultSet results = qexec.execSelect();
                ResultSetFormatter.out(System.out, results);
                end = System.nanoTime();
                delta = end - start;
                delta = delta / 1000000d;
                IO.println("B : "+delta);
            }
        } catch (IOException ex) {
            System.getLogger(Test.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
        }
    }
    
}

/*
                    #?wow a geo:Feature .
                    #?wow geo:hasGeometry 
                    #?wow a ?type .
                    #?wow hal:measurement  .
                    #?wow hal:classification ?classification
                    #?wow hal:classification <http://snomed.info/id/48512009> 

                #where {graph ?g {<https://ebremer.com> a ?o}}
                #where {graph <https://halcyon.is/ns/grid/6/1/2> {<https://ebremer.com> a ?o}}
                #where {graph <https://halcyon.is/ns/grid/6/1/2> {?s ?p ?o}}
*/