package com.ebremer.beakgraph.extra;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.RDF;

/**
 *
 * @author erich
 */
public class newJavaFile {

    /**
     * @param args the command line arguments
     */
    public static void main(String args[]) {
        Model m = ModelFactory.createDefaultModel();        
        m.createResource("").addProperty(RDF.type, FOAF.Person);
        m.createResource("happy").addProperty(RDF.type, FOAF.Person);
        m.createResource("happy/bremer").addProperty(RDF.type, FOAF.Person);              
        m.write(System.out, "N-TRIPLE", "http://ebremer.com/");
    }
}
