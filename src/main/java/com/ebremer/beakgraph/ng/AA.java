package com.ebremer.beakgraph.ng;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.RDF;

/**
 *
 * @author erich
 */
public class AA {
    
    public static void main(String[] args) throws IOException {
        Model m = ModelFactory.createDefaultModel();
        Statement s = m.createLiteralStatement(m.createResource(), RDF.value, 10L);
        RDFDatatype dt = s.getObject().asLiteral().getDatatype();
        Class<?> c = dt.getJavaClass();
        if (c.equals(Long.class)) {
            System.out.println("YAY LONG");
        }

    }
}
