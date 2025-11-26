package com.ebremer.beakgraph.ng;

import java.io.IOException;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;
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
