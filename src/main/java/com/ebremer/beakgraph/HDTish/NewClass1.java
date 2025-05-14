package com.ebremer.beakgraph.HDTish;

import java.io.IO;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.ResourceFactory;

/**
 *
 * @author Erich Bremer
 */
public class NewClass1 {
    
    public static void main(String[] args) {
        Literal ha = ResourceFactory.createTypedLiteral("434434", XSDDatatype.XSDlong);
        System.out.println(ha);
        Node haha = ha.asNode();
        Object hh = haha.getLiteralValue();
        System.out.println(hh.getClass().toGenericString());
    }
    
}
