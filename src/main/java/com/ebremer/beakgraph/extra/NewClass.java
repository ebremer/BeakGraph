/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.ebremer.beakgraph.extra;

import org.apache.jena.graph.BlankNodeId;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.AnonId;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.vocabulary.RDF;

/**
 *
 * @author erich
 */
public class NewClass {
    
    public static void main(String[] args) {
        Model m = ModelFactory.createDefaultModel();
        Node n = NodeFactory.createBlankNode("ha");
        System.out.println(n);
        System.out.println(n.getBlankNodeId());
        System.out.println(n.getBlankNodeLabel());
        System.out.println(n.getBlankNodeId());
        m.add(m.createResource(new AnonId("HA")), RDF.value, "HA!!");
        RDFDataMgr.write(System.out, m, RDFFormat.NTRIPLES);
    }
    
}
