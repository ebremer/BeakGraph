/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.ebremer.beakgraph.extra;

import org.apache.jena.graph.BlankNodeId;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;

/**
 *
 * @author erich
 */
public class NewClass {
    
    public static void main(String[] args) {
        BlankNodeId c;
        Node n = NodeFactory.createBlankNode("_:h222");
        System.out.println(n);
    }
    
}
