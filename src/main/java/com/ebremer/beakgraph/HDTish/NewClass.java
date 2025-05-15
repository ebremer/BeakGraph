package com.ebremer.beakgraph.HDTish;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

/**
 *
 * @author Erich Bremer
 */
public class NewClass {
    
    public static void main(String[] args) {
        Model m = ModelFactory.createDefaultModel();
        Set<Node> list = new HashSet<>();
        list.add(m.createResource().asNode());
        list.add(m.createResource().asNode());
        list.add(m.createResource().asNode());
        list.add(m.createResource().asNode());
        Node x = m.createResource().asNode();
        list.add(x);
        list.add(m.createResource().asNode());
        list.add(m.createResource().asNode());
        list.add(m.createResource().asNode());
        list.add(m.createResource().asNode());
        ArrayList<Node> sorted = NodeSorter.parallelSort(list);
        sorted.forEach(n->System.out.println(n));
        
        System.out.println("========================================");
        System.out.println(x);
        System.out.println(NodeSearch.findPosition(sorted, x));
        
    }
    
}
