package com.ebremer.beakgraph.core;

import java.util.stream.Stream;
import org.apache.jena.graph.Node;

/**
 *
 * @author Erich Bremer
 */
public interface GSPODictionary {

    public Dictionary getGraphs();
    public Dictionary getSubjects();
    public Dictionary getPredicates();
    public Dictionary getObjects();
    
    public long locateGraph(Node element);
    public Node extractGraph(long id);
    
    public long locateSubject(Node element);
    public Node extractSubject(long id);
    
    public long locatePredicate(Node element);
    public Node extractPredicate(long id);
    
    public long locateObject(Node element);
    public Node extractObject(long id);
    
    public Stream<Node> streamGraphs();
    public Stream<Node> streamSubjects();
    public Stream<Node> streamPredicates();
    public Stream<Node> streamObjects();
    
}
