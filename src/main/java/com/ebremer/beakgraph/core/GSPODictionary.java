package com.ebremer.beakgraph.core;

import java.util.stream.Stream;
import org.apache.jena.graph.Node;

/**
 *
 * @author Erich Bremer
 */
public interface GSPODictionary {
    
    public long locateGraph(Node element);
    public Object extractGraph(int id);
    
    public long locateSubject(Node element);
    public Object extractSubject(int id);
    
    public long locatePredicate(Node element);
    public Object extractPredicate(int id);
    
    public long locateObject(Node element);
    public Object extractObject(int id);
    
    public Stream<Node> streamGraphs();
    public Stream<Node> streamSubjects();
    public Stream<Node> streamPredicates();
    public Stream<Node> streamObjects();
}
