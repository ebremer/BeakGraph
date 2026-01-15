package com.ebremer.beakgraph.core;

import java.util.stream.Stream;
import org.apache.jena.graph.Node;

/**
 *
 * @author Erich Bremer
 */
public interface GSPODictionary {
    
    public long locateGraph(Node element);
    public Object extractGraph(long id);
    
    public long locateSubject(Node element);
    public Object extractSubject(long id);
    
    public long locatePredicate(Node element);
    public Object extractPredicate(long id);
    
    public long locateObject(Node element);
    public Object extractObject(long id);
    
    public Stream<Node> streamGraphs();
    public Stream<Node> streamSubjects();
    public Stream<Node> streamPredicates();
    public Stream<Node> streamObjects();
}
