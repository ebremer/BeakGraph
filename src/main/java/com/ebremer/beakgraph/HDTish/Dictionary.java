package com.ebremer.beakgraph.HDTish;

import org.apache.jena.graph.Node;

/**
 *
 * @author Erich Bremer
 */
public interface Dictionary {
    public int locateGraph(Node element);
    public Object extractGraph(int id);
    public int locateSubject(Node element);
    public Object extractSubject(int id);
    public int locatePredicate(Node element);
    public Object extractPredicate(int id);
    public int locateObject(Node element);
    public Object extractObject(int id);
}
