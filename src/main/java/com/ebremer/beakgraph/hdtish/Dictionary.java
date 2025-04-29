package com.ebremer.beakgraph.hdtish;

import org.apache.jena.graph.Node;

/**
 *
 * @author Erich Bremer
 */
public interface Dictionary {
    public int locate(Node element);
    public Object extract(int id);
}
