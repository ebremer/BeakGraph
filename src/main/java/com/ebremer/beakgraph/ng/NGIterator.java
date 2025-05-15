package com.ebremer.beakgraph.ng;

import java.util.Iterator;
import org.apache.arrow.vector.IntVector;
import org.apache.jena.graph.Node;

/**
 *
 * @author erich
 */
public class NGIterator implements Iterator<Node> {
    private final IntVector ngid;
    private int ci;
    private final NodeTable nt;
    
    public NGIterator(IntVector ngid, NodeTable nt) {
        this.ngid = ngid;
        this.ci = 0;
        this.nt = nt;
    }

    @Override
    public boolean hasNext() {
        return ci < ngid.getValueCount();
    }

    @Override
    public Node next() {
        return nt.getNamedGraph(ngid.get(ci++));
    }
    
}
