package com.ebremer.beakgraph.rdf;

import com.ebremer.beakgraph.store.NodeId;
import java.util.Iterator;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

/**
 *
 * @author erich
 */
public class PARIterator implements Iterator {
    private final PAR par;
    private final Iterator<DataType> dt;
    private StructVector pa;
    private int curr;
    private final Node pnode;
    private final NodeTable nt;
    
    public PARIterator(PAR par, NodeTable nt) {
        this.par = par;
        this.dt = par.getAllTypes().keySet().iterator();
        this.pnode = par.getPredicateNode();
        this.nt = nt;
        this.pa = (StructVector) par.getAllTypes().get(dt.next()).getChild("so");
        this.curr = 0;
    }

    @Override
    public boolean hasNext() {
        return dt.hasNext()||(curr<pa.getValueCount());
    }

    @Override
    public Triple next() {
        Node snode = nt.getNodeForNodeId(new NodeId((int) pa.getChild("s").getObject(curr)));
        Node onode = nt.getNodeForNodeId(new NodeId(pa.getChild("o").getObject(curr)));
        curr++;
        if (curr==pa.getValueCount()) {
            if (dt.hasNext()) {
                pa = (StructVector) par.getAllTypes().get(dt.next()).getChild("so");
                curr = 0;
            }
        }
        return new Triple(snode, pnode, onode);
    }
}
