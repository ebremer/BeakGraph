package com.ebremer.beakgraph.ng;

import java.util.Iterator;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final boolean hasData;
    private DataType datatype;
    private final int namedgraph;
    private static final Logger logger = LoggerFactory.getLogger(PARIterator.class);
    
    public PARIterator(int ng, PAR par, NodeTable nt) {
        logger.trace("Create PARIterator : "+par.getPredicateNode());
        this.namedgraph = ng;
        this.par = par;
        this.dt = par.getAllTypes(ng).keySet().iterator();
        this.pnode = par.getPredicateNode();
        this.nt = nt;
        this.datatype = dt.next();
        this.pa = (StructVector) par.getAllTypes(ng).get(datatype).getChild("so");
        this.hasData = !pa.getChildFieldNames().isEmpty();
        this.curr = 0;
    }

    @Override
    public boolean hasNext() {
        logger.trace("hasNext()");
        return hasData&&(dt.hasNext()||(curr<pa.getValueCount()));
    }

    @Override
    public Triple next() {
        logger.trace("next()");
        try {
            Node snode = nt.getNodeForNodeId(new NodeId((int) pa.getChild("s").getObject(curr)));
            Node onode;
            onode = switch (datatype) {
                case RESOURCE -> nt.getNodeForNodeId(new NodeId((int) pa.getChild("o").getObject(curr)));
                default -> nt.getNodeForNodeId(new NodeId(pa.getChild("o").getObject(curr)));
            };
            curr++;
            if (curr==pa.getValueCount()) {
                if (dt.hasNext()) {
                    datatype = dt.next();
                    pa = (StructVector) par.getAllTypes(namedgraph).get(datatype).getChild("so");
                    curr = 0;
                }
            }
            return Triple.create(snode, pnode, onode);
        } catch (IndexOutOfBoundsException ex) {
            throw new Error("PARIterator -> "+ex.getMessage());
        }
    }
}
