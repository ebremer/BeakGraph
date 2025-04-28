package com.ebremer.beakgraph.ng;

import java.util.Iterator;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author erich
 */
public class BindingBG extends BindingBase {    
    private final NodeTable nodeTable;
    private final BindingNodeId idBinding;
    private static final Logger logger = LoggerFactory.getLogger(BindingBG.class);
    
    public BindingBG(BindingNodeId idBinding, NodeTable nodeTable) {
        super(idBinding.getParentBinding());
        this.idBinding = idBinding;
        this.nodeTable = nodeTable;
    }
    
    public BindingNodeId getBindingId() {
        return idBinding ;
    }
    
    @Override
    protected Node get1(Var var) {
        if (idBinding.containsKey(var)) {
            NodeId i = idBinding.get(var);
            return nodeTable.getNodeForNodeId(i);
        }
        return null;
    }   

    @Override
    protected Iterator<Var> vars1() {
        return idBinding.iterator();
    }

    @Override
    protected int size1() {
        return idBinding.size();
    }

    @Override
    protected boolean isEmpty1() {
        return idBinding.isEmpty();
    }

    @Override
    protected boolean contains1(Var var) {
        return idBinding.containsKey(var);
    }

    @Override
    protected Binding detachWithNewParent(Binding newParent) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
