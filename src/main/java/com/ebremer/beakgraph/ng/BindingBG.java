package com.ebremer.beakgraph.ng;

import java.util.Iterator;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.BindingBase;

/**
 *
 * @author erich
 */
public class BindingBG extends BindingBase {
    
    private final NodeTable nodeTable;
    private final BindingNodeId idBinding;
    
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
}
