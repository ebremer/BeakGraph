package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.BeakGraph;
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
    private final BeakGraph bGraph;
    private final BindingNodeId idBinding;
    private static final Logger logger = LoggerFactory.getLogger(BindingBG.class);
    
    public BindingBG(BindingNodeId idBinding, BeakGraph bGraph) {
        super(idBinding.getParentBinding());
        this.idBinding = idBinding;
        this.bGraph = bGraph;
    }
    
    public BindingNodeId getBindingId() {
        return idBinding ;
    }
    
    @Override
    protected Node get1(Var var) {
        if (idBinding.containsKey(var)) {
            NodeId id = idBinding.get(var);
            // A var bound to "does not exist" (e.g. a VALUES/BIND term not in this store)
            // has no node here; return null so BindingBase falls back to the parent binding,
            // which still carries the original term.
            if (NodeId.isDoesNotExist(id)) {
                return null;
            }
            return bGraph.getReader().getNodeTable().getNodeForNodeId(id);
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
