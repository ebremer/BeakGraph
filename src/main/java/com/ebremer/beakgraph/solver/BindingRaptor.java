package com.ebremer.beakgraph.solver;

import com.ebremer.beakgraph.rdf.NodeTable;
import com.ebremer.beakgraph.store.NodeId;
import java.util.Iterator;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.BindingBase;

/**
 *
 * @author erich
 */
public class BindingRaptor extends BindingBase {
    
    private final NodeTable nodeTable;
    private final BindingNodeId idBinding;
    
    public BindingRaptor(BindingNodeId idBinding, NodeTable nodeTable) {
        super(idBinding.getParentBinding());
        this.idBinding = idBinding;
        this.nodeTable = nodeTable;
    }
    
    public BindingNodeId getBindingId() {
        return idBinding ;
    }
    
    @Override
    protected Node get1(Var var) {
        NodeId i = idBinding.get(var);
        return nodeTable.getNodeForNodeId(i);
    }   

    @Override
    protected Iterator<Var> vars1() {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    protected int size1() {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    protected boolean isEmpty1() {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    protected boolean contains1(Var var) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }
}
