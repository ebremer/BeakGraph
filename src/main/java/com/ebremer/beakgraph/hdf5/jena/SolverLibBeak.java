package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.BeakGraph;
import java.util.Iterator;
import java.util.function.Function;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;

/**
 *
 * @author erich
 */
public class SolverLibBeak {
    
    public static Iterator<BindingNodeId> convertToIds(Iterator<Binding> iterBindings, BeakGraph bGraph) {
        return Iter.map(iterBindings, convFromBinding(bGraph));
    }
    
    public static Iterator<BindingNodeId> convFromBinding(Iterator<Binding> input, BeakGraph bGraph) {
        return Iter.map(input, convFromBinding(bGraph));
    }

    public static Iterator<Binding> convertToNodes(Iterator<BindingNodeId> iterBindingIds, BeakGraph bGraph) {
        return Iter.map(iterBindingIds, bindingNodeIds -> convToBinding(bindingNodeIds, bGraph));
    }

    public static Binding convToBinding(BindingNodeId bindingNodeIds, BeakGraph bGraph) {
        return new BindingBG(bindingNodeIds, bGraph);
    }

    public static Function<Binding, BindingNodeId> convFromBinding(final BeakGraph bGraph) {
        return binding -> SolverLibBeak.convert(binding, bGraph);
    }

    public static BindingNodeId convert(Binding binding, BeakGraph bGraph) {
        if ( binding instanceof BindingBG bindingRaptor ) {
            return bindingRaptor.getBindingId();
        }
        BindingNodeId b = new BindingNodeId(binding);
        Iterator<Var> vars = binding.vars();
        for ( ; vars.hasNext() ; ) {
            Var v = vars.next();
            Node n = binding.get(v);
            if ( n == null )
                // Variable mentioned in the binding but not actually defined.
                // Can occur with BindingProject
                continue;

            // Rely on the node table cache for efficency - we will likely be
            // repeatedly looking up the same node in different bindings.
            NodeId id = bGraph.getReader().getNodeTable().getNodeIdForNode(n);
            // Even put in "does not exist" for a node now known not to be in the DB.
            // Optional: whether to put in "known missing"
            // Currently, we do. The rest of the code should work with either choice.

            // if ( ! NodeId.isDoesNotExist(id) )
            b.put(v, id);
        }
        return b;
    }
}
