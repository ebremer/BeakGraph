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
        // Reuse the id layer only when it was produced against THIS store's
        // dictionary. Ids are dictionary ranks, so a row from another store
        // (GRAPH <a> joined with GRAPH <b> in one dataset - the stage generator
        // is global) would feed rank 4711 of A's terms straight into B's
        // iterators as an unrelated term. Compare readers, not graphs:
        // BGDatasetGraph.getGraph mints a fresh view per call over one reader.
        if ( binding instanceof BindingBG bindingRaptor
                && bindingRaptor.getGraph().getReader() == bGraph.getReader() ) {
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
            long id = bGraph.getReader().getNodeTable().getNodeIdForNode(n);
            // A layer holds four bindings; an incoming Jena binding can carry
            // more (VALUES with many columns, joins re-entering a BGP, embedded
            // triple-term vars) - chain layers rather than overflow.
            if (b.isFull()) {
                b = new BindingNodeId(b);
            }
            // Record even a "does not exist" id: HDF5Reader.Read short-circuits a pattern
            // bound to it to no rows, and BindingBG falls back to the parent term for output.
            b.put(v, id);
        }
        return b;
    }
}
