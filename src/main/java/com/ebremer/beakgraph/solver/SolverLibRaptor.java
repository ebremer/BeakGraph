package com.ebremer.beakgraph.solver;

import com.ebremer.beakgraph.rdf.NodeTable;
import com.ebremer.beakgraph.store.NodeId;
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
public class SolverLibRaptor {
    
    public static Iterator<BindingNodeId> convertToIds(Iterator<Binding> iterBindings, NodeTable nodeTable) {
        return Iter.map(iterBindings, convFromBinding(nodeTable));
    }
    
    public static Iterator<BindingNodeId> convFromBinding(Iterator<Binding> input, NodeTable nodeTable) {
        return Iter.map(input, convFromBinding(nodeTable));
    }

    public static Iterator<Binding> convertToNodes(Iterator<BindingNodeId> iterBindingIds, NodeTable nodeTable) {
        return Iter.map(iterBindingIds, bindingNodeIds -> convToBinding(bindingNodeIds, nodeTable));
    }

    public static Binding convToBinding(BindingNodeId bindingNodeIds, NodeTable nodeTable) {
        return new BindingRaptor(bindingNodeIds, nodeTable);
    }

    public static Function<Binding, BindingNodeId> convFromBinding(final NodeTable nodeTable) {
        return binding -> SolverLibRaptor.convert(binding, nodeTable);
    }

    public static BindingNodeId convert(Binding binding, NodeTable nodeTable) {
        if ( binding instanceof BindingRaptor bindingRaptor )
            return bindingRaptor.getBindingId();
        BindingNodeId b = new BindingNodeId(binding);
        // and copy over, getting NodeIds.
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
            NodeId id = nodeTable.getNodeIdForNode(n);
            // Even put in "does not exist" for a node now known not to be in the DB.
            // Optional: whether to put in "known missing"
            // Currently, we do. The rest of the code should work with either choice.

            // if ( ! NodeId.isDoesNotExist(id) )
            b.put(v, id);
        }
        return b;
    }

    /*

    static QueryIterator testForGraphName(DatasetGraphTDB ds, Node graphNode, QueryIterator input,
                                                 Predicate<Tuple<NodeId>> filter, ExecutionContext execCxt) {
        NodeId nid = TDBInternal.getNodeId(ds, graphNode);
        boolean exists = !NodeId.isDoesNotExist(nid);
        if ( exists ) {
            // Node exists but is it used in the quad position?
            NodeTupleTable ntt = ds.getQuadTable().getNodeTupleTable();
            // Don't worry about abortable - this iterator should be fast
            // (with normal indexing - at least one G???).
            // Either it finds a starting point, or it doesn't.  We are only
            // interested in the first .hasNext.
            Iterator<Tuple<NodeId>> iter1 = ntt.find(nid, NodeId.NodeIdAny, NodeId.NodeIdAny, NodeId.NodeIdAny);
            if ( filter != null )
                iter1 = Iter.filter(iter1, filter);
            exists = iter1.hasNext();
        }

        if ( exists )
            return input;
        else {
            input.close();
            return QueryIterNullIterator.create(execCxt);
        }
    }

    static QueryIterator graphNames(DatasetGraphTDB ds, Node graphNode, QueryIterator input,
                                           Predicate<Tuple<NodeId>> filter, ExecutionContext execCxt) {
        List<Abortable> killList = new ArrayList<>();
        Iterator<Tuple<NodeId>> iter1 = ds.getQuadTable().getNodeTupleTable().find(NodeId.NodeIdAny, NodeId.NodeIdAny,
                                                                                   NodeId.NodeIdAny, NodeId.NodeIdAny);
        if ( filter != null )
            iter1 = Iter.filter(iter1, filter);

        Iterator<NodeId> iter2 = Iter.map(iter1, t -> t.get(0));
        // Project is cheap - don't brother wrapping iter1
        iter2 = makeAbortable(iter2, killList);

        Iterator<NodeId> iter3 = Iter.distinct(iter2);
        iter3 = makeAbortable(iter3, killList);

        Iterator<Node> iter4 = NodeLib.nodes(ds.getQuadTable().getNodeTupleTable().getNodeTable(), iter3);

        final Var var = Var.alloc(graphNode);
        Iterator<Binding> iterBinding = Iter.map(iter4, node -> BindingFactory.binding(var, node));
        return new QueryIterAbortable(iterBinding, killList, input, execCxt);
    }

    static Set<NodeId> convertToNodeIds(Collection<Node> nodes, DatasetGraphTDB dataset)
    {
        Set<NodeId> graphIds = new HashSet<>();
        NodeTable nt = dataset.getQuadTable().getNodeTupleTable().getNodeTable();
        for ( Node n : nodes )
            graphIds.add(nt.getNodeIdForNode(n));
        return graphIds;
    }

    public static Iterator<Tuple<NodeId>> unionGraph(NodeTupleTable ntt)
    {
        Iterator<Tuple<NodeId>> iter = ntt.find((NodeId)null, null, null, null);
        iter = Iter.map(iter, quadsToAnyTriples);
        //iterMatches = Iter.distinct(iterMatches);

        // This depends on the way indexes are choose and
        // the indexing pattern. It assumes that the index
        // chosen ends in G so same triples are adjacent
        // in a union query.
        /// See TupleTable.scanAllIndex that ensures this.
        iter = Iter.distinctAdjacent(iter);
        return iter;
    }

    private static Function<Tuple<NodeId>, Tuple<NodeId>> quadsToAnyTriples = item -> {
        return TupleFactory.create4(NodeId.NodeIdAny, item.get(1), item.get(2), item.get(3) );
    };
    */
}
