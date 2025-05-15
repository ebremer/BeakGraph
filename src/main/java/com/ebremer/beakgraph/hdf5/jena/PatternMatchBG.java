package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.BeakGraph;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.Abortable;
import org.apache.jena.sparql.engine.iterator.QueryIterAbortable;
import org.apache.jena.sparql.expr.ExprList;
import static org.apache.jena.sparql.engine.main.solver.SolverLib.makeAbortable;

/**
 *
 * @author erich
 */
public class PatternMatchBG {

    public static QueryIterator execute(BeakGraph bGraph, BasicPattern bgp, QueryIterator input, ExprList filter, ExecutionContext execCxt) {
        //Set<String> list = bgp.getList().stream().map(triple -> triple.getPredicate().toString()).collect(Collectors.toSet());
        List<Triple> triples = bgp.getList();
        List<Abortable> killList = new ArrayList<>();
        Iterator<BindingNodeId> chain = Iter.map(input, SolverLibBeak.convFromBinding(bGraph));
        for ( Triple triple : triples ) {
            chain = solve(bGraph, triple, filter, chain, execCxt);
            chain = makeAbortable(chain, killList);
        }
        Iterator<Binding> iterBinding = SolverLibBeak.convertToNodes(chain, bGraph);
        iterBinding = makeAbortable(iterBinding, killList);
        return new QueryIterAbortable(iterBinding, killList, input, execCxt);
    }

    private static Iterator<BindingNodeId> solve(BeakGraph bGraph, Triple triple, ExprList filter, Iterator<BindingNodeId> chain, ExecutionContext execCxt) {     
        Function<BindingNodeId, Iterator<BindingNodeId>> step = bnid -> find(bGraph, bnid, triple, filter, execCxt);
        return Iter.flatMap(chain, step);
    }
    
    private static Iterator<BindingNodeId> find(BeakGraph bGraph, BindingNodeId bnid, Triple xPattern, ExprList filter, ExecutionContext execCxt) {
        return bGraph.getReader().Read(bGraph.getNamedGraph(), bnid, xPattern, filter, bGraph.getReader().getNodeTable());
    }
}
