package com.ebremer.beakgraph.solver;

import com.ebremer.beakgraph.rdf.BeakGraph;
import com.ebremer.beakgraph.rdf.NodeTable;
import com.ebremer.beakgraph.rdf.RaptorReader;
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
public class PatternMatchRaptor {

    public static QueryIterator execute(BeakGraph g, BasicPattern bgp, QueryIterator input, ExprList filter, ExecutionContext execCxt) {
        //System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$ QueryIterator execute : "+bgp+" FFF--->>>  "+filter);
        List<Triple> triples = bgp.getList();
        List<Abortable> killList = new ArrayList<>();
        NodeTable nodeTable = g.getReader().getNodeTable();
        Iterator<BindingNodeId> chain = Iter.map(input, SolverLibRaptor.convFromBinding(nodeTable));
        for ( Triple triple : triples ) {
            //System.out.println("Triple List "+triple);
            chain = solve(g, triple, filter, chain, execCxt);
            chain = makeAbortable(chain, killList);
        }
        Iterator<Binding> iterBinding = SolverLibRaptor.convertToNodes(chain, nodeTable);
        iterBinding = makeAbortable(iterBinding, killList);
        return new QueryIterAbortable(iterBinding, killList, input, execCxt);
    }

    private static Iterator<BindingNodeId> solve(BeakGraph graph, Triple triple, ExprList filter, Iterator<BindingNodeId> chain, ExecutionContext execCxt) {     
        //System.out.println("SOLVE!");
        Function<BindingNodeId, Iterator<BindingNodeId>> step = bnid -> find(graph, bnid, triple, filter, execCxt);
        return Iter.flatMap(chain, step);
        
    }
    
    private static Iterator<BindingNodeId> find(BeakGraph graph, BindingNodeId bnid, Triple xPattern, ExprList filter, ExecutionContext execCxt) {
        //System.out.println("PatternMatchRaptor "+bnid);
        NodeTable nodeTable = graph.getReader().getNodeTable();
        RaptorReader r = graph.getReader();
        return r.Read(bnid, xPattern, filter, nodeTable);
    }
}
