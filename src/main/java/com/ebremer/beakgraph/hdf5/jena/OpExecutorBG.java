package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.ARQInternalErrorException;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Substitute;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.iterator.QueryIterPeek;
import org.apache.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderProc;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderTransformation;
import org.apache.jena.sparql.expr.ExprList;

import java.util.Iterator;
import java.util.stream.Stream;
import org.apache.jena.sparql.algebra.optimize.TransformFilterPlacement;

/**
 * Custom OpExecutor for BeakGraph.
 * Optimized for Big Data Triple patterns and Dictionary Streaming (SELECT DISTINCT ?p).
 * @author erich
 */
public class OpExecutorBG extends OpExecutor {
        
    public final static OpExecutorFactory opExecFactoryBG = new OpExecutorFactory() {
        @Override
        public OpExecutor create(ExecutionContext execCxt) {
            return new OpExecutorBG(execCxt);
        }
    };

    private final boolean isForBeakGraph;

    public OpExecutorBG(ExecutionContext execCtx) {
        super(execCtx);
        isForBeakGraph = execCtx.getActiveGraph() instanceof BeakGraph;
    }

    /**
     * OPTIMIZATION: Overriding execute for OpDistinct to catch schema-listing queries.
     * Logic: If we are asking for DISTINCT predicates across a triple or quad pattern,
     * we skip the index scan and stream directly from the Predicate Dictionary.
     */
    @Override
    protected QueryIterator execute(OpDistinct opDistinct, QueryIterator input) {
        if (!isForBeakGraph) {
            return super.execute(opDistinct, input);
        }

        Op subOp = opDistinct.getSubOp();
        
        // Pattern: Distinct -> Project(?p) -> BGP(?s ?p ?o)
        if (subOp instanceof OpProject project && project.getVars().size() == 1) {
            Var pVar = project.getVars().get(0);
            Op innerOp = project.getSubOp();

            if (isFullPredicateScan(innerOp, pVar)) {
                // Determine if we can resolve the BeakGraph
                Graph active = execCxt.getActiveGraph();
                if (active instanceof BeakGraph bg) {
                    PositionalDictionaryReader dict = (PositionalDictionaryReader) bg.getReader().getDictionary();
                    return new QueryIterDictionary(pVar, dict.getPredicates().streamNodes(), execCxt);
                }
            }
        }
        
        return super.execute(opDistinct, input);
    }

    /**
     * Recursively checks if the pattern is a simple "?s ?p ?o" scan that encompasses 
     * the entire predicate space.
     */
    private boolean isFullPredicateScan(Op op, Var pVar) {
        // Unwrap OpFilter (if any exists around the pattern)
        if (op instanceof OpFilter filter) {
            return isFullPredicateScan(filter.getSubOp(), pVar);
        }

        // Case 1: Triple { ?s ?p ?o }
        if (op instanceof OpBGP bgp && bgp.getPattern().size() == 1) {
            Triple t = bgp.getPattern().get(0);
            return t.getSubject().isVariable() && 
                   t.getPredicate().isVariable() && t.getPredicate().equals(pVar) && 
                   t.getObject().isVariable();
        }

        // Case 2: Quad { GRAPH ?g { ?s ?p ?o } }
        if (op instanceof OpQuadPattern qp && qp.getPattern().size() == 1) {
            Quad q = qp.getPattern().get(0);
            return q.getGraph().isVariable() && 
                   q.getSubject().isVariable() && 
                   q.getPredicate().isVariable() && q.getPredicate().equals(pVar) && 
                   q.getObject().isVariable();
        }

        // Case 3: OpGraph ?g { ... }
        if (op instanceof OpGraph opGraph && opGraph.getNode().isVariable()) {
            return isFullPredicateScan(opGraph.getSubOp(), pVar);
        }

        return false;
    }

    @Override
    protected QueryIterator execute(OpPropFunc opPropFunc, QueryIterator input) {
        return super.execute(opPropFunc, input);
    }
    
    @Override
    protected QueryIterator execute(OpFilter opFilter, QueryIterator input) {
        if (!isForBeakGraph) {
            return super.execute(opFilter, input);
        }        
        if (OpBGP.isBGP(opFilter.getSubOp())) {
            BeakGraph graph = (BeakGraph)execCxt.getActiveGraph();
            OpBGP opBGP = (OpBGP)opFilter.getSubOp();
            return executeBGP(graph, opBGP, input, opFilter.getExprs(), execCxt);
        }
        return super.execute(opFilter, input);
    }

    @Override
    protected QueryIterator execute(OpBGP opBGP, QueryIterator input) {
        if ( !isForBeakGraph ) {
            return super.execute(opBGP, input);
        }
        BeakGraph graph = (BeakGraph)execCxt.getActiveGraph() ;
        return executeBGP(graph, opBGP, input, null, execCxt);
    }
    
    private static QueryIterator executeBGP(BeakGraph graph, OpBGP opBGP, QueryIterator input, ExprList exprs, ExecutionContext execCxt) {
        return optimizeExecuteTriples(graph, input, opBGP.getPattern(), exprs, execCxt) ;
    }
    
    private static QueryIterator optimizeExecuteTriples(BeakGraph graph, QueryIterator input, BasicPattern pattern, ExprList exprs, ExecutionContext execCxt) {
        if (!input.hasNext()) {
            return input;
        }
        if ( pattern.size() >= 2 ) {
            ReorderTransformation transform = graph.getReorderTransform();
            if ( transform != null ) {
                QueryIterPeek peek = QueryIterPeek.create(input, execCxt);
                input = peek;
                pattern = reorder(pattern, peek, transform) ;
            }
        }
        if ( exprs == null ) {
            return PatternMatchBG.execute(graph, pattern, input, exprs, execCxt);
        }
        Op op = TransformFilterPlacement.transform(exprs, pattern);
        return plainExecute(op, input, execCxt) ;
    }
    
    private static BasicPattern reorder(BasicPattern pattern, QueryIterPeek peek, ReorderTransformation transform) {
        if (transform!=null) {     
            if (!peek.hasNext()) {
                throw new ARQInternalErrorException("Peek iterator is already empty");
            }
            BasicPattern pattern2 = Substitute.substitute(pattern, peek.peek());
            ReorderProc proc = transform.reorderIndexes(pattern2);
            pattern = proc.reorder(pattern);
        }
        return pattern;
    }
    
    private static QueryIterator plainExecute(Op op, QueryIterator input, ExecutionContext execCxt) {
        ExecutionContextBG ec = new ExecutionContextBG(execCxt, op);
        ec.setExecutor(plainFactory);
        return QC.execute(op, input, ec) ;
    }
    
    private static final OpExecutorFactory plainFactory = new OpExecutorPlainFactoryBeak();
    
    private static class OpExecutorPlainFactoryBeak implements OpExecutorFactory {
        @Override
        public OpExecutor create(ExecutionContext execCxt) {
            return new OpExecutorPlainBeak(execCxt) ;
        }
    }
    
    private static class OpExecutorPlainBeak extends OpExecutor {
        final ExprList filter;
        
        public OpExecutorPlainBeak(ExecutionContext execCxt) {
            super(execCxt);
            ExecutionContextBG ecr = (ExecutionContextBG) execCxt;
            filter = ecr.getFilter();
        }
        
        @Override
        public QueryIterator execute(OpBGP opBGP, QueryIterator input) {
            Graph g = execCxt.getActiveGraph();
            if ( g instanceof BeakGraph graphRaptor ) {
                BasicPattern bgp = opBGP.getPattern() ;
                return PatternMatchBG.execute(graphRaptor, bgp, input, filter, execCxt);
            }
            return super.execute(opBGP, input) ;
        }
    }

    /**
     * Inner class to wrap the Dictionary Stream as a Jena QueryIterator.
     */
    private static class QueryIterDictionary extends QueryIterPlainWrapper {
        public QueryIterDictionary(Var var, Stream<Node> nodes, ExecutionContext execCxt) {
            super(convert(var, nodes.iterator()), execCxt);
        }

        private static Iterator<Binding> convert(Var var, Iterator<Node> nodes) {
            return new Iterator<>() {
                @Override public boolean hasNext() { return nodes.hasNext(); }
                @Override public Binding next() {
                    return BindingFactory.binding(var, nodes.next());
                }
            };
        }
    }
}
