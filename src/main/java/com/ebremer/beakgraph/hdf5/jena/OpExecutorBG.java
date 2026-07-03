package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.BeakGraph;
import org.apache.jena.graph.Graph;
import org.apache.jena.sparql.ARQInternalErrorException;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Substitute;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.iterator.QueryIterPeek;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderProc;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderTransformation;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.algebra.optimize.TransformFilterPlacement;

/**
 * Custom OpExecutor for BeakGraph.
 * Optimized for Big Data Triple patterns and Dictionary Streaming (SELECT DISTINCT ?p, ?g, etc.).
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

    // NOTE: an earlier version intercepted OpDistinct ("SELECT DISTINCT ?p { ?s ?p ?o }")
    // and streamed the dictionary's per-position id lists instead of executing the
    // query. That was removed as unsound: the columnar lists are file-global (they
    // span every named graph, including the always-present VoID metadata graph, so
    // default-graph queries over-reported terms), any FILTER wrapped around the
    // pattern was silently dropped, DISTINCT ?g included the default graph, and the
    // incoming iterator (join semantics) was discarded. A correct fast path would
    // need per-graph id lists in the format plus filter/join guards; until then
    // DISTINCT executes normally.

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
        // Jena 6: ExecutionContext is final, so the placed filter can no longer
        // ride on a context subclass (the old ExecutionContextBG). It rides on a
        // per-call executor FACTORY instead - immutable and per-execution, so
        // executors created lazily during iteration (e.g. substitution joins
        // re-executing the RHS per binding) still see exactly their op's filter.
        ExprList filter = (op instanceof OpFilter opFilter) ? opFilter.getExprs() : null;
        ExecutionContext ec = ExecutionContext.copyChangeExecutor(execCxt, new OpExecutorPlainFactoryBeak(filter));
        return QC.execute(op, input, ec) ;
    }

    private static class OpExecutorPlainFactoryBeak implements OpExecutorFactory {
        private final ExprList filter;

        OpExecutorPlainFactoryBeak(ExprList filter) {
            this.filter = filter;
        }

        @Override
        public OpExecutor create(ExecutionContext execCxt) {
            return new OpExecutorPlainBeak(execCxt, filter) ;
        }
    }

    private static class OpExecutorPlainBeak extends OpExecutor {
        final ExprList filter;

        public OpExecutorPlainBeak(ExecutionContext execCxt, ExprList filter) {
            super(execCxt);
            this.filter = filter;
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
}
