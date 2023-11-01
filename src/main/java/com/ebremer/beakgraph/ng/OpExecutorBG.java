package com.ebremer.beakgraph.ng;

import org.apache.jena.graph.Graph;
import org.apache.jena.sparql.ARQInternalErrorException;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpDistinct;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpReduced;
import org.apache.jena.sparql.algebra.optimize.TransformFilterPlacement;
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

/**
 *
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
    protected QueryIterator execute(OpDistinct opDistinct, QueryIterator input) {
        return super.execute(opDistinct, input) ;
    }
    
    @Override
    protected QueryIterator execute(OpReduced opReduced, QueryIterator input) {
        return super.execute(opReduced, input) ;
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
            BeakGraph g = (BeakGraph) execCxt.getActiveGraph();
            return PatternMatchBG.execute(g, pattern, input, exprs, execCxt);
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
        
        public OpExecutor create(ExecutionContext execCxt, OpFilter op) {
            return new OpExecutorPlainBeak(execCxt);
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
}
