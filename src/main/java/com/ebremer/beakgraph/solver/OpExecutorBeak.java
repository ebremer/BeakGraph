package com.ebremer.beakgraph.solver;

import com.ebremer.beakgraph.rdf.BeakGraph;
import org.apache.jena.atlas.logging.Log;
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
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderProc;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderTransformation;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.mgt.Explain;

/**
 *
 * @author erich
 */
public class OpExecutorBeak extends OpExecutor {
    
    public final static OpExecutorFactory opExecFactoryRaptor = new OpExecutorFactory() {
        @Override
        public OpExecutor create(ExecutionContext execCxt) { 
            return new OpExecutorBeak(execCxt) ; 
        }
    };
    
    private final boolean isForRaptor;

    protected OpExecutorBeak(ExecutionContext execCtx) {
	super(execCtx);
	isForRaptor = execCtx.getActiveGraph() instanceof BeakGraph;
    }
    
    @Override
    protected QueryIterator execute(OpFilter opFilter, QueryIterator input) {
        if (!isForRaptor) return super.execute(opFilter, input);
        
//       if (opFilter instanceof OpFilter) throw new Error("BAM");
       Op what = opFilter.getSubOp();
        boolean whatisthis = OpBGP.isBGP(what);
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
        if ( !isForRaptor )
            return super.execute(opBGP, input);
        BeakGraph graph = (BeakGraph)execCxt.getActiveGraph() ;
        return executeBGP(graph, opBGP, input, null, execCxt);
    }
    
    private static QueryIterator executeBGP(BeakGraph graph, OpBGP opBGP, QueryIterator input, ExprList exprs, ExecutionContext execCxt) {
        return optimizeExecuteTriples(graph, input, opBGP.getPattern(), exprs, execCxt) ;
    }
    
    private static QueryIterator optimizeExecuteTriples(BeakGraph graph, QueryIterator input, BasicPattern pattern, ExprList exprs, ExecutionContext execCxt) {
       // System.out.println("optimizeExecuteTriples "+pattern);
        if (!input.hasNext()) return input;
        if ( pattern.size() >= 2 ) {
	    ReorderTransformation transform = graph.getReorderTransform();
	    if ( transform != null ) {
	        QueryIterPeek peek = QueryIterPeek.create(input, execCxt);
	        input = peek;
	        pattern = reorder(pattern, peek, transform) ;
            }
        }
        if ( exprs == null ) {
            Explain.explain("Execute", pattern, execCxt.getContext());
           // Predicate<Tuple<NodeId>> filter = QC2.getFilter(execCxt.getContext());
            BeakGraph g = (BeakGraph) execCxt.getActiveGraph();
            return PatternMatchBeak.execute(g, pattern, input, exprs, execCxt);
        }
        Op op = TransformFilterPlacement.transform(exprs, pattern);
        return plainExecute(op, input, execCxt) ;
    }
    
    private static BasicPattern reorder(BasicPattern pattern, QueryIterPeek peek, ReorderTransformation transform) {
        if (transform!=null) {     
            if (!peek.hasNext())
                throw new ARQInternalErrorException("Peek iterator is already empty") ;
            BasicPattern pattern2 = Substitute.substitute(pattern, peek.peek());
            ReorderProc proc = transform.reorderIndexes(pattern2);
            pattern = proc.reorder(pattern);
        }
        return pattern;
    }
    
    private static QueryIterator plainExecute(Op op, QueryIterator input, ExecutionContext execCxt) {
        ExecutionContextBeak ec2 = new ExecutionContextBeak(execCxt, op);
        ec2.setExecutor(plainFactory);
        return QC2.execute(op, input, ec2) ;
    }
    
    private static final OpExecutorFactory plainFactory = new OpExecutorPlainFactoryRaptor();
    
    private static class OpExecutorPlainFactoryRaptor implements OpExecutorFactory {
        
        @Override
        public OpExecutor create(ExecutionContext execCxt) {
            return new OpExecutorPlainRaptor(execCxt) ;
        }
        
        public OpExecutor create(ExecutionContext execCxt, OpFilter op) {
            return new OpExecutorPlainRaptor(execCxt);
        }
    }
    
    private static class OpExecutorPlainRaptor extends OpExecutor {
        final ExprList filter;
        
        @SuppressWarnings("unchecked")
        public OpExecutorPlainRaptor(ExecutionContext execCxt) {
            super(execCxt);
            ExecutionContextBeak ecr = (ExecutionContextBeak) execCxt;
            filter = ecr.getFilter();
        }
        
        @Override
        public QueryIterator execute(OpBGP opBGP, QueryIterator input) {
            Graph g = execCxt.getActiveGraph();
            if ( g instanceof BeakGraph graphRaptor ) {
                BasicPattern bgp = opBGP.getPattern() ;
                //System.out.println("OPE bgp -> "+bgp);
              //  bgp.forEach(t->{
                //    System.out.println("each - > "+t.getSubject()+" "+t.getPredicate()+" "+t.getObject());
               // });
                Explain.explain("Execute", bgp, execCxt.getContext()) ;
                if (filter!=null) {
                    return PatternMatchBeak.execute(graphRaptor, bgp, input, filter, execCxt);
                }
                return PatternMatchBeak.execute(graphRaptor, bgp, input, filter, execCxt);
            }
            Log.warn(this, "Non-RaptorGraph passed to OpExecutorPlainRaptor");
            return super.execute(opBGP, input) ;
        }
    }
}
