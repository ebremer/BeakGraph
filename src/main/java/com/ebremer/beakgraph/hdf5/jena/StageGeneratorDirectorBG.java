package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.BeakGraph;
import org.apache.jena.graph.Graph;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.main.StageGenerator;

/**
 * The ARQ-global stage generator: every BGP whose active graph is a BeakGraph
 * but which is NOT executed by {@link OpExecutorBG} lands here - a general
 * dataset that holds BeakGraph views as named graphs, a {@code GRAPH}
 * sub-plan under a demoted outer executor. It plans exactly as the executor
 * does ({@link OpExecutorBG#optimizeExecuteTriples}): most-selective-first
 * reordering, then the id-level solver. It used to hand the pattern to the
 * solver in written order, so those routes joined in whatever order the
 * query author typed (BG-445).
 */
public class StageGeneratorDirectorBG implements StageGenerator {
    private final StageGenerator above;

    public StageGeneratorDirectorBG(StageGenerator original) {
        above = original ;
    }

    @Override
    public QueryIterator execute(BasicPattern pattern, QueryIterator input, ExecutionContext execCxt) {
        Graph g = execCxt.getActiveGraph() ;
        if (!(g instanceof BeakGraph graph)) {
            return above.execute(pattern, input, execCxt);
        }
        return OpExecutorBG.optimizeExecuteTriples(graph, input, pattern, null, execCxt);
    }
}
