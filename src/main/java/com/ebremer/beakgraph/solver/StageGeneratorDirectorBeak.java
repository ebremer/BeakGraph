package com.ebremer.beakgraph.solver;

import com.ebremer.beakgraph.rdf.BeakGraph;
import org.apache.jena.graph.Graph;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.main.StageGenerator;

/** Execute TDB requests directly -- no reordering
 *  Using OpExecutor is preferred.
 */
public class StageGeneratorDirectorBeak implements StageGenerator
{
    // Using OpExecutor is preferred.
    StageGenerator above = null ;

    public StageGeneratorDirectorBeak(StageGenerator original) {
        above = original ;
    }

    @Override
    public QueryIterator execute(BasicPattern pattern, QueryIterator input, ExecutionContext execCxt) {
        // --- In case this isn't for Raptor
        Graph g = execCxt.getActiveGraph() ;
        if (!( g instanceof BeakGraph ))
            // Not us - bounce up the StageGenerator chain
            return above.execute(pattern, input, execCxt);
        BeakGraph graph = (BeakGraph)g ;
        //Predicate<Tuple<NodeId>> filter = QC2.getFilter(execCxt.getContext());
        return PatternMatchBeak.execute(graph, pattern, input, null, execCxt);
    }
}
