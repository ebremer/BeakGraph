package com.ebremer.beakgraph.ng;

import org.apache.jena.graph.Graph;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.main.StageGenerator;

public class StageGeneratorDirectorBG implements StageGenerator {
    private final StageGenerator above;

    public StageGeneratorDirectorBG(StageGenerator original) {
        above = original ;
    }

    @Override
    public QueryIterator execute(BasicPattern pattern, QueryIterator input, ExecutionContext execCxt) {
        Graph g = execCxt.getActiveGraph() ;
        if (!(g instanceof BeakGraph)) {
            return above.execute(pattern, input, execCxt);
        }
        BeakGraph graph = (BeakGraph)g ;
        return PatternMatchBG.execute(graph, pattern, input, null, execCxt);
    }
}
