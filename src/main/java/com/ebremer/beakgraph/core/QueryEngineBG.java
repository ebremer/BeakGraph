package com.ebremer.beakgraph.core;

import org.apache.jena.graph.Graph;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryEngineFactory;
import org.apache.jena.sparql.engine.QueryEngineRegistry;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.main.QueryEngineMain;
import org.apache.jena.sparql.graph.GraphWrapper;
import org.apache.jena.sparql.util.Context;

/**
 * The query engine for any dataset whose default graph is a {@link BeakGraph},
 * however that dataset was built. BeakGraph's execution wiring (its
 * OpExecutor and property-function registry) used to live only in
 * {@link BGDatasetGraph}'s context, so SPARQL run over
 * {@code ds.getDefaultModel()} or {@code ModelFactory.createModelForGraph(bg)}
 * - Jena wraps such a Model in a fresh {@code DatasetGraphOne} - silently
 * lost it: rdfs:member patterns were rewritten into container membership and
 * answered nothing, and join reordering, filter pushdown, spatial seeding and
 * the DISTINCT/COUNT fast paths were bypassed. This engine (the TDB pattern:
 * a {@link QueryEngineFactory} registered ahead of the default) accepts those
 * datasets and installs the wiring into each execution's own context.
 */
public final class QueryEngineBG extends QueryEngineMain {

    public static final QueryEngineFactory FACTORY = new QueryEngineFactory() {
        @Override
        public boolean accept(Query query, DatasetGraph dsg, Context context) {
            return isBeakGraphDataset(dsg);
        }

        @Override
        public Plan create(Query query, DatasetGraph dsg, Binding input, Context context) {
            return new QueryEngineBG(query, dsg, input, context).getPlan();
        }

        @Override
        public boolean accept(Op op, DatasetGraph dsg, Context context) {
            return isBeakGraphDataset(dsg);
        }

        @Override
        public Plan create(Op op, DatasetGraph dsg, Binding input, Context context) {
            return new QueryEngineBG(op, dsg, input, context).getPlan();
        }
    };

    private static volatile boolean registered;

    /** Registers {@link #FACTORY} once; consulted before Jena's default engine. */
    public static synchronized void register() {
        if (!registered) {
            QueryEngineRegistry.addFactory(FACTORY);
            registered = true;
        }
    }

    /** True for a {@link BGDatasetGraph} or any dataset whose (unwrapped) default graph is a BeakGraph. */
    public static boolean isBeakGraphDataset(DatasetGraph dsg) {
        if (dsg == null) {
            return false;
        }
        if (dsg instanceof BGDatasetGraph) {
            return true;
        }
        Graph g;
        try {
            g = dsg.getDefaultGraph();
        } catch (RuntimeException unsupported) {
            return false;
        }
        while (g instanceof GraphWrapper w) {
            g = w.get();
        }
        return g instanceof BeakGraph;
    }

    private QueryEngineBG(Query query, DatasetGraph dsg, Binding input, Context context) {
        super(query, dsg, input, context);
        BGDatasetGraph.wire(this.context);
    }

    private QueryEngineBG(Op op, DatasetGraph dsg, Binding input, Context context) {
        super(op, dsg, input, context);
        BGDatasetGraph.wire(this.context);
    }
}
