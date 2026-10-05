package com.ebremer.beakgraph.core;

import org.apache.jena.sparql.graph.NodeTransformLib;
import org.apache.jena.sparql.engine.iterator.QueryIterConvert;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.ExecutionContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.DatasetDescription;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphMapLink;
import org.apache.jena.sparql.core.DynamicDatasets;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryEngineFactory;
import org.apache.jena.sparql.engine.QueryEngineRegistry;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.main.QueryEngineMain;
import org.apache.jena.sparql.graph.GraphOps;
import org.apache.jena.sparql.graph.GraphWrapper;
import org.apache.jena.sparql.graph.GraphZero;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.sparql.util.NodeUtils;
import com.ebremer.beakgraph.hdf5.jena.BGReader;

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
 * <p>
 * It also builds the dataset a {@code FROM} / {@code FROM NAMED} clause (or
 * the SPARQL protocol's {@code default-graph-uri}) asks for. Jena's generic
 * {@code DynamicDatasets} makes the default graph a {@code GraphUnionRead}
 * even for a single {@code FROM <g>}, so the active graph was no longer a
 * BeakGraph: every BGP went to Jena's executor over {@code Graph.find} with
 * a HashSet of materialised triples for de-duplication, and the id-level
 * joins, range and spatial pushdown, reordering, parallel scans and the
 * DISTINCT/COUNT fast paths were all lost (BG-336). Here the default graph
 * is the BeakGraph view itself (one graph) or a graph-set view (several,
 * see {@link BeakGraph#BeakGraph(List, BGReader)}), with the same dataset
 * semantics as Jena's construction.
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

    // The store's document base, when the BeakGraph has one: absolute IRIs
    // the query names under it are rewritten to the stored relative form
    // before planning, and every result binding is resolved on the way out -
    // the servlets' behaviour, now for any ARQ execution over the graph (BG-396).
    private final RelativeIRIResolver resolver;
    private final java.util.function.Predicate<Node> storedTerm;

    private QueryEngineBG(Query query, DatasetGraph dsg, Binding input, Context context) {
        super(query, dsg, input, context);
        BGDatasetGraph.wire(this.context);
        BeakGraph bg = beakGraphOf(dsg);
        this.resolver = (bg == null) ? null : bg.resolver();
        this.storedTerm = (bg == null) ? null : bg.storedTerm();
    }

    private QueryEngineBG(Op op, DatasetGraph dsg, Binding input, Context context) {
        super(op, dsg, input, context);
        BGDatasetGraph.wire(this.context);
        BeakGraph bg = beakGraphOf(dsg);
        this.resolver = (bg == null) ? null : bg.resolver();
        this.storedTerm = (bg == null) ? null : bg.storedTerm();
    }

    /** The BeakGraph a dataset is built on: a BGDatasetGraph's own, else its (unwrapped) default graph. */
    public static BeakGraph beakGraphOf(DatasetGraph dsg) {
        if (dsg instanceof BGDatasetGraph b) {
            return b.getBeakGraph();
        }
        Graph g;
        try {
            g = dsg.getDefaultGraph();
        } catch (RuntimeException unsupported) {
            return null;
        }
        while (g instanceof GraphWrapper w) {
            g = w.get();
        }
        return (g instanceof BeakGraph bg) ? bg : null;
    }

    @Override
    protected Op modifyOp(Op op) {
        Op out = super.modifyOp(op);
        if (resolver != null) {
            out = NodeTransformLib.transform(resolver.absoluteToStorage(storedTerm), out);
        }
        return out;
    }

    @Override
    public QueryIterator eval(Op op, DatasetGraph dsg, Binding input, Context context) {
        QueryIterator it = super.eval(op, dsg, input, context);
        if (resolver == null) {
            return it;
        }
        ExecutionContext execCxt = ExecutionContext.create(dsg, dsg.getDefaultGraph(), context);
        return new QueryIterConvert(it, resolver::resolve, execCxt);
    }

    @Override
    protected DatasetGraph dynamicDataset(DatasetDescription dsDesc, DatasetGraph dsg, boolean defaultUnionGraph) {
        if (!(dsg instanceof BGDatasetGraph bg) || dsDesc == null || dsDesc.isEmpty()) {
            return super.dynamicDataset(dsDesc, dsg, defaultUnionGraph);
        }
        return dynamicDataset(bg, dsDesc, defaultUnionGraph);
    }

    /**
     * The BeakGraph-backed equivalent of {@code DynamicDatasets.dynamicDataset}:
     * same graph selection rules, same context handling and marker symbols,
     * but every graph is a BeakGraph view.
     */
    public static DatasetGraph dynamicDataset(BGDatasetGraph bg, DatasetDescription dsDesc, boolean defaultUnionGraph) {
        Set<Node> defaults = NodeUtils.convertToSetNodes(dsDesc.getDefaultGraphURIs());
        Set<Node> named = NodeUtils.convertToSetNodes(dsDesc.getNamedGraphURIs());
        DatasetGraph dsg2 = new DatasetGraphMapLink(defaultGraphFor(bg, defaults, defaultUnionGraph));
        for (Node gn : named) {
            if (Quad.isUnionGraph(gn)) {
                continue;
            }
            Graph g = GraphOps.getGraph(bg, gn);
            if (g != null) {
                dsg2.addGraph(gn, g);
            }
        }
        dsg2.getContext().putAll(bg.getContext());
        DatasetGraph dyn = new BGDynamicDatasetGraph(dsg2, bg);
        dyn.getContext().set(ARQConstants.symDatasetDefaultGraphs, defaults);
        dyn.getContext().set(ARQConstants.symDatasetNamedGraphs, named);
        return dyn;
    }

    /**
     * Jena's dynamic-dataset marker type (what the engine and Fuseki inspect),
     * minus its read-only graph wrapping: {@code DatasetGraphReadOnly} hands
     * out {@code GraphReadOnly} views, which would hide the BeakGraph type
     * from the executor. The views are read-only by construction anyway.
     */
    private static final class BGDynamicDatasetGraph extends DynamicDatasets.DynamicDatasetGraph {
        BGDynamicDatasetGraph(DatasetGraph viewDsg, DatasetGraph original) {
            super(viewDsg, original);
        }

        @Override
        public Graph getDefaultGraph() {
            return getR().getDefaultGraph();
        }

        @Override
        public Graph getGraph(Node graphNode) {
            return getR().getGraph(graphNode);
        }
    }

    private static Graph defaultGraphFor(BGDatasetGraph bg, Set<Node> defaults, boolean defaultUnionGraph) {
        BGReader reader = bg.getBeakGraph().getReader();
        if (defaultUnionGraph || defaults.contains(Quad.unionGraph)) {
            if (!defaults.contains(Quad.defaultGraphIRI)) {
                return bg.getGraph(Quad.unionGraph);
            }
            // Union of all named graphs plus the default graph.
            List<Node> members = new ArrayList<>();
            members.add(Quad.defaultGraphIRI);
            bg.listGraphNodes().forEachRemaining(members::add);
            return new BeakGraph(members, reader, bg.getBeakGraph());
        }
        if (defaults.isEmpty()) {
            return GraphZero.instance(); // FROM NAMED only: the default graph is empty
        }
        if (defaults.size() == 1) {
            Node g = defaults.iterator().next();
            return Quad.isDefaultGraph(g) ? bg.getDefaultGraph() : bg.getGraph(g);
        }
        return new BeakGraph(new ArrayList<>(defaults), reader, bg.getBeakGraph());
    }
}
