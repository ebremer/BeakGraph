package com.ebremer.beakgraph.core;

import com.ebremer.beakgraph.hdf5.jena.BindingNodeId;
import com.ebremer.beakgraph.hdf5.jena.OpExecutorBG;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapFactory;
import org.apache.jena.sparql.core.DatasetGraphBase;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.sparql.core.TransactionalLock;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.pfunction.PropertyFunctionRegistry;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.util.iterator.WrappedIterator;
import org.apache.jena.vocabulary.RDFS;

/**
 *
 * @author erich
 */
public class BGDatasetGraph extends DatasetGraphBase {
    private final BeakGraph bg;
    // Per-dataset execution wiring (the TDB pattern): the engine merges this context
    // over the global ARQ context when a query runs against this dataset, so BG's
    // OpExecutor and property-function scoping apply here and ONLY here - never to
    // other datasets in the JVM.
    private final Context context = Context.create();
    // Tracks per-thread read-transaction lifecycle (begin/commit/abort/end pairing);
    // the store is immutable, so the lock only provides honest bookkeeping.
    private final Transactional txn = TransactionalLock.createMRSW();

    /**
     * The standard property-function registry minus rdfs:member. BG stores use
     * rdfs:member as a plain stored predicate; Jena's container-membership property
     * function would rewrite those patterns into rdf:_1/rdf:_2... lookups and answer
     * nothing. Scoped here per dataset - the global registry is left untouched.
     */
    private static final class BGPropertyFunctions {
        static final PropertyFunctionRegistry INSTANCE = build();
        private static PropertyFunctionRegistry build() {
            PropertyFunctionRegistry global = PropertyFunctionRegistry.get();
            PropertyFunctionRegistry reg = new PropertyFunctionRegistry();
            global.keys().forEachRemaining(uri -> {
                if (!RDFS.member.getURI().equals(uri)) {
                    reg.put(uri, global.get(uri));
                }
            });
            return reg;
        }
    }

    /**
     * The global filter-function registry plus BeakGraph's geof:sfIntersects
     * evaluator (the JTS verification stage the spatial index relies on),
     * scoped to BG executions: an embedder that registers another
     * implementation globally (jena-geosparql) keeps it for its own datasets,
     * and BG datasets are not at the mercy of initialization order.
     */
    private static final class BGFunctions {
        static final org.apache.jena.sparql.function.FunctionRegistry INSTANCE = build();
        private static org.apache.jena.sparql.function.FunctionRegistry build() {
            org.apache.jena.sparql.function.FunctionRegistry global = org.apache.jena.sparql.function.FunctionRegistry.get();
            org.apache.jena.sparql.function.FunctionRegistry reg = org.apache.jena.sparql.function.FunctionRegistry.createFrom(global);
            reg.put(com.ebremer.ns.GEOF.sfIntersects.getURI(), com.ebremer.beakgraph.turbo.Intersects.class);
            return reg;
        }
    }

    public BGDatasetGraph(BeakGraph g) {
        this.bg = g;
        wire(context);
    }

    /**
     * Installs BeakGraph's execution wiring into {@code context}: the BG
     * OpExecutor factory and the rdfs:member-free property-function registry.
     * Used for this dataset's own context and, by {@link QueryEngineBG}, for
     * the per-execution context of any query whose default graph is a
     * BeakGraph reached some other way (a Model over the graph, a
     * {@code DatasetGraphOne} wrapper) - those never see a BGDatasetGraph
     * context and used to fall back to the global registry, where rdfs:member
     * is rewritten into container membership and answers nothing.
     */
    public static void wire(Context context) {
        QC.setFactory(context, OpExecutorBG.opExecFactoryBG);
        PropertyFunctionRegistry.set(context, BGPropertyFunctions.INSTANCE);
        org.apache.jena.sparql.function.FunctionRegistry.set(context, BGFunctions.INSTANCE);
        // Re-pin CDT support for each dataset: a later ARQ.setStrictMode() call
        // elsewhere in the JVM flips the global off, and BeakGraph's stored
        // cdt: literals need composite semantics to query correctly.
        org.apache.jena.sparql.SystemARQ.EnableCDTs = true;
    }

    @Override
    public Context getContext() {
        return context;
    }
    
    @Override
    public void close() {
        bg.close();
    }
    
    public BeakGraph getBeakGraph() {
        return bg;
    }

    @Override
    public Graph getDefaultGraph() {
        return bg;
    }

    @Override
    public Graph getGraph(Node node) {
        // A non-owning view over the shared reader (closing it is a no-op).
        return new BeakGraph(node, bg.getReader());
    }

    @Override
    public void addGraph(Node graphName, Graph graph) {
        throw new UnsupportedOperationException("BeakGraph is read-only.");
    }

    @Override
    public void removeGraph(Node graphName) {
        throw new UnsupportedOperationException("BeakGraph is read-only.");
    }

    @Override
    public Iterator<Node> listGraphNodes() {
        return bg.getReader().getDictionary().streamGraphs()
                .filter(n -> !Quad.isDefaultGraph(n) && !Quad.isUnionGraph(n))
                .iterator();
    }
    
    @Override
    public boolean containsGraph(Node graphNode) {
        // The union and default graphs are synthetic names that never appear in
        // the stored graphs list, but ARQ gates GRAPH <g> execution on this
        // method - they must answer true here (the TDB convention).
        if (Quad.isUnionGraph(graphNode) || Quad.isDefaultGraph(graphNode)) {
            return true;
        }
        return bg.getReader().containsGraph(graphNode);
    }

    @Override
    public Iterator<Quad> find(Node g, Node s, Node p, Node o) {
        // If the graph is ANY (Wildcard), we must iterate over all known graphs
        // because the underlying indices (GSPO) require a concrete Graph ID 
        // to jump to the correct segment.
        if (g == null || Node.ANY.equals(g)) {
            return findInAnyGraph(s, p, o);
        }
        
        // Concrete Graph Search
        return findInSpecificGraph(g, s, p, o);
    }
    
    @Override
    public Iterator<Quad> findNG(Node g, Node s, Node p, Node o) {
        // findNG matches NAMED graphs only - unlike find(ANY,...), the default
        // graph's quads are excluded.
        if (Quad.isUnionGraph(g)) {
            return findInSpecificGraph(g, s, p, o); // read() handles the union semantics
        }
        if (g == null || Node.ANY.equals(g)) {
            // Lazy per-graph chaining - see findInAnyGraph.
            return org.apache.jena.atlas.iterator.Iter.flatMap(listGraphNodes(),
                    gn -> findInSpecificGraph(gn, s, p, o));
        }
        if (Quad.isDefaultGraph(g)) {
            return Collections.emptyIterator();
        }
        return findInSpecificGraph(g, s, p, o);
    }

    private Iterator<Quad> findInSpecificGraph(Node g, Node s, Node p, Node o) {
        // Map Node.ANY to Variables for the binding system
        Var sVar = Var.alloc("s");
        Var pVar = Var.alloc("p");
        Var oVar = Var.alloc("o");

        Node sPattern = (s == null || Node.ANY.equals(s)) ? sVar : s;
        Node pPattern = (p == null || Node.ANY.equals(p)) ? pVar : p;
        Node oPattern = (o == null || Node.ANY.equals(o)) ? oVar : o;

        Triple triplePattern = Triple.create(sPattern, pPattern, oPattern);
        NodeTable nodeTable = bg.getReader().getNodeTable();

        // Execute Read against the specific graph
        Iterator<BindingNodeId> it = bg.getReader().read(g, new BindingNodeId(), triplePattern, null, nodeTable);

        // Convert Bindings to Quads
        return WrappedIterator.create(it).mapWith(bnid -> {
            Node sRes = sPattern.isConcrete() ? s : nodeTable.getNodeForNodeId(bnid.get(sVar));
            Node pRes = pPattern.isConcrete() ? p : nodeTable.getNodeForNodeId(bnid.get(pVar));
            Node oRes = oPattern.isConcrete() ? o : nodeTable.getNodeForNodeId(bnid.get(oVar));
            return Quad.create(g, sRes, pRes, oRes);
        });
    }

    private Iterator<Quad> findInAnyGraph(Node s, Node p, Node o) {
        // Lazily chain the default graph and every named graph: constructing
        // each graph's iterator up front paid its index searches before the
        // first quad came back, and spatial stores hold thousands of tile
        // graphs. Skip default if it appears in the graph list (duplicates).
        Iterator<Node> graphs = org.apache.jena.atlas.iterator.Iter.concat(
                List.of(Quad.defaultGraphIRI).iterator(),
                org.apache.jena.atlas.iterator.Iter.filter(listGraphNodes(),
                        gn -> !gn.equals(Quad.defaultGraphIRI)));
        return org.apache.jena.atlas.iterator.Iter.flatMap(graphs, gn -> findInSpecificGraph(gn, s, p, o));
    }

    // One mutable prefix map per dataset: the old implementation built a whole
    // fresh in-memory dataset on EVERY call and returned its (disconnected)
    // prefixes, so registrations silently vanished between calls.
    private final PrefixMap prefixMap = PrefixMapFactory.create();

    @Override
    public PrefixMap prefixes() {
        return prefixMap;
    }

    // --- Minimal Transaction Support (Read-Only) ---
    
    @Override
    public boolean supportsTransactions() {
        return true;
    }

    @Override
    public void begin(TxnType type) {
        if (type == TxnType.WRITE) throw new UnsupportedOperationException("Write transactions not supported");
        txn.begin(type);
    }

    @Override
    public void begin(ReadWrite readWrite) {
        if (readWrite == ReadWrite.WRITE) throw new UnsupportedOperationException("Write transactions not supported");
        txn.begin(readWrite);
    }

    @Override
    public boolean promote(Promote mode) {
        return false;
    }

    @Override
    public void commit() {
        txn.commit();
    }

    @Override
    public void abort() {
        txn.abort();
    }

    @Override
    public void end() {
        txn.end();
    }

    @Override
    public ReadWrite transactionMode() {
        return txn.transactionMode();
    }

    @Override
    public TxnType transactionType() {
        return txn.transactionType();
    }

    @Override
    public boolean isInTransaction() {
        // Real per-thread state, not an unconditional "true": lying that a
        // transaction is always active masked mispaired begin/end in callers.
        // The data itself is immutable, so reads need no lock protection - the
        // delegate exists purely to track lifecycle honestly.
        return txn.isInTransaction();
    }
}
