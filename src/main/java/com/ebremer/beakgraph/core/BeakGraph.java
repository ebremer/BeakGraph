package com.ebremer.beakgraph.core;

import com.ebremer.beakgraph.hdf5.jena.BGReader;
import com.ebremer.beakgraph.hdf5.jena.BindingNodeId;
import com.ebremer.beakgraph.hdf5.jena.StageGeneratorDirectorBG;
import com.ebremer.beakgraph.turbo.Spatial;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.shared.AddDeniedException;
import org.apache.jena.shared.DeleteDeniedException;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.main.StageBuilder;
import org.apache.jena.sparql.engine.main.StageGenerator;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderLib;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderTransformation;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.graph.NodeTransform;
import org.apache.jena.sparql.graph.NodeTransformLib;
import org.apache.jena.sparql.graph.NodeTransform;
import org.apache.jena.sparql.graph.NodeTransformLib;
import org.apache.jena.sparql.graph.NodeTransform;
import org.apache.jena.sparql.graph.NodeTransformLib;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.sys.JenaSystem;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author erich
 */
public class BeakGraph extends GraphBase implements AutoCloseable {
    private static final Object initLock = new Object() ;
    private static volatile boolean initialized = false ;
    private final Node namedgraph;
    private final BGReader reader;
    // Non-null only for a GRAPH-SET view - the default graph of a FROM <g1>
    // FROM <g2> dataset (QueryEngineBG): the set union of the members, answered
    // by the reader's union read with id-level de-duplication. Such a view has
    // no stored graph of its own; namedgraph is the synthetic GRAPH_SET.
    private final List<Node> memberGraphs;
    // Whether this instance owns (and on close() must close) the reader. Named-graph
    // views handed out by BGDatasetGraph.getGraph share the dataset's reader; closing
    // such a view must not shut the storage under every other graph of the dataset.
    private final boolean ownsReader;
    private static final Logger logger = LoggerFactory.getLogger(BeakGraph.class);
    private final URI uri;
    // The document base document-relative IRIs (<>, <image.png>) stored in the
    // file resolve against - the URL the store is served from. Null when the
    // API user gave none and the source is not remote: terms are then handed
    // out in their stored (relative) form. The constructor used to accept and
    // ignore it (BG-275); the query engine and the dataset graph now honour it
    // (BG-396).
    private final URI base;
    // Lazily-computed triple count for this graph. -1 = not yet computed; the graph
    // is read-only so the value is stable once counted.
    private int cachedSize = -1;
    // Lazily-built join-reorder transform (read-only graph -> built once, then reused).
    // The statistics are DATASET-wide (VoidStats reads the whole VoID graph), so
    // every view handed out by BGDatasetGraph.getGraph shares its root graph's
    // transform instead of reloading the stats per view - GRAPH ?g used to load
    // them once per named graph per query (BG-1).
    private volatile ReorderTransformation reorderTransform;
    private final BeakGraph root;

    static {
        JenaSystem.init();
        Spatial.init();
        com.ebremer.halcyon.hilbert.WKTDatatype.register();
    }

    /** As {@link #BeakGraph(BGReader, URI, URI)} with the base {@link #defaultBase} derives from {@code uri}. */
    public BeakGraph(BGReader reader, URI uri) {
        this(reader, uri, defaultBase(uri));
    }

    /**
     * The dataset-owning graph over {@code reader}: closing it closes the reader.
     *
     * @param uri  the store's identity ({@link #getURI()})
     * @param base the URL the store is served from, against which its
     *             document-relative IRIs resolve in query results, patterns
     *             and {@code find()} - or null to hand them out as stored
     */
    public BeakGraph(BGReader reader, URI uri, URI base) {
        logger.trace("BeakGraph -> {}", uri.toString());
        init();
        this.uri = uri;
        this.base = base;
        this.reader = reader;
        this.namedgraph = Quad.defaultGraphIRI;
        this.memberGraphs = null;
        this.ownsReader = true;
        this.root = null;
    }

    /**
     * The base a store identified by {@code uri} resolves against unless told
     * otherwise: the URL itself for a remote (http/https) store - what a
     * client fetching it would resolve its relative references against - and
     * none for a local file, whose location says nothing about where the
     * data is published.
     */
    public static URI defaultBase(URI uri) {
        if (uri == null || uri.getScheme() == null) {
            return null;
        }
        String scheme = uri.getScheme().toLowerCase(java.util.Locale.ROOT);
        return (scheme.equals("http") || scheme.equals("https")) ? uri : null;
    }

    /** The document base relative IRIs resolve against, or null (terms are served as stored). */
    public URI getBase() {
        return (root != null) ? root.getBase() : base;
    }

    /** The resolver for {@link #getBase()}, or null when there is no base. */
    public RelativeIRIResolver resolver() {
        URI b = getBase();
        if (b == null) {
            return null;
        }
        RelativeIRIResolver r = new RelativeIRIResolver(b.toString());
        return r.isActive() ? r : null;
    }

    /** Whether {@code n} is a term of this store - the probe the resolver's input rewrite needs. */
    public java.util.function.Predicate<Node> storedTerm() {
        NodeTable nodeTable = reader.getNodeTable();
        return n -> !com.ebremer.beakgraph.hdf5.jena.NodeId.isDoesNotExist(nodeTable.getNodeIdForNode(n));
    }

    /** The synthetic name a graph-set view reports from {@link #getNamedGraph()}; never a stored graph. */
    public static final Node GRAPH_SET = NodeFactory.createURI("urn:x-beakgraph:graph-set");

    /**
     * A non-owning view over the SET UNION of {@code members} (each a stored
     * graph name, the default graph, or {@code urn:x-arq:UnionGraph}) - what a
     * {@code FROM <g1> FROM <g2>} dataset clause makes the default graph.
     * Jena's own {@code GraphUnionRead} would answer that with per-member
     * {@code find()} calls de-duplicated in a HashSet of materialised triples;
     * this view keeps the id-level engine (BG-336).
     */
    public BeakGraph(List<Node> members, BGReader reader) {
        this(members, reader, null);
    }

    /** As {@link #BeakGraph(List, BGReader)}, sharing {@code root}'s reorder statistics. */
    public BeakGraph(List<Node> members, BGReader reader, BeakGraph root) {
        logger.trace("Create a graph-set view -> {}", members);
        init();
        this.uri = reader.getURI();
        this.base = (root == null) ? defaultBase(uri) : null; // a rooted view answers getBase() from its root
        this.reader = reader;
        this.namedgraph = GRAPH_SET;
        this.memberGraphs = List.copyOf(members);
        this.ownsReader = false;
        this.root = (root == null) ? null : root.rootGraph();
    }
    
    /** The dataset-owning graph, identified by the reader's URI and resolving against {@link #defaultBase}. */
    public BeakGraph(BGReader reader) {
        this(reader, reader.getURI(), defaultBase(reader.getURI()));
    }
    
    public URI getURI() {
        return uri;
    }
        
    public BeakGraph(Node namedgraph, BGReader reader) {
        this(namedgraph, reader, null);
    }

    /**
     * A named-graph view sharing {@code root}'s reorder statistics (the
     * dataset's graph; null for a standalone view).
     */
    public BeakGraph(Node namedgraph, BGReader reader, BeakGraph root) {
        logger.trace("Create a SubBeakGraph -> {}", namedgraph);
        init();
        this.uri = reader.getURI();
        this.base = (root == null) ? defaultBase(uri) : null; // a rooted view answers getBase() from its root
        this.reader = reader;
        this.namedgraph = namedgraph;
        this.memberGraphs = null;
        this.ownsReader = false; // a view over a reader owned by the dataset
        this.root = (root == null) ? null : root.rootGraph();
    }

    /** The graph whose reorder statistics this one shares: itself unless it is a view with a root. */
    private BeakGraph rootGraph() {
        return (root == null) ? this : root;
    }
    
    private static void init() {
        if ( initialized ) {
            // The global stage generator is a slot any component may overwrite
            // (StageBuilder.setGenerator with something that does not delegate):
            // a BeakGraph queried through the plain Model path then lost the
            // id-level solver without a word. Re-wrap whenever the director is
            // no longer in place - it delegates every other graph, so this is
            // idempotent and bounded by the foreign replacements (BG-63).
            if (!(StageBuilder.chooseStageGenerator(ARQ.getContext()) instanceof StageGeneratorDirectorBG)) {
                synchronized (initLock) {
                    if (!(StageBuilder.chooseStageGenerator(ARQ.getContext()) instanceof StageGeneratorDirectorBG)) {
                        logger.warn("ARQ's global stage generator was replaced after BeakGraph was wired; re-installing"
                                + " the BeakGraph director around the current generator");
                        wireIntoExecution();
                    }
                }
            }
            return ;
        }
        synchronized(initLock) {
            if ( initialized ) {
                return ;
            }
            // The OpExecutor factory is wired per-dataset (BGDatasetGraph's own
            // context), NOT into the global ARQ context: a global factory would
            // change query execution for every other dataset in the JVM. Only the
            // stage-generator director is global - the standard Jena pattern -
            // because it dispatches on the active graph's type and delegates
            // everything that is not a BeakGraph.
            wireIntoExecution() ;
            // Pin CDT support on: BeakGraph stores cdt:List/cdt:Map literals and
            // its comparator/guards assume their datatypes behave as composites.
            // Jena defaults this to true, but ARQ.setStrictMode() anywhere in the
            // JVM silently flips it off; BGDatasetGraph re-pins per dataset for
            // the strictMode-after-init case.
            org.apache.jena.sparql.SystemARQ.EnableCDTs = true;
            // Publish only after wiring succeeded, so a failure here is retried by
            // the next caller instead of leaving the JVM half-wired forever.
            initialized = true ;
        }
    }
    
    public BGReader getReader() {
        return reader;
    }
    
    @Override
    public void close() {
        // GraphBase's own closed flag: isClosed() answers true and every
        // find() through GraphBase fails with ClosedException instead of
        // running on a closed reader's leftover buffers (BG-5). A view marks
        // only itself closed.
        super.close();
        if (!ownsReader) {
            return; // closing a named-graph view must not close the shared reader
        }
        try {
            reader.close();
        } catch (Exception ex) {
            logger.error("Error closing BeakGraph reader for {}", uri, ex);
        }
    }
    
    public Node getNamedGraph() {
        return namedgraph;
    }

    /** True for a view over several graphs (see {@link #BeakGraph(List, BGReader)}); the fast paths keyed on one graph decline it. */
    public boolean isGraphSetView() {
        return memberGraphs != null;
    }

    /** The members of a graph-set view, or null for an ordinary graph. */
    public List<Node> getMemberGraphs() {
        return memberGraphs;
    }

    /**
     * The id-level read of {@code triple} in this graph: the reader's read of
     * the named graph, or the union read of a graph-set view's members.
     */
    public Iterator<BindingNodeId> read(BindingNodeId bnid, Triple triple, ExprList filter) {
        NodeTable nodeTable = reader.getNodeTable();
        if (memberGraphs == null) {
            return reader.read(namedgraph, bnid, triple, filter, nodeTable);
        }
        return reader.readGraphs(memberGraphs, bnid, triple, filter, nodeTable);
    }
    
    /**
     * A dataset whose default graph is THIS graph and whose named graphs are
     * the store's. On the dataset-owning graph that is the whole store; on a
     * named-graph view (from {@code BGDatasetGraph.getGraph}) the default
     * graph is the view's graph in every API - {@code getDefaultGraph()},
     * {@code find(defaultGraph, ..)}, SPARQL - while the stored default graph
     * is not reachable through it (BG-14). Closing a view's dataset closes
     * only the view, never the shared reader.
     */
    /**
     * A dataset whose default graph is THIS graph and whose named graphs are
     * the store's. On the dataset-owning graph that is the whole store; on a
     * named-graph view (from {@code BGDatasetGraph.getGraph}) the default
     * graph is the view's graph in every API - {@code getDefaultGraph()},
     * {@code find(defaultGraph, ..)}, SPARQL - while the stored default graph
     * is not reachable through it (BG-14). Closing a view's dataset closes
     * only the view, never the shared reader.
     */
    /**
     * A dataset whose default graph is THIS graph and whose named graphs are
     * the store's. On the dataset-owning graph that is the whole store; on a
     * named-graph view (from {@code BGDatasetGraph.getGraph}) the default
     * graph is the view's graph in every API - {@code getDefaultGraph()},
     * {@code find(defaultGraph, ..)}, SPARQL - while the stored default graph
     * is not reachable through it (BG-14). Closing a view's dataset closes
     * only the view, never the shared reader.
     */
    public Dataset getDataset() {
        BGDatasetGraph dsg = new BGDatasetGraph(this);
        return DatasetFactory.wrap(dsg);
    }
    
    @Override
    protected ExtendedIterator<Triple> graphBaseFind(Triple tp) {
        RelativeIRIResolver resolver = resolver();
        Triple probe = tp;
        if (resolver != null) {
            // A pattern naming the document by its served URL must reach the
            // stored relative term; results come back resolved (BG-396).
            probe = NodeTransformLib.transform(resolver.absoluteToStorage(storedTerm()), tp);
        }
        ExtendedIterator<Triple> found = (memberGraphs == null)
                ? reader.graphBaseFind(namedgraph, probe) : findInMembers(probe);
        if (resolver == null) {
            return found;
        }
        NodeTransform out = resolver.storageToAbsolute();
        return found.mapWith(t -> NodeTransformLib.transform(out, t));
    }

    private ExtendedIterator<Triple> findInMembers(Triple tp) {
        Var sVar = Var.alloc("s");
        Var pVar = Var.alloc("p");
        Var oVar = Var.alloc("o");
        Node s = tp.getSubject().isConcrete() ? tp.getSubject() : sVar;
        Node p = tp.getPredicate().isConcrete() ? tp.getPredicate() : pVar;
        Node o = tp.getObject().isConcrete() ? tp.getObject() : oVar;
        NodeTable nodeTable = reader.getNodeTable();
        Iterator<BindingNodeId> it = reader.readGraphs(memberGraphs, new BindingNodeId(), Triple.create(s, p, o), null, nodeTable);
        return WrappedIterator.create(it).mapWith(b -> {
            Node sRes = s.isConcrete() ? s : nodeTable.getNodeForNodeId(b.get(sVar));
            Node pRes = p.isConcrete() ? p : nodeTable.getNodeForNodeId(b.get(pVar));
            Node oRes = o.isConcrete() ? o : nodeTable.getNodeForNodeId(b.get(oVar));
            // An unresolvable id is dropped, as the reader's own graphBaseFind
            // does, instead of a Triple.create that throws mid-iteration (BG-235).
            return (sRes == null || pRes == null || oRes == null) ? null : Triple.create(sRes, pRes, oRes);
        }).filterDrop(t -> t == null);
    }

    @Override
    public ExtendedIterator<Triple> find() {
        return find(Triple.ANY); // through GraphBase.find, which checks the closed flag (BG-5)
    }

    // Read-only: both the Triple and the (s, p, o) forms of add/delete reach
    // these through GraphBase and fail with Jena's denied exceptions - the
    // signal Jena callers (Model.add, GraphUtil, update paths) catch. The
    // former (s, p, o) overrides threw an unrelated UnsupportedOperationException
    // reading as a TODO (BG-15).
    @Override
    public void performAdd(Triple t) {
        throw new AddDeniedException("BeakGraph is read-only.", t);
    }

    @Override
    public void performDelete(Triple t) {
        throw new DeleteDeniedException("BeakGraph is read-only.", t);
    }

    // Graph.stream(s, p, o) / stream() keep Jena's find-based defaults: they
    // used to throw here, which broke every Jena consumer of the stream API
    // (DatasetGraphOne, DatasetGraphMap, the RDFS engine) over a BeakGraph (BG-263).
    
    @Override
    protected int graphBaseSize() {
        int size = cachedSize;
        if (size >= 0) {
            return size;
        }
        // Answered from index structure when possible (a few select1 calls);
        // otherwise count this graph's distinct triples by scanning it once.
        // Quads are de-duplicated in the index, so each triple is visited exactly
        // once. Cached because the graph is read-only.
        long count = (memberGraphs == null) ? reader.countTriples(namedgraph) : -1;
        if (count < 0) {
            count = 0;
            ExtendedIterator<Triple> it = graphBaseFind(Triple.create(Node.ANY, Node.ANY, Node.ANY));
            try {
                while (it.hasNext()) {
                    it.next();
                    count++;
                }
            } finally {
                it.close();
            }
        }
        size = (count > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) count;
        cachedSize = size;
        return size;
    }
    
    private static void wireIntoExecution() {
        Context cxt = ARQ.getContext() ;
        StageGenerator orig = StageBuilder.chooseStageGenerator(cxt) ;
        StageGenerator stageGenerator = new StageGeneratorDirectorBG(orig) ;
        StageBuilder.setGenerator(ARQ.getContext(), stageGenerator) ;
        // The engine that carries BG's per-execution wiring to every dataset
        // whose default graph is a BeakGraph - including the Model path, which
        // never sees a BGDatasetGraph context (see QueryEngineBG).
        QueryEngineBG.register();
    }

    /**
     * Join-reordering transform for BGP optimization. Built lazily from the persisted VoID stats
     * (BeakGraph-/index-aware selectivity) and cached, falling back to Jena's fixed heuristic when
     * no stats are available. Returns a usable transform rather than null so multi-pattern BGPs are
     * reordered most-selective-first.
     */
    public ReorderTransformation getReorderTransform() {
        if (root != null) {
            return root.getReorderTransform();
        }
        ReorderTransformation r = reorderTransform;
        if (r == null) {
            synchronized (this) {
                r = reorderTransform;
                if (r == null) {
                    reorderTransform = r = buildReorderTransform();
                }
            }
        }
        return r;
    }

    private ReorderTransformation buildReorderTransform() {
        try {
            VoidStats stats = VoidStats.load(reader);
            if (stats.usable()) {
                return new BGReorderTransform(stats);
            }
            logger.debug("No VoID stats for {}; using fixed reorder heuristic", uri);
        } catch (RuntimeException ex) {
            logger.warn("Failed to load VoID stats for {}; using fixed reorder heuristic", uri, ex);
        }
        return ReorderLib.fixed();
    }
}
