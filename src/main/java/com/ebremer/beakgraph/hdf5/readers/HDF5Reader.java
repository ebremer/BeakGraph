package com.ebremer.beakgraph.hdf5.readers;

import org.apache.jena.shared.ClosedException;
import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.GSPODictionary;
import com.ebremer.beakgraph.hdf5.jena.BGIteratorMaster;
import com.ebremer.beakgraph.hdf5.jena.BGReader;
import com.ebremer.beakgraph.hdf5.jena.BindingNodeId;
import com.ebremer.beakgraph.hdf5.jena.NodeId;
import com.ebremer.beakgraph.hdf5.jena.NodeType;
import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.hdf5.jena.SimpleNodeTable;
import com.ebremer.beakgraph.turbo.Spatial;
import com.ebremer.beakgraph.store.OpenStore;
import com.ebremer.beakgraph.store.StoreGroup;
import java.io.File;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sys.JenaSystem;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NullIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A BeakGraph store, opened for reading. Named for the container it was
 * first written for: the store is the same in an HDF5 file and in a Zarr
 * directory store (SPECIFICATIONS.md §3), and this reader reads either,
 * through {@link OpenStore} - nothing below this class knows which.
 */
public class HDF5Reader implements BGReader {

    private static final Logger LOG = LoggerFactory.getLogger(HDF5Reader.class);

    /**
     * The store in its container: owns the memory mappings its arrays are
     * views of, and the channel of a channel-backed store.
     */
    private final OpenStore store;
    private final StoreGroup hdt;
    private final PositionalDictionaryReader dict;
    private final Node defaultGraph;
    private final SimpleNodeTable nodeTable;
    private final Map<Index, IndexReader> indexCache = new ConcurrentHashMap<>();
    private final URI uri;
    private final long formatVersion;
    private final boolean hasFormatVersionAttribute;
    private final long numQuads;
    // Closed readers must be detectable (pool validation) and close() must be
    // idempotent (a poisoned pooled instance is closed again on destroy).
    private volatile boolean open = true;

    static {
        JenaSystem.init();
        com.ebremer.halcyon.hilbert.WKTDatatype.register();
        Spatial.init();
    }

    public HDF5Reader(Path src) {
        this(src.toFile());
    }

    /**
     * Opens the store at {@code src}: an HDF5 file, or the directory of a
     * Zarr store. Either is memory-mapped.
     */
    public HDF5Reader(File src) {
        this(OpenStore.open(src.toPath()), src.toURI());
    }

    /**
     * Reads a BeakGraph through any {@link SeekableByteChannel} - e.g. an
     * {@code HTTPSeekableByteChannel} for querying a remote file in place.
     * The reader takes ownership of the channel: it is closed by
     * {@link #close()}, and also released if construction fails.
     *
     * @param channel positioned at the start of the HDF5 file
     * @param source  identifies the data for {@link #getURI()} and messages
     */
    public HDF5Reader(SeekableByteChannel channel, URI source) {
        this(OpenStore.open(channel, source), source);
    }

    /**
     * Reads a store that is already open, in whichever container. The reader
     * takes ownership of it: it is closed by {@link #close()}, and also
     * released if construction fails.
     *
     * @param uri identifies the data for {@link #getURI()} and messages
     */
    public HDF5Reader(OpenStore store, URI uri) {
        this.store = store;
        try {
            StoreGroup bg;
            try {
                bg = store.root().group(Params.BG);
            } catch (IllegalStateException notAGroup) {
                bg = null; // something else under the name
            }
            if (bg == null) {
                throw new IllegalStateException(
                        "Not a BeakGraph file (no '" + Params.BG + "' group): " + uri);
            }
            this.hdt = bg;
            // A Zarr store is many files: map them all now, so that a store replaced
            // while this reader is open is still read whole (nothing to do in HDF5).
            bg.load();
            this.formatVersion = readFormatVersion(bg);
            this.hasFormatVersionAttribute = bg.hasAttribute(Params.FORMAT_VERSION_ATTR);
            this.numQuads = readNumQuads(bg);
            if (formatVersion > Params.FORMAT_VERSION) {
                throw new IllegalStateException(
                        "BeakGraph HDF5 format version " + formatVersion + " in " + uri
                      + " is newer than this build supports (max " + Params.FORMAT_VERSION
                      + "). Upgrade BeakGraph.");
            }
            if (formatVersion < Params.RANK_DIRECTORY_MIN_VERSION) {
                // Legal (SPECIFICATIONS §4.1 accepts older files) but a cliff an
                // operator could not see: without the rank/select directories
                // every block lookup is a linear scan of the level bitmap (BG-84).
                LOG.warn("BeakGraph store {} is format v{}: its rank/select directories are ignored and every"
                        + " index lookup falls back to the linear select1 scan; rebuild it (format v{}) for O(log n) lookups",
                        uri, formatVersion, Params.FORMAT_VERSION);
            }
            StoreGroup dictionary = StoreProfile.group(hdt, Params.DICTIONARY);
            if (dictionary == null) {
                // A file with the .BG group but no dictionary (a foreign writer's
                // partial output) used to surface as a bare NullPointerException
                // from inside the dictionary reader (BG-88).
                throw new IllegalStateException(
                        "Not a BeakGraph file (no '" + Params.BG + "/" + Params.DICTIONARY + "' group): " + uri);
            }
            this.dict = new PositionalDictionaryReader(dictionary);
            // Lower bound: format v4 changed how composite (cdt) literals are
            // ordered, and ids are comparator ranks. A pre-v4 file holding them
            // would open fine and then silently miss terms on every lookup.
            // Files without composite literals are unaffected by that change.
            if (formatVersion < Params.CDT_LEXICAL_ORDER_MIN_VERSION && dict.literalsContainCompositeDatatype()) {
                throw new IllegalStateException(
                        "BeakGraph HDF5 format version " + formatVersion + " in " + uri
                      + " was built by BeakGraph 0.17.0 or earlier and contains cdt:List/cdt:Map literals,"
                      + " whose dictionary order changed in format version " + Params.CDT_LEXICAL_ORDER_MIN_VERSION
                      + "; queries on this file would silently miss terms. Rebuild it from source.");
            }
            this.defaultGraph = Quad.defaultGraphIRI;
            nodeTable = new SimpleNodeTable(dict);
            this.uri = uri;
        } catch (RuntimeException | Error e) {
            // Close the backing storage before propagating: a leaked file stays
            // mapped (and on Windows, locked) - or its channel open - with no
            // way to release it.
            closeStorage();
            throw e;
        }
    }

    /** Unmaps the store's files (or drops what was cached of a channel-backed one), then closes the channel it was read through. */
    private void closeStorage() {
        try { store.close(); } catch (Exception ignore) {}
    }

    /**
     * Reads the on-disk format version from the .BG group. Files written before
     * format versioning have no attribute and are treated as version 1 - the
     * only case SPECIFICATIONS §4.1 defines as v1. An attribute that is present
     * but unreadable, non-numeric or below 1 is corruption, not a legacy file:
     * it used to be silently read as v1, which disabled the rank directories
     * and turned every lookup into a linear scan (BG-350).
     */
    private static long readFormatVersion(StoreGroup hdt) {
        if (!hdt.hasAttribute(Params.FORMAT_VERSION_ATTR)) {
            return 1L;
        }
        long version;
        try {
            version = hdt.attribute(Params.FORMAT_VERSION_ATTR);
        } catch (IllegalStateException e) {
            // Not a scalar, not an integer, or not readable at all.
            throw new IllegalStateException("formatVersion attribute is present but not a version number: "
                    + e.getMessage());
        }
        if (version >= 1) {
            return version;
        }
        throw new IllegalStateException("formatVersion attribute is present but not a version number: " + version);
    }

    /** False for a legacy (pre-versioning) file, which the reader treats as format v1. */
    public boolean hasFormatVersionAttribute() {
        return hasFormatVersionAttribute;
    }

    /** The store's numQuads attribute (source quads before de-duplication, SPECIFICATIONS §9.6), or -1 when absent. */
    public long getNumQuads() {
        return numQuads;
    }

    private static long readNumQuads(StoreGroup hdt) {
        try {
            if (hdt.hasAttribute(Params.NUM_QUADS)) {
                return hdt.attribute(Params.NUM_QUADS);
            }
        } catch (RuntimeException ignore) {
            // informational only
        }
        return -1;
    }

    @Override
    public URI getURI() {
        return uri;
    }

    @Override
    public long getFormatVersion() {
        return formatVersion;
    }
    
    public IndexReader getIndexReader(Index indexType) {
        checkOpen(); // the lazy load reads through the (closed) file otherwise
        return indexCache.computeIfAbsent(indexType, type -> {
            StoreGroup indexGroup = StoreProfile.group(hdt, type.name());
            if (indexGroup == null) {
                return null; // index not present in this file; the caller falls back
            }
            try {
                return new IndexReader(indexGroup, type, formatVersion);
            } catch (Exception e) {
                // A present-but-unreadable index means a corrupt/incompatible file - fail
                // loudly rather than returning null (which reads as "index absent").
                throw new IllegalStateException("Failed to load index " + type + " from " + uri, e);
            }
        });
    }
    
    /**
     * The storage layer is reached directly by the executor and the stage
     * generator, which GraphBase's closed flag cannot protect: a closed reader
     * used to keep answering from cached buffers and fail deep inside the
     * HDF5 library for anything uncached (BG-5).
     */
    private void checkOpen() {
        if (!open) {
            throw new ClosedException("BeakGraph reader already closed: " + uri, null);
        }
    }

    @Override
    public Iterator<BindingNodeId> read(Node ng, BindingNodeId bnid, Triple triple, ExprList filter, NodeTable nodeTable) {
        checkOpen();
        // An EMPTY store (built from an empty source - legal) has no dictionary
        // sections, and its index groups hold only their seeded directories
        // (SPECIFICATIONS §7.9); every pattern answers no solutions rather
        // than each downstream layer having to tolerate absent structures.
        // Deliberately keyed on dictionary absence, NOT numQuads: that attribute
        // counts SOURCE quads only, and a -void build of an empty source has
        // zero source quads but real stored metadata quads.
        if (dict.isEmpty()) {
            return Collections.emptyIterator();
        }
        BindingNodeId binding = orEmpty(bnid);
        // A pattern variable already bound to a node that does not exist in this store
        // (e.g. a VALUES/BIND term not present here) cannot match anything, so the pattern
        // yields no solutions - rather than failing to resolve the missing id.
        Triple pattern = resolve(triple, binding, nodeTable);
        if (pattern == null) {
            return Collections.emptyIterator();
        }
        if (Quad.isUnionGraph(ng)) {
            return readUnion(namedGraphIds(), binding, pattern, filter, nodeTable);
        }
        boolean isDefault = ng.equals(Quad.defaultGraphNodeGenerated) || ng.equals(Quad.defaultGraphIRI);
        Node g = isDefault ? this.defaultGraph : ng;
        Quad quadPattern = new Quad(g, pattern.getSubject(), pattern.getPredicate(), pattern.getObject());
        return new BGIteratorMaster(this, dict, binding, quadPattern, filter, nodeTable);
    }

    @Override
    public Iterator<BindingNodeId> read(long graphId, BindingNodeId bnid, Triple triple, ExprList filter, NodeTable nodeTable) {
        checkOpen();
        if (dict.isEmpty() || graphId < 1) {
            return Collections.emptyIterator();
        }
        BindingNodeId binding = orEmpty(bnid);
        Triple pattern = resolve(triple, binding, nodeTable);
        if (pattern == null) {
            return Collections.emptyIterator();
        }
        return readGraph(graphId, binding, pattern, filter, nodeTable);
    }

    @Override
    public java.util.stream.LongStream graphIds() {
        return dict.streamGraphIds();
    }

    /**
     * The pattern over the graph with dictionary id {@code gid}. The id is
     * handed to the iterator as resolved; the quad's graph slot is a
     * placeholder it never looks up.
     */
    private Iterator<BindingNodeId> readGraph(long gid, BindingNodeId binding, Triple pattern, ExprList filter, NodeTable nodeTable) {
        Quad quadPattern = new Quad(Quad.unionGraph, pattern.getSubject(), pattern.getPredicate(), pattern.getObject());
        return new BGIteratorMaster(this, dict, binding, quadPattern, filter, nodeTable, gid);
    }

    /**
     * Ids of the named graphs straight from the columnar graph list: the
     * union fan-out used to extract every graph's term and hand it back to
     * read(), whose iterator located it again - the round trip the
     * variable-graph path in BGIteratorMaster already avoids (BG-259).
     */
    private java.util.PrimitiveIterator.OfLong namedGraphIds() {
        long dg = dict.getGraphs().locate(Quad.defaultGraphIRI);
        long generated = dict.getGraphs().locate(Quad.defaultGraphNodeGenerated);
        return dict.streamGraphIds().filter(id -> id != dg && id != generated).iterator();
    }

    /** A null binding means "no bindings" - every layer below read() already treats it so (BG-340). */
    private static BindingNodeId orEmpty(BindingNodeId bnid) {
        return (bnid != null) ? bnid : new BindingNodeId();
    }

    /**
     * The pattern as the iterators take it under {@code binding}, or null when
     * no row can match. One lookup a position decides both things that were
     * two passes over the pattern:
     * <ul>
     *   <li>A variable bound to a node that does not exist in this store (a
     *       VALUES/BIND term not present here) cannot match anything: null.</li>
     *   <li>A bound variable's id is used DIRECTLY by the iterators (they
     *       consult the binding before the dictionary) whenever its id-space
     *       matches the position; only a cross-space binding (predicate id used
     *       in an entity position or vice versa) is materialized to its term
     *       here so the iterator re-locates it in the position's own
     *       dictionary. The former unconditional substitution paid an
     *       extract() + full binary search per bound variable per input row.</li>
     * </ul>
     * The reader is asked once for every input row of a join and once for
     * every graph of a walk over the named graphs, so the lookups count.
     */
    private Triple resolve(Triple triple, BindingNodeId binding, NodeTable nodeTable) {
        Node s = resolve(triple.getSubject(), binding, nodeTable, false);
        if (s == null) return null;
        Node p = resolve(triple.getPredicate(), binding, nodeTable, true);
        if (p == null) return null;
        Node o = resolve(triple.getObject(), binding, nodeTable, false);
        if (o == null) return null;
        return (s == triple.getSubject() && p == triple.getPredicate() && o == triple.getObject())
                ? triple : Triple.create(s, p, o);
    }

    @Override
    public Iterator<BindingNodeId> readGraphs(java.util.Collection<Node> graphs, BindingNodeId bnid, Triple triple, ExprList filter, NodeTable nodeTable) {
        checkOpen();
        if (dict.isEmpty()) {
            return Collections.emptyIterator();
        }
        BindingNodeId binding = orEmpty(bnid);
        Triple pattern = resolve(triple, binding, nodeTable);
        if (pattern == null) {
            return Collections.emptyIterator();
        }
        // Members are terms (a FROM list): each is located once, here; the
        // union graph member expands to every named graph; an absent member
        // contributes nothing.
        java.util.stream.LongStream ids = graphs.stream().flatMapToLong(n -> {
            if (Quad.isUnionGraph(n)) {
                return dict.streamGraphIds().filter(id -> id != dict.getGraphs().locate(Quad.defaultGraphIRI));
            }
            Node g = Quad.isDefaultGraph(n) ? this.defaultGraph : n;
            return java.util.stream.LongStream.of(dict.getGraphs().locate(g));
        }).filter(id -> id >= 1).distinct();
        return readUnion(ids.iterator(), binding, pattern, filter, nodeTable);
    }

    /**
     * The set union of {@code graphs} - {@code urn:x-arq:unionGraph} (every
     * named graph) or an explicit FROM list - with the SPARQL-mandated set
     * semantics: a triple present in several members appears once. Rows are
     * deduplicated on the values of the pattern's variables (the concrete
     * positions are identical across graphs by construction).
     */
    private Iterator<BindingNodeId> readUnion(java.util.PrimitiveIterator.OfLong graphIds, BindingNodeId bnid, Triple triple, ExprList filter, NodeTable nodeTable) {
        List<Var> varList = new ArrayList<>(3);
        for (Node n : new Node[]{triple.getSubject(), triple.getPredicate(), triple.getObject()}) {
            if (n.isVariable()) {
                Var v = Var.alloc(n);
                if (!varList.contains(v)) varList.add(v);
            } else if (n.isTripleTerm() && !n.isConcrete()) {
                // Variables embedded in a triple-term pattern identify a row as
                // fully as top-level ones (given the pattern's concrete parts,
                // the matched stored term determines them 1:1) - they must key
                // the union dedup or distinct rows would collapse.
                collectTripleTermVars(n, varList);
            }
        }
        // Lazy per-graph chaining: constructing every graph's iterator up front
        // paid each one's index binary searches before the first row came back
        // (spatial stores hold thousands of tile graphs).
        Iterator<Long> graphs = graphIds;
        Iterator<BindingNodeId> chain = Iter.flatMap(graphs, gid -> readGraph(gid, bnid, triple, filter, nodeTable));
        // The dedup set is inherent to union set-semantics (rows arrive per
        // graph, not globally sorted). Up to three variables - every pattern
        // shape before triple-term patterns existed - keeps the historical
        // open-addressing three-long set, free of per-row key/box allocations;
        // more variables (only reachable with embedded triple-term vars) use
        // the array-keyed generalization.
        if (varList.size() <= 3) {
            Var v0 = varList.size() > 0 ? varList.get(0) : null;
            Var v1 = varList.size() > 1 ? varList.get(1) : null;
            Var v2 = varList.size() > 2 ? varList.get(2) : null;
            LongTripleSet seen = new LongTripleSet();
            return Iter.filter(chain, b -> {
                // Packed ids (type bits included) key the dedup: identical variable
                // values across graphs carry identical packed ids by construction.
                long k0 = (v0 != null) ? b.get(v0) : NodeId.NONE;
                long k1 = (v1 != null) ? b.get(v1) : NodeId.NONE;
                long k2 = (v2 != null) ? b.get(v2) : NodeId.NONE;
                return seen.add(k0, k1, k2);
            });
        }
        Var[] vars = varList.toArray(Var[]::new);
        LongTupleSet seen = new LongTupleSet(vars.length);
        long[] probe = new long[vars.length]; // reused per row; copied only on insert
        return Iter.filter(chain, b -> {
            for (int i = 0; i < vars.length; i++) {
                probe[i] = b.get(vars[i]);
            }
            return seen.add(probe);
        });
    }

    /** Adds every variable inside a triple-term pattern (all depths) to {@code out}, without duplicates. */
    private static void collectTripleTermVars(Node tripleTerm, List<Var> out) {
        Triple t = tripleTerm.getTriple();
        for (Node n : new Node[]{t.getSubject(), t.getPredicate(), t.getObject()}) {
            if (n.isVariable()) {
                Var v = Var.alloc(n);
                if (!out.contains(v)) out.add(v);
            } else if (n.isTripleTerm() && !n.isConcrete()) {
                collectTripleTermVars(n, out);
            }
        }
    }

    /**
     * Open-addressing hash set of fixed-width long tuples - the union dedup
     * structure for patterns carrying more than three variables (embedded
     * triple-term vars). Linear probing at &le; 50% load; grows by doubling.
     * add() copies the caller's (reused) probe buffer only on insert.
     * Not thread-safe (one per union iterator).
     */
    private static final class LongTupleSet {
        private final int width;
        private long[][] keys;
        private int size;

        LongTupleSet(int width) {
            this.width = width;
            this.keys = new long[1 << 10][];
        }

        boolean add(long[] key) {
            if ((size << 1) >= keys.length) {
                grow();
            }
            int mask = keys.length - 1;
            long h = 0;
            for (long k : key) {
                h = mix(h ^ k);
            }
            int i = (int) (h & mask);
            while (keys[i] != null) {
                if (java.util.Arrays.equals(keys[i], key)) {
                    return false;
                }
                i = (i + 1) & mask;
            }
            keys[i] = java.util.Arrays.copyOf(key, width);
            size++;
            return true;
        }

        private void grow() {
            long[][] old = keys;
            keys = new long[old.length << 1][];
            size = 0;
            for (long[] k : old) {
                if (k != null) {
                    add(k);
                }
            }
        }

        /** splitmix64 finalizer - full-avalanche mix (same as LongTripleSet). */
        private static long mix(long z) {
            z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
            z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
            return z ^ (z >>> 31);
        }
    }

    /**
     * Open-addressing hash set of (long, long, long) keys - the union scan's
     * dedup structure. Linear probing at &le; 50% load; grows by doubling.
     * Not thread-safe (one per union iterator).
     */
    private static final class LongTripleSet {
        private long[] a, b, c;
        private boolean[] used;
        private int size;

        LongTripleSet() {
            alloc(1 << 10);
        }

        private void alloc(int capacity) {
            a = new long[capacity];
            b = new long[capacity];
            c = new long[capacity];
            used = new boolean[capacity];
            size = 0;
        }

        /** splitmix64 finalizer - full-avalanche mix. */
        private static long mix(long z) {
            z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
            z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
            return z ^ (z >>> 31);
        }

        boolean add(long x, long y, long z) {
            if ((size << 1) >= used.length) {
                grow();
            }
            int mask = used.length - 1;
            int i = (int) (mix(x ^ mix(y ^ mix(z))) & mask);
            while (used[i]) {
                if (a[i] == x && b[i] == y && c[i] == z) {
                    return false;
                }
                i = (i + 1) & mask;
            }
            used[i] = true;
            a[i] = x;
            b[i] = y;
            c[i] = z;
            size++;
            return true;
        }

        private void grow() {
            long[] oa = a, ob = b, oc = c;
            boolean[] ou = used;
            alloc(ou.length << 1);
            for (int i = 0; i < ou.length; i++) {
                if (ou[i]) {
                    add(oa[i], ob[i], oc[i]);
                }
            }
        }
    }

    /**
     * One position of {@link #resolve(Triple, BindingNodeId, NodeTable)}: the
     * node itself (a term, an unbound variable, or a variable whose bound id
     * the iterator reads from the binding), the term a cross-space binding
     * stands for, or null for a variable bound to a node that does not exist
     * here.
     *
     * <p>A bound variable is replaced with its concrete term ONLY when its
     * NodeId lives in a different id-space than the position it is used at.
     * GRAPH/SUBJECT ids are entity-space and OBJECT ids are entity-space plus
     * offset literals, so among those three positions a bound id is directly
     * comparable (a literal id in an entity position simply never matches -
     * correct, since a literal cannot be a subject or graph). Only the isolated
     * PREDICATE space needs the materialize-and-relocate round trip, in either
     * direction.
     */
    private Node resolve(Node n, BindingNodeId bnid, NodeTable nodeTable, boolean predicatePosition) {
        if (!n.isVariable()) {
            return n;
        }
        long id = bnid.get(Var.alloc(n));
        if (id == NodeId.NONE) {
            return n; // unbound
        }
        if (NodeId.isDoesNotExist(id)) {
            return null; // bound to a node this store does not hold
        }
        if (NodeId.isPredicateSpace(id) == predicatePosition) {
            return n; // same space: the iterator consumes the bound id directly
        }
        Node concrete = nodeTable.getNodeForNodeId(id);
        return (concrete != null) ? concrete : n;
    }

    @Override
    public ExtendedIterator<Triple> graphBaseFind(Node graph, Triple tp) {
        checkOpen();
        // Map Node.ANY (wildcards) to specific Variables
        Var sVar = Var.alloc("s");
        Var pVar = Var.alloc("p");
        Var oVar = Var.alloc("o");

        Node s = tp.getSubject().isConcrete() ? tp.getSubject() : sVar;
        Node p = tp.getPredicate().isConcrete() ? tp.getPredicate() : pVar;
        Node o = tp.getObject().isConcrete() ? tp.getObject() : oVar;
        // Absent-term early exit. The iterators would answer empty anyway, but for a
        // union/named-graph fan-out this saves constructing per-graph iterators; the
        // "duplicate" locate the iterator then performs is a dictionary search-cache
        // hit, not a second binary search.
        if (s.isConcrete() && dict.getSubjects().locate(s) == -1) {
            return new NullIterator<>();
        }
        if (p.isConcrete() && dict.getPredicates().locate(p) == -1) {
            return new NullIterator<>();
        }
        if (o.isConcrete() && dict.getObjects().locate(o) == -1) {
            return new NullIterator<>();
        }        
        Triple pattern = Triple.create(s, p, o);

        // Execute against the specific graph requested by BeakGraph
        Iterator<BindingNodeId> it = read(graph, new BindingNodeId(), pattern, null, nodeTable);

        return WrappedIterator.create(it).mapWith(bnid -> {
            Node sRes = tp.getSubject().isConcrete() ? tp.getSubject() : nodeTable.getNodeForNodeId(bnid.get(sVar));
            Node pRes = tp.getPredicate().isConcrete() ? tp.getPredicate() : nodeTable.getNodeForNodeId(bnid.get(pVar));
            Node oRes = tp.getObject().isConcrete() ? tp.getObject() : nodeTable.getNodeForNodeId(bnid.get(oVar));
            // An unresolvable id resolves to null; Triple.create would NPE on it. Map such a row to
            // null and drop it below rather than emit a malformed (or null) triple to the consumer.
            if (sRes == null || pRes == null || oRes == null) {
                return null;
            }
            return Triple.create(sRes, pRes, oRes);
        }).filterDrop(t -> t == null);
    }

    @Override
    public NodeTable getNodeTable() { return nodeTable; }

    /**
     * Whether the file is read through a caller-supplied
     * {@link SeekableByteChannel} (an HTTP range channel, typically) rather
     * than mapped from a local path. Every dataset read then goes through the
     * channel's single lock and its block cache, so concurrent readers of one
     * file gain nothing and only contend; the query engine keeps such stores
     * on the sequential scan path (BG-247). Same test as
     * {@code DatasetBytes.of}, which picks the lazy channel-reading view for
     * exactly these files.
     */
    public boolean isChannelBacked() {
        return store.isChannelBacked();
    }

    /** The container the store is kept in: an HDF5 file, or a Zarr store. */
    public com.ebremer.beakgraph.store.Container getContainer() {
        return store.container();
    }

    @Override
    public void close() {
        if (!open) return;
        open = false;
        // Unmaps the file here and now: every dataset is a view of Falcon's
        // one mapping of it, so from this point the file is free to replace
        // or delete, on Windows too, and a RandomAccessBytes view still in
        // use throws IllegalStateException (DatasetBytes, BG-353). The
        // channel of a channel-backed store, which the reader owns, closes too.
        closeStorage();
        // The node table advertises AutoCloseable and its two caches hold up to
        // a million weighted Node entries each; a closed reader that stays
        // referenced (a field, a registry) used to keep them all reachable and
        // kept answering from them (BG-287).
        try {
            nodeTable.close();
        } catch (Exception ignore) {
            // cache invalidation does not fail
        }
        indexCache.clear();
    }

    @Override
    public boolean isOpen() { return open; }

    @Override public GSPODictionary getDictionary() { return dict; }

    @Override
    public Iterator<Node> listGraphNodes() {
        checkOpen();
        return dict.streamGraphs().iterator();
    }
    
    @Override
    public long countTriples(Node graph) {
        checkOpen();
        // Quads are de-duplicated per graph in the index, so the graph's quad
        // count is its triple count.
        return IndexCounts.quads(this, graph);
    }

    @Override
    public boolean containsGraph(Node graphNode) {
        checkOpen();
        // Graphs share the universal entity dictionary, so locate() alone matches
        // every subject/object entity too; membership in the columnar graphs list
        // is what makes an entity an actual graph.
        long id = dict.getGraphs().locate(graphNode);
        return id != -1 && dict.isGraph(id);
    }
    
}
