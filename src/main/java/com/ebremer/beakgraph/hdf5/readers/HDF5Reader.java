package com.ebremer.beakgraph.hdf5.readers;

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
import io.jhdf.HdfFile;
import io.jhdf.api.Attribute;
import io.jhdf.api.Group;
import java.io.File;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.InvalidPathException;
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

public class HDF5Reader implements BGReader {

    private final HdfFile hdf;
    private final Group hdt;
    private final PositionalDictionaryReader dict;
    private final Node defaultGraph;
    private final SimpleNodeTable nodeTable;
    private final Map<Index, IndexReader> indexCache = new ConcurrentHashMap<>();
    private final URI uri;
    private final long formatVersion;
    // Closed readers must be detectable (pool validation) and close() must be
    // idempotent (a poisoned pooled instance is closed again on destroy).
    private volatile boolean open = true;

    static {
        JenaSystem.init();
        Spatial.init();
    }

    public HDF5Reader(Path src) {
        this(src.toFile());
    }

    public HDF5Reader(File src) {
        this(new HdfFile(src.toPath()), src.toURI());
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
        this(open(channel, source), source);
    }

    private static HdfFile open(SeekableByteChannel channel, URI source) {
        try {
            return new HdfFile(channel, jhdfDisplayUri(source));
        } catch (RuntimeException | Error e) {
            // jHDF leaves the channel open when construction fails (e.g. not an
            // HDF5 file); the reader owns the channel, so release it here.
            try { channel.close(); } catch (Exception ignore) {}
            throw e;
        }
    }

    /**
     * jHDF derives a display {@link Path} from the URI's path component;
     * substitute a placeholder for URIs it cannot hold (opaque URNs, path
     * characters illegal in local paths) so such sources still open.
     */
    private static URI jhdfDisplayUri(URI source) {
        try {
            String path = source.getPath();
            if (path != null) {
                Path.of(path);
                return source;
            }
        } catch (InvalidPathException cannotDisplay) {
            // fall through to the placeholder
        }
        return URI.create("bg:/channel");
    }

    private HDF5Reader(HdfFile hdf, URI uri) {
        this.hdf = hdf;
        try {
            this.hdt = (Group) hdf.getChild(Params.BG);
            if (hdt == null) {
                throw new IllegalStateException(
                        "Not a BeakGraph file (no '" + Params.BG + "' group): " + uri);
            }
            this.formatVersion = readFormatVersion(hdt);
            if (formatVersion > Params.FORMAT_VERSION) {
                throw new IllegalStateException(
                        "BeakGraph HDF5 format version " + formatVersion + " in " + uri
                      + " is newer than this build supports (max " + Params.FORMAT_VERSION
                      + "). Upgrade BeakGraph.");
            }
            Group dictionary = (Group) hdt.getChild(Params.DICTIONARY);
            this.dict = new PositionalDictionaryReader(dictionary);
            this.defaultGraph = Quad.defaultGraphIRI;
            nodeTable = new SimpleNodeTable(dict);
            this.uri = uri;
        } catch (RuntimeException | Error e) {
            // Close the backing storage before propagating: a leaked HdfFile pins
            // the file handle (and on Windows, the file lock) - or the channel -
            // with no way to release it.
            try { hdf.close(); } catch (Exception ignore) {}
            throw e;
        }
    }

    /**
     * Reads the on-disk format version from the .BG group. Files written before
     * format versioning have no attribute and are treated as version 1.
     */
    private static long readFormatVersion(Group hdt) {
        try {
            Attribute a = hdt.getAttribute("formatVersion");
            if (a != null && a.getData() instanceof Number n) {
                return n.longValue();
            }
        } catch (Exception ignore) {
            // unreadable attribute - treat as a legacy (pre-versioning) file
        }
        return 1L;
    }

    @Override
    public URI getURI() {
        return uri;
    }
    
    public IndexReader getIndexReader(Index indexType) {
        return indexCache.computeIfAbsent(indexType, type -> {
            Group indexGroup = (Group) hdt.getChild(type.name());
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
    
    @Override
    public Iterator<BindingNodeId> read(Node ng, BindingNodeId bnid, Triple triple, ExprList filter, NodeTable nodeTable) {
        // A pattern variable already bound to a node that does not exist in this store
        // (e.g. a VALUES/BIND term not present here) cannot match anything, so the pattern
        // yields no solutions - rather than failing to resolve the missing id.
        if (boundToMissing(triple.getSubject(), bnid)
                || boundToMissing(triple.getPredicate(), bnid)
                || boundToMissing(triple.getObject(), bnid)) {
            return Collections.emptyIterator();
        }
        if (Quad.isUnionGraph(ng)) {
            return readUnion(bnid, triple, filter, nodeTable);
        }
        boolean isDefault = ng.equals(Quad.defaultGraphNodeGenerated) || ng.equals(Quad.defaultGraphIRI);
        Node g = isDefault ? this.defaultGraph : ng;
        // A bound variable's id is used DIRECTLY by the iterators (they consult the
        // binding before the dictionary) whenever its id-space matches the position;
        // only a cross-space binding (predicate id used in an entity position or
        // vice versa) is materialized to its term here so the iterator re-locates it
        // in the position's own dictionary. The former unconditional substitution
        // paid an extract() + full binary search per bound variable per input row.
        Node s = substituteIfCrossSpace(triple.getSubject(), bnid, nodeTable, false);
        Node p = substituteIfCrossSpace(triple.getPredicate(), bnid, nodeTable, true);
        Node o = substituteIfCrossSpace(triple.getObject(), bnid, nodeTable, false);
        Quad quadPattern = new Quad(g, s, p, o);
        return new BGIteratorMaster(this, dict, bnid, quadPattern, filter, nodeTable);
    }

    /**
     * {@code urn:x-arq:unionGraph}: the union of all named graphs, with the
     * SPARQL-mandated set semantics - a triple present in several named graphs
     * appears once. Rows are deduplicated on the values of the pattern's
     * variables (the concrete positions are identical across graphs by
     * construction).
     */
    private Iterator<BindingNodeId> readUnion(BindingNodeId bnid, Triple triple, ExprList filter, NodeTable nodeTable) {
        List<Var> varList = new ArrayList<>(3);
        for (Node n : new Node[]{triple.getSubject(), triple.getPredicate(), triple.getObject()}) {
            if (n.isVariable()) varList.add(Var.alloc(n));
        }
        Var v0 = varList.size() > 0 ? varList.get(0) : null;
        Var v1 = varList.size() > 1 ? varList.get(1) : null;
        Var v2 = varList.size() > 2 ? varList.get(2) : null;
        // Lazy per-graph chaining: constructing every graph's iterator up front
        // paid each one's index binary searches before the first row came back
        // (spatial stores hold thousands of tile graphs).
        Iterator<Node> graphs = dict.streamGraphs()
            .filter(n -> !(n.equals(Quad.defaultGraphIRI) || n.equals(Quad.defaultGraphNodeGenerated)))
            .iterator();
        Iterator<BindingNodeId> chain = Iter.flatMap(graphs, gn -> read(gn, bnid, triple, filter, nodeTable));
        // The dedup set is inherent to union set-semantics (rows arrive per
        // graph, not globally sorted); an open-addressing set of three raw longs
        // keeps a large union scan free of per-row key/box allocations.
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

    /** True when {@code n} is a variable already bound to a node that does not exist here. */
    private static boolean boundToMissing(Node n, BindingNodeId bnid) {
        if (bnid != null && n.isVariable()) {
            return NodeId.isDoesNotExist(bnid.get(Var.alloc(n)));
        }
        return false;
    }

    /**
     * Replaces a bound variable with its concrete term ONLY when its NodeId lives
     * in a different id-space than the position it is used at. GRAPH/SUBJECT ids
     * are entity-space and OBJECT ids are entity-space plus offset literals, so
     * among those three positions a bound id is directly comparable (a literal id
     * in an entity position simply never matches - correct, since a literal cannot
     * be a subject or graph). Only the isolated PREDICATE space needs the
     * materialize-and-relocate round trip, in either direction.
     */
    private Node substituteIfCrossSpace(Node n, BindingNodeId bnid, NodeTable nodeTable, boolean predicatePosition) {
        if (!n.isVariable()) {
            return n;
        }
        long id = bnid.get(Var.alloc(n));
        if (id == NodeId.NONE || NodeId.isDoesNotExist(id)) {
            return n; // unbound (or already short-circuited by boundToMissing)
        }
        if (NodeId.isPredicateSpace(id) == predicatePosition) {
            return n; // same space: the iterator consumes the bound id directly
        }
        Node concrete = nodeTable.getNodeForNodeId(id);
        return (concrete != null) ? concrete : n;
    }

    @Override
    public ExtendedIterator<Triple> graphBaseFind(Node graph, Triple tp) {
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

    @Override
    public void close() {
        if (!open) return;
        open = false;
        hdf.close();
    }

    @Override
    public boolean isOpen() { return open; }

    @Override public GSPODictionary getDictionary() { return dict; }

    @Override
    public Iterator<Node> listGraphNodes() {
        return dict.streamGraphs().iterator();
    }
    
    @Override
    public long countTriples(Node graph) {
        // Quads are de-duplicated per graph in the index, so the graph's quad
        // count is its triple count.
        return IndexCounts.quads(this, graph);
    }

    @Override
    public boolean containsGraph(Node graphNode) {
        // Graphs share the universal entity dictionary, so locate() alone matches
        // every subject/object entity too; membership in the columnar graphs list
        // is what makes an entity an actual graph.
        long id = dict.getGraphs().locate(graphNode);
        return id != -1 && dict.isGraph(id);
    }
    
}
