package com.ebremer.beakgraph.hdf5.readers;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.GSPODictionary;
import com.ebremer.beakgraph.hdf5.jena.BGIteratorMaster;
import com.ebremer.beakgraph.hdf5.jena.BGReader;
import com.ebremer.beakgraph.hdf5.jena.BindingNodeId;
import com.ebremer.beakgraph.hdf5.jena.NodeId;
import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.hdf5.jena.SimpleNodeTable;
import com.ebremer.beakgraph.turbo.Spatial;
import io.jhdf.HdfFile;
import io.jhdf.api.Attribute;
import io.jhdf.api.Group;
import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
        this.hdf = new HdfFile(src.toPath());
        try {
            this.hdt = (Group) hdf.getChild(Params.BG);
            if (hdt == null) {
                throw new IllegalStateException(
                        "Not a BeakGraph file (no '" + Params.BG + "' group): " + src);
            }
            this.formatVersion = readFormatVersion(hdt);
            if (formatVersion > Params.FORMAT_VERSION) {
                throw new IllegalStateException(
                        "BeakGraph HDF5 format version " + formatVersion + " in " + src
                      + " is newer than this build supports (max " + Params.FORMAT_VERSION
                      + "). Upgrade BeakGraph.");
            }
            Group dictionary = (Group) hdt.getChild(Params.DICTIONARY);
            this.dict = new PositionalDictionaryReader(dictionary);
            this.defaultGraph = Quad.defaultGraphIRI;
            nodeTable = new SimpleNodeTable(dict);
            this.uri = src.toURI();
        } catch (RuntimeException | Error e) {
            // Close the mapped file before propagating: a leaked HdfFile pins the
            // file handle (and on Windows, the file lock) with no way to release it.
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
        Node s = substitute(triple.getSubject(), bnid, nodeTable);
        Node p = substitute(triple.getPredicate(), bnid, nodeTable);
        Node o = substitute(triple.getObject(), bnid, nodeTable);
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
        List<Var> vars = new ArrayList<>(3);
        for (Node n : new Node[]{triple.getSubject(), triple.getPredicate(), triple.getObject()}) {
            if (n.isVariable()) vars.add(Var.alloc(n));
        }
        // Lazy per-graph chaining: constructing every graph's iterator up front
        // paid each one's index binary searches before the first row came back
        // (spatial stores hold thousands of tile graphs).
        Iterator<Node> graphs = dict.streamGraphs()
            .filter(n -> !(n.equals(Quad.defaultGraphIRI) || n.equals(Quad.defaultGraphNodeGenerated)))
            .iterator();
        Iterator<BindingNodeId> chain = Iter.flatMap(graphs, gn -> read(gn, bnid, triple, filter, nodeTable));
        // The dedup set is inherent to union set-semantics (rows arrive per
        // graph, not globally sorted); the key is a record of three primitive
        // longs rather than a boxed List<Long>, cutting the per-row footprint
        // of a large union scan several-fold.
        record RowKey(long a, long b, long c) {}
        Set<RowKey> seen = new HashSet<>();
        return Iter.filter(chain, b -> {
            long[] k = {Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE};
            for (int i = 0; i < vars.size(); i++) {
                NodeId id = b.get(vars.get(i));
                if (id != null) k[i] = id.getId();
            }
            return seen.add(new RowKey(k[0], k[1], k[2]));
        });
    }

    /** True when {@code n} is a variable already bound to a node that does not exist here. */
    private static boolean boundToMissing(Node n, BindingNodeId bnid) {
        if (bnid != null && n.isVariable()) {
            NodeId id = bnid.get(Var.alloc(n));
            return id != null && NodeId.isDoesNotExist(id);
        }
        return false;
    }

    /**
     * Helper to replace Variables in the query pattern with concrete Nodes from the parent binding.
     */
    private Node substitute(Node n, BindingNodeId bnid, NodeTable nodeTable) {
        if (n.isVariable()) {
            Var v = Var.alloc(n);
            NodeId id = bnid.get(v);
            if (id != null) {
                // We found a binding. Resolve the ID to a Node so the Iterator can locate it.
                Node concrete = nodeTable.getNodeForNodeId(id);
                if (concrete != null) {
                    return concrete;
                }
            }
        }
        return n;
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
    public boolean containsGraph(Node graphNode) {
        // Graphs share the universal entity dictionary, so locate() alone matches
        // every subject/object entity too; membership in the columnar graphs list
        // is what makes an entity an actual graph.
        long id = dict.getGraphs().locate(graphNode);
        return id != -1 && dict.isGraph(id);
    }
    
}
