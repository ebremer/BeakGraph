package com.ebremer.beakgraph.hdf5.readers;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.GSPODictionary;
import com.ebremer.beakgraph.hdf5.jena.BGIteratorMaster;
import com.ebremer.beakgraph.hdf5.jena.BGReader;
import com.ebremer.beakgraph.hdf5.jena.BindingNodeId;
import com.ebremer.beakgraph.hdf5.jena.NodeId;
import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.hdf5.QuadID;
import com.ebremer.beakgraph.hdf5.jena.SimpleNodeTable;
import io.jhdf.HdfFile;
import io.jhdf.api.Group;
import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NullIterator;
import org.apache.jena.util.iterator.WrappedIterator;

public class HDF5Reader implements BGReader {

    private final HdfFile hdf;
    private final Group hdt;
    private final FiveSectionDictionaryReader dict;
    private final long totalQuads;
    private final Node defaultGraph;
    private final SimpleNodeTable nodeTable;
    private final Map<Index, IndexReader> indexCache = new HashMap<>();

    public HDF5Reader(File src) {
        this.hdf = new HdfFile(src.toPath());
        this.hdt = (Group) hdf.getChild(Params.BG);
        Group dictionary = (Group) hdt.getChild(Params.DICTIONARY);
        this.dict = new FiveSectionDictionaryReader(dictionary);
        this.totalQuads = (long) hdt.getAttribute("numQuads").getData();
        this.defaultGraph = Quad.defaultGraphIRI;
        nodeTable = new SimpleNodeTable(dict);
        //dict.getPredicates().streamNodes().forEach(p->{IO.println("PREDICATE : "+p);});
    }
    
    public IndexReader getIndexReader(Index indexType) {
        return indexCache.computeIfAbsent(indexType, type -> {
            try {
                Group indexGroup = (Group) hdt.getChild(type.name());
                return (indexGroup == null) ? null : new IndexReader(indexGroup, type);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        });
    }
    
    @Override
    public Iterator<BindingNodeId> Read(Node ng, BindingNodeId bnid, Triple triple, ExprList filter, NodeTable nodeTable) {
        boolean isDefault = ng.equals(Quad.defaultGraphNodeGenerated) || ng.equals(Quad.defaultGraphIRI);
        Node g = isDefault ? this.defaultGraph : ng;
        Node s = substitute(triple.getSubject(), bnid, nodeTable);
        Node p = substitute(triple.getPredicate(), bnid, nodeTable);
        Node o = substitute(triple.getObject(), bnid, nodeTable);        
        Quad quadPattern = new Quad(g, s, p, o);
        return new BGIteratorMaster(this, dict, bnid, quadPattern, filter, nodeTable);
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
        Iterator<BindingNodeId> it = Read(graph, new BindingNodeId(), pattern, null, nodeTable);

        return WrappedIterator.create(it).mapWith(bnid -> {
            Node sRes = tp.getSubject().isConcrete() ? tp.getSubject() : nodeTable.getNodeForNodeId(bnid.get(sVar));
            NodeId ha = bnid.get(pVar);
            Node pRes = tp.getPredicate().isConcrete() ? tp.getPredicate() : nodeTable.getNodeForNodeId(bnid.get(pVar));
            Node oRes = tp.getObject().isConcrete() ? tp.getObject() : nodeTable.getNodeForNodeId(bnid.get(oVar));
            try {
                return Triple.create(sRes, pRes, oRes);
            } catch (UnsupportedOperationException ex) {
                int c = 0;
                return null;
            }
        });
    }

    @Override
    public NodeTable getNodeTable() { return nodeTable; }
    @Override public void close() { hdf.close(); }
    @Override public GSPODictionary getDictionary() { return dict; }    
    @Override public int getNumberOfTriples(String ng) { return 0; }
    @Override public Stream<Quad> streamQuads() { return Stream.empty(); }
    @Override public Stream<QuadID> streamQuadID() { return Stream.empty(); }

    @Override
    public Iterator<Node> listGraphNodes() {
        return dict.streamGraphs().iterator();
    }
    
    @Override
    public boolean containsGraph(Node graphNode) {
        return (dict.getGraphs().locate(graphNode) != -1);
    }
    
    public BitPackedUnSignedLongBuffer getBitmapBuffer(char type) {
        throw new UnsupportedOperationException("Access via IndexReader");
    }
}