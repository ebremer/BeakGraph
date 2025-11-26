package com.ebremer.beakgraph.core;

import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.jena.BindingNodeId;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.collections4.iterators.IteratorChain;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.sparql.core.DatasetGraphBase;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.util.iterator.WrappedIterator;

/**
 *
 * @author erich
 */
public class BGDatasetGraph extends DatasetGraphBase {
    private final BeakGraph bg;
    
    public BGDatasetGraph(BeakGraph g) {
        this.bg = g;
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
        try {
            return new BeakGraph(node, bg.getReader());
        } catch (IOException ex) {
            Logger.getLogger(BGDatasetGraph.class.getName()).log(Level.SEVERE, null, ex);
        }
        return Graph.emptyGraph;
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
        return bg.getReader().listGraphNodes();
    }
    
    @Override
    public boolean containsGraph(Node graphNode) {
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
        // Same logic as find, but specifically for Named Graphs.
        // Since HDF5Reader includes all graphs in listGraphNodes, logic is identical.
        return find(g, s, p, o);
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
        Iterator<BindingNodeId> it = bg.getReader().Read(g, new BindingNodeId(), triplePattern, null, nodeTable);

        // Convert Bindings to Quads
        return WrappedIterator.create(it).mapWith(bnid -> {
            Node sRes = sPattern.isConcrete() ? s : nodeTable.getNodeForNodeId(bnid.get(sVar));
            Node pRes = pPattern.isConcrete() ? p : nodeTable.getNodeForNodeId(bnid.get(pVar));
            Node oRes = oPattern.isConcrete() ? o : nodeTable.getNodeForNodeId(bnid.get(oVar));
            return Quad.create(g, sRes, pRes, oRes);
        });
    }

    private Iterator<Quad> findInAnyGraph(Node s, Node p, Node o) {
        // 1. Default Graph
        Iterator<Quad> defaultGraphIter = findInSpecificGraph(Quad.defaultGraphIRI, s, p, o);
        
        // 2. All Named Graphs
        Iterator<Node> graphs = listGraphNodes();
        List<Iterator<Quad>> iterators = new ArrayList<>();
        iterators.add(defaultGraphIter);
        
        while(graphs.hasNext()) {
            Node graphNode = graphs.next();
            // Skip default if it appears in the list to avoid duplicates
            if (!graphNode.equals(Quad.defaultGraphIRI)) {
                iterators.add(findInSpecificGraph(graphNode, s, p, o));
            }
        }
        
        return new IteratorChain<>(iterators);
    }

    @Override
    public PrefixMap prefixes() {
        return DatasetFactory.createGeneral().asDatasetGraph().prefixes();
    }

    // --- Minimal Transaction Support (Read-Only) ---
    
    @Override
    public boolean supportsTransactions() {
        return true;
    }

    @Override
    public void begin(TxnType type) {
        if (type == TxnType.WRITE) throw new UnsupportedOperationException("Write transactions not supported");
    }

    @Override
    public void begin(ReadWrite readWrite) {
        if (readWrite == ReadWrite.WRITE) throw new UnsupportedOperationException("Write transactions not supported");
    }

    @Override
    public boolean promote(Promote mode) {
        return false;
    }

    @Override
    public void commit() {
        // No-op for read-only
    }

    @Override
    public void abort() {
        // No-op for read-only
    }

    @Override
    public void end() {
        // No-op for read-only
    }

    @Override
    public ReadWrite transactionMode() {
        return ReadWrite.READ;
    }

    @Override
    public TxnType transactionType() {
        return TxnType.READ;
    }

    @Override
    public boolean isInTransaction() {
        // Always behave as if in a transaction or allow access
        return true;
    }
}