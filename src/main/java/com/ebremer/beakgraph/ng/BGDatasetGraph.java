package com.ebremer.beakgraph.ng;

import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.collections4.Transformer;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.sparql.core.DatasetGraphBase;
import org.apache.jena.sparql.core.Quad;

/**
 *
 * @author erich
 */
public class BGDatasetGraph extends DatasetGraphBase {
    private final BeakGraph bg;
    private final Node DEFAULTGRAPH = NodeFactory.createBlankNode("urn:halcyon:defaultgraph");
    
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
        int s = bg.getReader().getNodeTable().getNGID(node);
        if (s<0) {
            return Graph.emptyGraph;
        }
        BeakGraph bg;
        try {
            bg = new BeakGraph(s, this.bg.getReader());
            return bg;
        } catch (IOException ex) {
            Logger.getLogger(BGDatasetGraph.class.getName()).log(Level.SEVERE, null, ex);
        }
        return Graph.emptyGraph;
    }

    @Override
    public void addGraph(Node graphName, Graph graph) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void removeGraph(Node graphName) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Iterator<Node> listGraphNodes() {
        return bg.getReader().listGraphNodes();
    }

    @Override
    public Iterator<Quad> find(Node g, Node s, Node p, Node o) {
        return IteratorUtils.transformedIterator(bg.find(s, p, o), new Triple2Quad(g));
    }
    
    private class Triple2Quad implements Transformer<Triple, Quad> {
        private final Node g;
        
        public Triple2Quad(Node g) { this.g = g; }
        
        @Override
        public Quad transform(Triple triple) {
            return Quad.create(g, triple);
        }
    }

    @Override
    public Iterator<Quad> findNG(Node g, Node s, Node p, Node o) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public PrefixMap prefixes() {
        return DatasetFactory.createGeneral().asDatasetGraph().prefixes();
        //return new BGPrefixMap();
        //throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean supportsTransactions() {
        return false;
    }

    @Override
    public void begin(TxnType type) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean promote(Promote mode) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void commit() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void abort() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void end() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ReadWrite transactionMode() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public TxnType transactionType() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isInTransaction() {
        return false;
    }
}
