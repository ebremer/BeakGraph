package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.core.GSPODictionary;
import java.net.URI;
import java.util.Iterator;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.util.iterator.ExtendedIterator;

/**
 * Read-side storage contract. (Two former members - getNumberOfTriples and
 * streamQuads - were stubs that answered 0 / empty and had no callers; they
 * were removed rather than left to mislead.)
 *
 * @author Erich Bremer
 */
public interface BGReader extends AutoCloseable {
    public GSPODictionary getDictionary();
    public NodeTable getNodeTable();
    public Iterator<BindingNodeId> read(Node ng, BindingNodeId bnid, Triple triple, ExprList filter, NodeTable nodeTable);
    public ExtendedIterator<Triple> graphBaseFind(Node graph, Triple tp);
    public Iterator<Node> listGraphNodes();
    public boolean containsGraph(Node graphNode);
    public URI getURI();

    /**
     * Whether this reader is still usable. Pool validation uses this to evict
     * instances whose underlying storage has been closed.
     */
    public default boolean isOpen() { return true; }
}
