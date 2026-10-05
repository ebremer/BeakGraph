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

    /**
     * The set union of several graphs - a {@code FROM <g1> FROM <g2>} dataset
     * clause - with the union graph's semantics: a row present in more than
     * one member is returned once. Members may name the default graph and
     * {@code urn:x-arq:UnionGraph}; an absent member contributes nothing.
     */
    public Iterator<BindingNodeId> readGraphs(java.util.Collection<Node> graphs, BindingNodeId bnid, Triple triple, ExprList filter, NodeTable nodeTable);

    /**
     * The pattern over the graph whose dictionary id is {@code graphId} - an
     * id from {@link #graphIds()}. A caller walking every graph passes the id
     * it already holds instead of a term the reader would locate again (the
     * extract-then-locate round trip, BG-259). An id below 1 answers nothing.
     */
    public Iterator<BindingNodeId> read(long graphId, BindingNodeId bnid, Triple triple, ExprList filter, NodeTable nodeTable);

    /** Ids of the stored graphs (the columnar graph list, ascending), the default graph's included when it holds quads. */
    public java.util.stream.LongStream graphIds();
    public ExtendedIterator<Triple> graphBaseFind(Node graph, Triple tp);
    public Iterator<Node> listGraphNodes();
    public boolean containsGraph(Node graphNode);
    public URI getURI();

    /**
     * Whether this reader is still usable. Pool validation uses this to evict
     * instances whose underlying storage has been closed.
     */
    public default boolean isOpen() { return true; }

    /**
     * The on-disk format version of the store ({@code formatVersion} on the
     * {@code .BG} group; files written before versioning are 1), or -1 when the
     * storage has no such notion. What {@code -verify} prints so operators can
     * tell which stores predate a format change (BG-266).
     */
    public default long getFormatVersion() { return -1; }

    /**
     * Exact triple count of {@code graph} computed from index structure alone,
     * or -1 when not directly computable (union graph, missing index) - callers
     * fall back to counting by scan.
     */
    public default long countTriples(Node graph) { return -1; }
}
