package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.core.GSPODictionary;
import com.ebremer.beakgraph.hdf5.QuadID;
import java.util.Iterator;
import java.util.stream.Stream;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.expr.ExprList;

/**
 *
 * @author Erich Bremer
 */
public interface BGReader extends AutoCloseable {   
    public GSPODictionary getDictionary();
    public int getNumberOfTriples(String ng);
    public NodeTable getNodeTable();
    public Iterator<BindingNodeId> Read(Node ng, BindingNodeId bnid, Triple triple, ExprList filter, NodeTable nodeTable);
    public Iterator<Node> listGraphNodes();
    public Stream<Quad> streamQuads();
    public boolean containsGraph(Node graphNode);
    public Stream<QuadID> streamQuadID();
}
