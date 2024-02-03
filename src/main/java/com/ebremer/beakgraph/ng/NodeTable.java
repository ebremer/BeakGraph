package com.ebremer.beakgraph.ng;

import java.util.HashMap;
import java.util.Iterator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.BlankNodeId;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.AnonId;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author erich
 */
public class NodeTable implements AutoCloseable {
    private final HashMap<String, Integer> map;
    private final HashMap<Node,Integer> namedgraphs;
    private HashMap<Node,Integer> blanknodes;
    private final HashMap<Integer,Node> int2blanknodes;
    private final HashMap<Node,Integer> resources;
    private VarCharVector IRI2idx = null;
    private IntVector idx2id = null;
    private VarCharVector id2IRI = null;
    private IntVector id2ng = null;    
    private IntVector ngid = null;
    private static final Logger logger = LoggerFactory.getLogger(NodeTable.class);
    
    public NodeTable() {
        blanknodes = new HashMap<>(25000000);
        namedgraphs = new HashMap<>(5000000);
        int2blanknodes = new HashMap<>(5000000);
        resources = new HashMap<>();
        map = new HashMap<>();
    }
    
    public VarCharVector getid2IRI() {
        return id2IRI;
    }
    
    @Override
    public void close() {
        if (IRI2idx!=null) {
            IRI2idx.close();
        }
        if (idx2id!=null) {
            idx2id.close();
        }
        if (id2IRI!=null) {
            id2IRI.close();
        }
        if (id2ng!=null) {
            id2ng.close();
        }
        if (ngid!=null) {
            ngid.close();
        }
    }
    
    public int getNGID(Node node) {
        int s = Search.Find(IRI2idx, node.getURI());
        if (s<0) {
            return -1;
        }
        s = idx2id.get(s);
        if (id2ng.isNull(s)) {
            return -1;
        }
        return id2ng.get(s);
    }
    
    public Node getNamedGraph(int id) {
        return NodeFactory.createURI(new String(id2IRI.get(id)));
    }

    public void setDictionaryVectors(VarCharVector IRI2idx, IntVector idx2id, VarCharVector id2IRI, IntVector id2ng) {
        this.IRI2idx = IRI2idx;
        this.idx2id = idx2id;
        this.id2IRI = id2IRI;
        this.id2ng = id2ng;
        resources.clear();
        for (int k=0; k<id2IRI.getValueCount(); k++) {
            String hold = new String(id2IRI.get(k));
            System.out.println(k+"   "+hold);
            Resource r = ResourceFactory.createResource(hold);
            resources.put(r.asNode(), k);
            map.put(hold, k);
        }
    }

    public void setNamedGraphVector(IntVector ngid) {
        this.ngid = ngid;
    }    

    public void setBlankNodes(HashMap<Node,Integer> blanknodes) {
        this.blanknodes = blanknodes;
    }
    
    public Iterator<Node> listGraphNodes() {
        return new NGIterator(ngid, this);
    }
    
    public HashMap<Node,Integer> getNGResources() {
        return namedgraphs;
    }
    
    public Node getURINode(int id) {
        if (id<id2IRI.getValueCount()) {
            return NodeFactory.createURI(new String(id2IRI.get(id)));
        }
        return NodeFactory.createURI(BG.NS+"UNKNOWNERROR");
        /*
                    int ff = id2IRI.getValueCount();
            for (int v=0; v<ff; v++) {
                String yay = new String(id2IRI.get(v));
                System.out.println(v+"  "+yay);
            }
        */
    }
    
    public HashMap<Node,Integer> getResources() {
        return resources;
    }
    
    public int AddResource(Node r) {
        return getID(r);
    }

    public int AddNamedGraph(Node r) {
        if (namedgraphs.containsKey(r)) {
            return namedgraphs.get(r);
        }
        namedgraphs.put(r, namedgraphs.size());
        return getID(r);
    }
    
    public HashMap<Node,Integer> getBlankNodes() {
        return blanknodes;
    }
    
    public int getID(Node s) {
        if (s.isURI()) {
            if (resources.containsKey(s)) {
                return resources.get(s);
            } else {
                resources.put(s, resources.size());                
                return resources.get(s);
            }
        } else if (s.isBlank()) {
            if (blanknodes.containsKey(s)) {
                return blanknodes.get(s);
            } else {
                int size = blanknodes.size()+1;
                blanknodes.put(s, size);
                int2blanknodes.putIfAbsent(-size, s);
                return size;
            }        
        } else {
            throw new Error("What is this : "+s);
        }
    }
    
    public NodeId getNodeIdForNode(Node n) {
        if (n.isURI()) {
            return new NodeId(map.get(n.getURI()));
        } else if (n.isLiteral()) {
            return new NodeId(n.getLiteralValue());
        }
        throw new Error("UGH");
    }

    public Node getNodeForNodeId(NodeId id) {
        logger.trace("getNodeForNodeId() : "+id);
        if (id.getType() == NodeType.RESOURCE) {
            logger.trace("ID : "+id.getID());
            if (id.getID()<0) {
                Node bn = int2blanknodes.get(id.getID());
                if (bn!=null) {
                    return bn;
                }
                String k = "h"+String.valueOf(-id.getID());
                Node ha = NodeFactory.createBlankNode(k);
                int2blanknodes.putIfAbsent(id.getID(), ha);
                return ha;
            }
            return NodeFactory.createURI(new String(id2IRI.get(id.getID())));
        } else if (id.getType() == NodeType.LITERAL) {
            Object x = id.getValue();
            if (x instanceof Float) {
                return NodeFactory.createLiteralByValue(x, XSDDatatype.XSDfloat);
            } else if (x instanceof Double) {
                return NodeFactory.createLiteralByValue(x, XSDDatatype.XSDdouble);
            } else if (x instanceof Long) {
                return NodeFactory.createLiteralByValue(x, XSDDatatype.XSDlong);
            } else if (x instanceof Integer) {
                return NodeFactory.createLiteralByValue(x, XSDDatatype.XSDint);
            } else if (x instanceof org.apache.arrow.vector.util.Text) {
                return NodeFactory.createLiteral(x.toString());
            } else if (x instanceof org.apache.jena.rdf.model.impl.ResourceImpl xxx) {
                return xxx.asNode();
            }             
            throw new Error("I cannot deal with this : "+x.getClass().toGenericString());
        }
        throw new Error("I cannot deal with this : "+id.getID());
    }
}
