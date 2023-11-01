package com.ebremer.beakgraph.ng;

import java.util.HashMap;
import java.util.Iterator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

/**
 *
 * @author erich
 */
public class NodeTable implements AutoCloseable {
    private final HashMap<String, Integer> map;
    private final HashMap<Resource,Integer> namedgraphs;
    private HashMap<Resource,Integer> blanknodes;
    private final HashMap<Resource,Integer> resources;
    private VarCharVector IRI2idx = null;
    private IntVector idx2id = null;
    private VarCharVector id2IRI = null;
    private IntVector id2ng = null;    
    private IntVector ngid = null;
    
    public NodeTable() {
        blanknodes = new HashMap<>(25000000);
        namedgraphs = new HashMap<>(5000000);
        resources = new HashMap<>();
        map = new HashMap<>();
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
       // System.out.println("getNGID\n"+IRI2idx+"\n"+idx2id+"\n"+id2ng+"\n"+ngid);
        //for (int c=0; c<IRI2idx.getValueCount(); c++) {
          //  System.out.println(new String(IRI2idx.get(c)));
        //}
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
        //System.out.println("getNamedGraph --> "+id+"\n"+id2IRI);
        return NodeFactory.createURI(new String(id2IRI.get(id)));
    }

    public void setDictionaryVectors(VarCharVector IRI2idx, IntVector idx2id, VarCharVector id2IRI, IntVector id2ng) {
        this.IRI2idx = IRI2idx;
        this.idx2id = idx2id;
        this.id2IRI = id2IRI;
        this.id2ng = id2ng;
        resources.clear();
        for (int k=0; k<id2IRI.getValueCount(); k++) {
            Resource r = ResourceFactory.createResource(new String(id2IRI.get(k)));
            //System.out.println("setting resource --> "+r+"     "+k);
            resources.put(r, k);
            map.put(new String(id2IRI.get(k)), k);
        }
    }

    public void setNamedGraphVector(IntVector ngid) {
        this.ngid = ngid;
    }    

    public void setBlankNodes(HashMap<Resource,Integer> blanknodes) {
        this.blanknodes = blanknodes;
    }
    
    public Iterator<Node> listGraphNodes() {
        //System.out.println("# of graphs ---> "+ngid.getValueCount()+"\n"+ngid+"\n"+id2IRI);
        return new NGIterator(ngid, this);
    }
    

    public HashMap<Resource,Integer> getNGResources() {
        return namedgraphs;
    }
    
    public Node getURINode(int id) {
        //System.out.println("getURINode "+id+"\n"+IRI2idx+"\n"+idx2id+"\n"+id2ng+"\n"+id2IRI);
        return NodeFactory.createURI(new String(id2IRI.get(id)));
    }
    
    public HashMap<Resource,Integer> getResources() {
        return resources;
    }
    
    public int AddResource(Resource r) {
        return getID(r);
    }

    public int AddNamedGraph(Resource r) {
        if (namedgraphs.containsKey(r)) {
            return namedgraphs.get(r);
        }
        namedgraphs.put(r, namedgraphs.size());
        return getID(r);
    }
    
    public HashMap<Resource,Integer> getBlankNodes() {
        return blanknodes;
    }
    
    public int getID(Resource s) {
      //  System.out.println("getID(): "+s);
        if (s.isURIResource()) {
            if (resources.containsKey(s)) {
                return resources.get(s);
            } else {
                resources.put(s, resources.size());
                return resources.get(s);
            }
        } else if (s.isAnon()) {
            if (blanknodes.containsKey(s)) {
                return blanknodes.get(s);
            } else {
                blanknodes.put(s, blanknodes.size());
                return blanknodes.get(s);
            }        
        } else {
            throw new Error("What is this : "+s);
        }
    }
    
    public NodeId getNodeIdForNode(Node n) {
        //System.out.println("---->>>>> "+n+" "+n.isBlank());
        if (n.isURI()) {
        //    if (map.containsKey(n.getURI())) {
                return new NodeId(map.get(n.getURI()));
          //  } else {
            //    return NodeId.NodeDoesNotExist;
            //}
        } else if (n.isLiteral()) {
            return new NodeId(n.getLiteralValue());
        }
        throw new Error("UGH");
    }

    public Node getNodeForNodeId(NodeId id) {
       //System.out.println("getNodeForNodeId() : "+id);
        if (id.getType() == NodeType.RESOURCE) {
           // System.out.println("ID : "+id.getID());
            if (id.getID()<0) {
                String k = "_:h"+String.valueOf(-id.getID());
                return NodeFactory.createURI(k);
            }
            String gen = new String(id2IRI.get(id.getID()));
            return NodeFactory.createURI(gen);
        } else if (id.getType() == NodeType.LITERAL) {
            Object x = id.getValue();
            if (x instanceof Float) {
                return NodeFactory.createLiteralByValue(x, XSDDatatype.XSDfloat);
            } else if (x instanceof Long) {
                return NodeFactory.createLiteralByValue(x, XSDDatatype.XSDlong);
            } else if (x instanceof Integer) {
                return NodeFactory.createLiteralByValue(x, XSDDatatype.XSDint);
            } else if (x instanceof org.apache.arrow.vector.util.Text) {
                return NodeFactory.createLiteral(x.toString());
            } else if (x instanceof org.apache.jena.rdf.model.impl.ResourceImpl xxx) {
                //System.out.println("THIS NODE ID VALUE IS : "+id.getID());
                return xxx.asNode();
            }             
            throw new Error("I cannot deal with this : "+x.getClass().toGenericString());
        }
        throw new Error("I cannot deal with this : "+id.getID());
    }
}
