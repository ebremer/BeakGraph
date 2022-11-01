package com.ebremer.beakgraph.rdf;

import com.ebremer.beakgraph.store.NodeId;
import com.ebremer.beakgraph.store.NodeType;
import java.util.HashMap;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;

/**
 *
 * @author erich
 */
public class NodeTable {
    
    private final VarCharVector dictionary;
    private final HashMap<String, Integer> map;
    private final Dictionary dict;
    
    public NodeTable(Dictionary v) {
        this.dict = v;
        dictionary = (VarCharVector) v.getVector();
        map = new HashMap<>(dictionary.getValueCount());
        for(int i=0; i<dictionary.getValueCount();i++) {
            map.put(new String(dictionary.get(i)), i);
        }
    }
    
    public Dictionary getDictionary() {
        return dict;
    }

    public int getID(String n) {
        if (map.containsKey(n)) {
            return map.get(n);
        }
        throw new Error("not in dictionary : "+n);
    }
    
    public HashMap<String, Integer> getMap() {
        return map;
    }
    
    public NodeId getNodeIdForNode(Node n) {
        if (n.isURI()) {
            return new NodeId(map.get(n.getURI()));
        }
        if (n.isLiteral()) {
            throw new Error("is Literal");
        }
        throw new Error("UGH");
    }
    
    public String getStringforID(int id) {
        return new String(dictionary.get(id));
    }

    public Node getNodeForNodeId(NodeId id) {
       //System.out.println("getNodeForNodeId() : "+id+" "+id.getType());
        if (id.getType() == NodeType.RESOURCE) {
            String ha = new String(dictionary.get(id.getID()));
            if (ha.startsWith("_:")) {
                //System.out.println("push BLANK NODE "+ha+"  ==== "+id.getID());
                Node bn = NodeFactory.createBlankNode(ha);
                //System.out.println("is bnode? "+bn.isBlank());
                return bn;
            } else {
                String gen = new String(dictionary.get(id.getID()));
                //System.out.println("GEN "+gen);
                return NodeFactory.createURI(gen);
            }
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
                System.out.println("THIS NODE ID VALUE IS : "+id.getID());
                return xxx.asNode();
            }             
            throw new Error("I cannot deal with this : "+x.getClass().toGenericString());
        }
        throw new Error("I cannot deal with this : "+id.getID());
    }
}
