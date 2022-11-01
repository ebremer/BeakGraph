package com.ebremer.beakgraph.rdf;

import com.ebremer.beakgraph.store.NodeId;
import com.ebremer.beakgraph.store.NodeType;
import java.util.HashMap;
import org.apache.arrow.algorithm.search.VectorSearcher;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.Resource;

/**
 *
 * @author erich
 */
public class NodeTable {
    
    private final LargeVarCharVector dictionary;
    private final HashMap<String, Integer> map;
    private final Dictionary dict;
    private final VectorValueComparator<LargeVarCharVector> comparator;
    private long hits = 0;
    private HashMap<String,Integer> blanknodes;
    
    public NodeTable(Dictionary v) {
        this.dict = v;
        dictionary = (LargeVarCharVector) v.getVector();
        map = new HashMap<>(dictionary.getValueCount());
        comparator = DefaultVectorComparators.createDefaultComparator(dictionary);
        blanknodes = new HashMap<>();
    }
    
    public Dictionary getDictionary() {
        return dict;
    }
    
    public void setBlankNodes(HashMap<String,Integer> blanknodes) {
        this.blanknodes = blanknodes;
    }
    
    public void close() {
        dictionary.close();
    }
    
    public HashMap<String,Integer> getBlankNodes() {
        return blanknodes;
    }
    
    public int getID(Resource s) {
        //System.out.println("getID(): "+s);
        if (map.containsKey(s.toString())) {
            return map.get(s.toString());
        } else if (s.isAnon()) {
            if (blanknodes.containsKey(s.toString())) {
                return blanknodes.get(s.toString());
            } else {
                throw new Error("Missing blank node in dictionary : "+s);
            }
        }
        try (
            BufferAllocator allocator = new RootAllocator();
            LargeVarCharVector key = new LargeVarCharVector("", allocator);
        ) {
            key.allocateNew(1);
            key.setValueCount(1);
            key.set(0, s.toString().getBytes());
            int result = VectorSearcher.binarySearch(dictionary, comparator, key, 0);
            if (result!=VectorSearcher.SEARCH_FAIL_RESULT) {
                hits++;
                String blah = new String(dictionary.get(result));
                map.put(blah, result);
                return result;
            }
        }      
        throw new Error("not in dictionary : "+s);
    }
    
    public NodeId getNodeIdForNode(Node n) {
        //System.out.println("---->>>>> "+n+" "+n.isBlank());
        if (n.isURI()) {
            return new NodeId(map.get(n.getURI()));
        } else if (n.isLiteral()) {
            return new NodeId(n.getLiteralValue());
        }
        throw new Error("UGH");
    }

    public Node getNodeForNodeId(NodeId id) {
       //System.out.println("getNodeForNodeId() : "+id+" "+id.getType()+" ---> "+id.getID());
        if (id.getType() == NodeType.RESOURCE) {
            //System.out.println("ID : "+id.getID());
            if (id.getID()<0) {
                String k = "_:h"+String.valueOf(-id.getID());
                return NodeFactory.createURI(k);
            }
            String gen = new String(dictionary.get(id.getID()));
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
