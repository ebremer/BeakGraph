package com.ebremer.beakgraph.hdtish;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.vocabulary.XSD;

/**
 *
 * @author erbre
 */
public class PlainDictionary implements Dictionary {
    
    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    private final DataOutputStream dos = new DataOutputStream(baos);
    private final ArrayList<Integer> offsets = new ArrayList<>();
    private final ArrayList<NumericType> dt = new ArrayList<>();
    
    public int size() {
        return baos.size();
    }
    public void Add(Node node) {
        if (node.isURI()||node.isBlank()) {
            offsets.add(baos.size());
            try {
                dos.writeChars(node.getURI());
            } catch (UnsupportedEncodingException ex) {
                Logger.getLogger(PlainDictionary.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(PlainDictionary.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else if (node.isLiteral()) {
            String datatype = node.getLiteralDatatypeURI();
            if (datatype.equals(XSD.xlong.getURI())) {
                offsets.add(Long.BYTES);
                dt.add(NumericType.LONG);
                if (node.getLiteralValue() instanceof Long x) {
                    try {
                        dos.writeLong(x);
                    } catch (IOException ex) {
                        Logger.getLogger(PlainDictionary.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } else if (datatype.equals(XSD.xint.getURI())) {
                offsets.add(Integer.BYTES);
                dt.add(NumericType.INT);
                if (node.getLiteralValue() instanceof Integer x) {
                    try {
                        dos.writeInt(x);
                    } catch (IOException ex) {
                        Logger.getLogger(PlainDictionary.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } else {
                throw new Error("What is : "+node);
            }
        } else {
            throw new Error("WTF : "+node);
        }
    }

    @Override
    public int locate(Node element) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public Node extract(int id) {
        return NodeFactory.createBlankNode();
    }
    
}
