package com.ebremer.beakgraph.hdtish;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
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
    
    private final ByteArrayOutputStream longbaos = new ByteArrayOutputStream();
    private final DataOutputStream longdos = new DataOutputStream(longbaos);
    private final ByteArrayOutputStream intbaos = new ByteArrayOutputStream();
    private final DataOutputStream intdos = new DataOutputStream(intbaos);
    private final ByteArrayOutputStream stringbaos = new ByteArrayOutputStream();
    private final DataOutputStream stringdos = new DataOutputStream(stringbaos);
    private final ArrayList<Integer> offsets = new ArrayList<>();
    private final ArrayList<DataType> dt = new ArrayList<>();
    
    public int size() {
        return longbaos.size();
    }
    
   
    public void Add(Node node) {
        if (node.isURI()||node.isBlank()) {
            offsets.add(stringbaos.size());
            try {
                stringdos.writeChars(node.getURI());
            } catch (UnsupportedEncodingException ex) {
                Logger.getLogger(PlainDictionary.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(PlainDictionary.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else if (node.isLiteral()) {
            String datatype = node.getLiteralDatatypeURI();
            if (datatype.equals(XSD.xlong.getURI())) {
                offsets.add(Long.BYTES);
                dt.add(DataType.LONG);
                if (node.getLiteralValue() instanceof Long x) {
                    try {
                        longdos.writeLong(x);
                    } catch (IOException ex) {
                        Logger.getLogger(PlainDictionary.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } else if (datatype.equals(XSD.xint.getURI())) {
                offsets.add(Integer.BYTES);
                dt.add(DataType.INT);
                if (node.getLiteralValue() instanceof Integer x) {
                    try {
                        intdos.writeInt(x);
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
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    public static String readCString(byte[] data, int offset) {
        int end = offset;
        while (end < data.length && data[end] != 0) {
            end++;
        }
        return new String(data, offset, end - offset, Charset.forName("UTF-8"));
    }

    @Override
    public Object extract(int id) {
        switch (dt.get(id)) {
            case DataType.LONG -> {
                LongBuffer buf = ByteBuffer.wrap(longbaos.toByteArray()).asLongBuffer();
                return buf.get(offsets.get(id)/Long.BYTES);
            }
            case DataType.INT -> {
                IntBuffer buf = ByteBuffer.wrap(intbaos.toByteArray()).asIntBuffer();
                return buf.get(offsets.get(id)/Integer.BYTES);
            }
            case DataType.STRING -> {
                ByteBuffer buf = ByteBuffer.wrap(stringbaos.toByteArray());
                int offset = buf.get(offsets.get(id));
                return readCString(stringbaos.toByteArray(), offset);
            }
        }
        return null;
    }
    
}
