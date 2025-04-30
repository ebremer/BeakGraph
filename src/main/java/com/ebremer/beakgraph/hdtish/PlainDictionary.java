package com.ebremer.beakgraph.hdtish;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
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
    
    private final BufferedOutputStream longbaos;
    private final DataOutputStream longdos;
    private final BufferedOutputStream doublebaos;
    private final DataOutputStream doubledos;
    private final BufferedOutputStream floatbaos;
    private final DataOutputStream floatdos;
    private final BufferedOutputStream intbaos;
    private final DataOutputStream intdos;
    private final BufferedOutputStream stringbaos;
    private final DataOutputStream stringdos;
    private final ArrayList<Integer> offsets = new ArrayList<>();
    private final ArrayList<DataType> dt = new ArrayList<>();
    
    public PlainDictionary() throws FileNotFoundException {
        stringbaos = new BufferedOutputStream( new FileOutputStream(new File("/tcga/strings")));
        stringdos = new DataOutputStream(stringbaos);
        longbaos = new BufferedOutputStream(new FileOutputStream(new File("/tcga/longs")));
        longdos = new DataOutputStream(longbaos);
        doublebaos = new BufferedOutputStream(new FileOutputStream(new File("/tcga/doubles")));
        doubledos = new DataOutputStream(doublebaos);
        floatbaos = new BufferedOutputStream(new FileOutputStream(new File("/tcga/floats")));
        floatdos = new DataOutputStream(floatbaos);
        intbaos = new BufferedOutputStream(new FileOutputStream(new File("/tcga/ints")));
        intdos = new DataOutputStream(intbaos);        
    }
    
    public int size() {
        return 0;
    }
    
   
    public void Add(Node node) {
        if (node.isURI()||node.isBlank()) {
            offsets.add(node.toString().getBytes().length);
            dt.add(DataType.STRING);
            try {
                stringdos.writeUTF(node.toString());
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
            } else if (datatype.equals(XSD.xdouble.getURI())) {
                offsets.add(Double.BYTES);
                dt.add(DataType.DOUBLE);
                if (node.getLiteralValue() instanceof Integer x) {
                    try {
                        doubledos.writeDouble(x);
                    } catch (IOException ex) {
                        Logger.getLogger(PlainDictionary.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } else if (datatype.equals(XSD.xfloat.getURI())) {
                offsets.add(Float.BYTES);
                dt.add(DataType.FLOAT);
                if (node.getLiteralValue() instanceof Integer x) {
                    try {
                        floatdos.writeFloat(x);
                    } catch (IOException ex) {
                        Logger.getLogger(PlainDictionary.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } else if (datatype.equals(XSD.xstring.getURI())) {                
                dt.add(DataType.STRING);
                if (node.getLiteralValue() instanceof String x) {
                    offsets.add(x.getBytes().length);
                    try {
                        stringdos.writeUTF(node.toString());
                    } catch (IOException ex) {
                        Logger.getLogger(PlainDictionary.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } else {
                throw new Error("What is : "+node+" "+node.getLiteralDatatypeURI());
            }
        } else {
            throw new Error("WTF : "+node);
        }
    }

    @Override
    public int locate(Node element) {
        if (element.isLiteral()) {
            // look at literal tables
        } else if (element.isURI()) {
            
        }
        return -1;
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
            //    LongBuffer buf = ByteBuffer.wrap(longbaos.toByteArray()).asLongBuffer();
            //    return buf.get(offsets.get(id)/Long.BYTES);
            }
            case DataType.INT -> {
             //   IntBuffer buf = ByteBuffer.wrap(intbaos.toByteArray()).asIntBuffer();
             //   return buf.get(offsets.get(id)/Integer.BYTES);
            }
            case DataType.STRING -> {
                //ByteBuffer buf = ByteBuffer.wrap(stringbaos.toByteArray());
                //int offset = buf.get(offsets.get(id));
               // return readCString(stringbaos.toByteArray(), offset);
            }
        }
        return null;
    }
    
}
