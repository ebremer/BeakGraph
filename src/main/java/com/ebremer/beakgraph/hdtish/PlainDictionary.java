package com.ebremer.beakgraph.hdtish;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.jena.graph.Node;
import org.apache.jena.vocabulary.XSD;

/**
 *
 * @author erbre
 */
public class PlainDictionary implements Dictionary {
    private final BitPackedWriter offsets;
    private final BitPackedWriter integers;
    private final BitPackedWriter longs;
    private final BitPackedWriter datatype;
    private DataBuffer floats;
    private DataBuffer doubles;
    private FCDBuilder text = new FCDBuilder(20);    
    private int width;
    
    private PlainDictionary(Builder builder) throws FileNotFoundException, IOException {
        width = (int) Math.ceil(Math.log(builder.getNodes().size())/Math.log(2d));
        System.out.print("Sorting nodes...");
        ArrayList<Node> sorted = NodeSorter.sortNodes(builder.getNodes());      
        System.out.println("Done.");
        doubles = new DataBuffer(new File("/tcga/doubles"));
        floats = new DataBuffer(new File("/tcga/floats"));
        offsets = BitPackedWriter.forFile(new File("/tcga/offsets"), width);
        integers = BitPackedWriter.forFile(new File("/tcga/integers"), width);
        longs = BitPackedWriter.forFile(new File("/tcga/longs"), width);
        datatype = BitPackedWriter.forFile(new File("/tcga/datatypes"), width);
    }
    
    private void Add(Node node) throws IOException {
        if (node.isURI()||node.isBlank()) {
            datatype.add(DataType.STRING);
            text.add(node.toString());
            offsets.writeInteger(node.toString().length());
        } else if (node.isLiteral()) {
            String dt = node.getLiteralDatatypeURI();
            if (dt.equals(XSD.xlong.getURI())) {              
                if (node.getLiteralValue() instanceof Long x) {
                    try {
                        offsets.writeInteger(Long.BYTES);
                        datatype.writeInteger(DataType.LONG);  
                        longs.writeLong(width);
                    } catch (IOException ex) {
                        Logger.getLogger(PlainDictionary.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } else if (dt.equals(XSD.xint.getURI())) {
                if (node.getLiteralValue() instanceof Integer x) {
                    try {
                        offsets.writeInteger(Integer.BYTES);
                        integers.writeInteger(width);
                        datatype.writeInteger(DataType.INT);
                    } catch (IOException ex) {
                        Logger.getLogger(PlainDictionary.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } else if (dt.equals(XSD.xdouble.getURI())) {
                if (node.getLiteralValue() instanceof Double x) {
                    try {
                        offsets.writeInteger(Double.BYTES);
                        doubles.writeDouble(x);
                        datatype.writeInteger(DataType.DOUBLE);
                    } catch (IOException ex) {
                        Logger.getLogger(PlainDictionary.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } else if (dt.equals(XSD.xfloat.getURI())) {
                if (node.getLiteralValue() instanceof Float x) {
                    try {
                        offsets.writeInteger(Float.BYTES);
                        doubles.writeDouble(x);
                        datatype.writeInteger(DataType.FLOAT);
                    } catch (IOException ex) {
                        Logger.getLogger(PlainDictionary.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } else if (dt.equals(XSD.xstring.getURI())) {                             
                if (node.getLiteralValue() instanceof String x) {
                    offsets.writeInteger(x.getBytes().length);
                    datatype.writeInteger(DataType.STRING);
                    text.add(node.toString());
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
        switch (datatype.get(id)) {

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
    
    public static class Builder {
        private HashSet<Node> nodes = new HashSet<>();
        
        public HashSet<Node> getNodes() {
            return nodes;
        }
        
        public void Add(Node node) throws IOException {
           nodes.add(node);
        }
        
        public PlainDictionary build() throws IOException {                  
            return new PlainDictionary(this);
        }
    }
    
}
