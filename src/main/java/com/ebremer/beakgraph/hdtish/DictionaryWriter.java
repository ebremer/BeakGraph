package com.ebremer.beakgraph.hdtish;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.jena.graph.Node;
import org.apache.jena.vocabulary.XSD;

/**
 *
 * @author erbre
 */
public class DictionaryWriter implements AutoCloseable {
    private final BitPackedWriter offsets;
    private final BitPackedWriter integers;
    private final BitPackedWriter longs;
    private final BitPackedWriter datatype;
    private DataBuffer floats;
    private DataBuffer doubles;
    private FCDBuilder text;    
    
    private DictionaryWriter(Builder builder) throws FileNotFoundException, IOException {
        System.out.print("Sorting nodes...");
        ArrayList<Node> sorted = NodeSorter.parallelSort(builder.getNodes());      
        System.out.println("Done.");
        doubles = new DataBuffer(new File("/tcga/doubles"));
        floats = new DataBuffer(new File("/tcga/floats"));
        offsets = BitPackedWriter.forFile(new File("/tcga/offsets"), MinBits(builder.getNodes().size()));
        integers = BitPackedWriter.forFile(new File("/tcga/integers"), MinBits(builder.getMaxInteger()));
        longs = BitPackedWriter.forFile(new File("/tcga/longs"), MinBits(builder.getMaxLong()));
        datatype = BitPackedWriter.forFile(new File("/tcga/datatypes"), DataType.values().length);
        text = new FCDBuilder(8);
        sorted.forEach(n->Add(n));
    }
    
    @Override
    public void close() throws Exception {
        offsets.close();
        integers.close();
        longs.close();
        datatype.close();
        floats.close();
        doubles.close();
        text.close();
    }
    
    private int MinBits(long x) {
        return (int) Math.ceil(Math.log(x)/Math.log(2d));
    }
    
    private void Add(Node node) {
        if (node.isBlank()) {           
            try {
                datatype.writeInteger(DataType.STRING.ordinal());
                //text.add(node.toString());
                offsets.writeInteger(node.toString().length());
            } catch (IOException ex) {
                Logger.getLogger(DictionaryWriter.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else if (node.isURI()) {           
            try {
                datatype.writeInteger(DataType.STRING.ordinal());
                text.add(node.toString());
                offsets.writeInteger(node.toString().length());
            } catch (IOException ex) {
                Logger.getLogger(DictionaryWriter.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else if (node.isLiteral()) {
            String dt = node.getLiteralDatatypeURI();
            if (dt.equals(XSD.xlong.getURI())) {              
                if (node.getLiteralValue() instanceof Long x) {
                    try {
                        offsets.writeInteger(Long.BYTES);
                        datatype.writeInteger(DataType.LONG.ordinal());  
                        longs.writeLong(x);
                    } catch (IOException ex) {
                        Logger.getLogger(DictionaryWriter.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } else if (dt.equals(XSD.xint.getURI())) {
                if (node.getLiteralValue() instanceof Integer x) {
                    try {
                        offsets.writeInteger(Integer.BYTES);
                        integers.writeInteger(x);
                        datatype.writeInteger(DataType.INT.ordinal());
                    } catch (IOException ex) {
                        Logger.getLogger(DictionaryWriter.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } else if (dt.equals(XSD.xdouble.getURI())) {
                if (node.getLiteralValue() instanceof Double x) {
                    try {
                        offsets.writeInteger(Double.BYTES);
                        doubles.writeDouble(x);
                        datatype.writeInteger(DataType.DOUBLE.ordinal());
                    } catch (IOException ex) {
                        Logger.getLogger(DictionaryWriter.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } else if (dt.equals(XSD.xfloat.getURI())) {
                if (node.getLiteralValue() instanceof Float x) {
                    try {
                        offsets.writeInteger(Float.BYTES);
                        floats.writeFloat(x);
                        datatype.writeInteger(DataType.FLOAT.ordinal());
                    } catch (IOException ex) {
                        Logger.getLogger(DictionaryWriter.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } else if (dt.equals(XSD.xstring.getURI())) {                             
                if (node.getLiteralValue() instanceof String x) {
                    try {
                        offsets.writeInteger(x.getBytes().length);
                        datatype.writeInteger(DataType.STRING.ordinal());
                        text.add(node.toString());
                    } catch (IOException ex) {
                        Logger.getLogger(DictionaryWriter.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } else {
                throw new Error("What is : "+node+" "+node.getLiteralDatatypeURI());
            }
        } else {
            throw new Error("WTF : "+node);
        }
    }
/*
    @Override
    public int locate(Node element) {
        if (element.isLiteral()) {
            // look at literal tables
        } else if (element.isURI()) {
            
        }
        return -1;
    }
*/    
    public static String readCString(byte[] data, int offset) {
        int end = offset;
        while (end < data.length && data[end] != 0) {
            end++;
        }
        return new String(data, offset, end - offset, Charset.forName("UTF-8"));
    }
    
  /*
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
    */
    public static class Builder {
        private Set<Node> nodes = new HashSet<>();
        private long maxLong = Long.MIN_VALUE;
        private int maxInteger = Integer.MIN_VALUE;
        //private File hdtish;

        public long getMaxLong() {
            return maxLong;
        }
        
        public int getMaxInteger() {
            return maxInteger;
        }
        
        public Set<Node> getNodes() {
            return nodes;
        }
        
        public Builder setFile(Set<Node> nodes) {
            this.nodes = nodes;
            return this;
        }
        
        /*
        private Builder Add(Node node) {
           nodes.add(node);
           if (node.isLiteral()) {
               if (node.getLiteralDatatypeURI().equals(XSD.xlong.toString())) {
                   switch (node.getLiteralValue()) {
                       case Long x -> maxLong = Math.max(maxLong, x);
                       case Integer x -> maxInteger = Math.max(maxInteger, x);
                       default -> {}
                   }
               }
           }
           return this;
        }*/
        
        public DictionaryWriter build() throws IOException {                  
            return new DictionaryWriter(this);
        }
    }    
}
