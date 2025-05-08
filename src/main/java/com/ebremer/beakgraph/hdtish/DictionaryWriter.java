package com.ebremer.beakgraph.hdtish;

import static com.ebremer.beakgraph.hdtish.UTIL.MinBits;
import java.io.FileNotFoundException;
import java.io.IO;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.jena.graph.Node;
import org.apache.jena.vocabulary.XSD;

/**
 *
 * @author erbre
 */
public class DictionaryWriter implements Dictionary, AutoCloseable {
    private final BitPackedWriter offsets;
    private final BitPackedWriter integers;
    private final BitPackedWriter longs;
    private final BitPackedWriter datatype;
    private DataOutputBuffer floats;
    private DataOutputBuffer doubles;
    private FCDWriter text;  
    private List<HDF5Buffer> list = new ArrayList<>();
    private ArrayList<Node> sorted;
    
    private DictionaryWriter(Builder builder) throws FileNotFoundException, IOException {
        IO.println("Name           : "+builder.getName());
        IO.println("MaxInteger     : " + builder.getMaxInteger());
        IO.println("MaxLong        : " + builder.getMaxLong());
        IO.println("MaxBitsInteger : " + MinBits( builder.getMaxInteger()));
        IO.println("MaxBitsLong    : " + MinBits( builder.getMaxLong()));
        System.out.print("Sorting nodes...");
        sorted = NodeSorter.parallelSort(builder.getNodes());      
        System.out.println("Done.");        
        doubles = new DataOutputBuffer( Path.of(builder.getName(), "doubles"));
        floats = new DataOutputBuffer( Path.of(builder.getName(), "floats"));
        offsets = BitPackedWriter.forBuffer( Path.of( builder.getName(), "offsets"), MinBits( builder.getNodes().size()) );
        integers = BitPackedWriter.forBuffer( Path.of( builder.getName(), "integers"), MinBits( builder.getMaxInteger()) );
        longs = BitPackedWriter.forBuffer( Path.of( builder.getName(), "longs"), MinBits( builder.getMaxLong()) );
        datatype = BitPackedWriter.forBuffer( Path.of( builder.getName(), "datatype"), DataType.values().length );
        text = new FCDWriter(Path.of(builder.getName(), "strings"), 8);
        list.add(text);
        list.add(doubles);
        list.add(floats);
        list.add(integers);
        list.add(longs);
        list.add(offsets);
        sorted.forEach(n->Add(n));
    }
    
    public List<HDF5Buffer> getBuffers() {
        return list;
    }
    
    @Override
    public void close() throws IOException, Exception {
        offsets.close();
        integers.close();
        longs.close();
        datatype.close();
        floats.close();
        doubles.close();
        text.close();
    }
    
    public int getNumberOfNodes() {
        return sorted.size();
    }
        
    @Override
    public int locateGraph(Node element) {
        return NodeSearch.findPosition(sorted, element);
    }

    @Override
    public Object extractGraph(int id) {
        return sorted.get(id);
    }
    
    @Override
    public int locateSubject(Node element) {
        return NodeSearch.findPosition(sorted, element);
    }

    @Override
    public Object extractSubject(int id) {
        return sorted.get(id);
    }
    
    @Override
    public int locatePredicate(Node element) {
        return NodeSearch.findPosition(sorted, element);
    }

    @Override
    public Object extractPredicate(int id) {
        return sorted.get(id);
    }    

    @Override
    public int locateObject(Node element) {
        return NodeSearch.findPosition(sorted, element);
    }

    @Override
    public Object extractObject(int id) {
        return sorted.get(id);
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
        private String name;
        private OutputStream baos;
        //private File hdtish;
        
        public OutputStream getOutputStream() {
            return baos;
        }

        public long getMaxLong() {
            return maxLong;
        }
        
        public int getMaxInteger() {
            return maxInteger;
        }
        
        public Set<Node> getNodes() {
            return nodes;
        }
        
        public Builder setNodes(Set<Node> nodes) {
            this.nodes = nodes;
            return this;
        }
        
        public Builder setName(String name) {
            this.name = name;
            return this;
        }
        
        public String getName() {
            return name;
        }
        
        public Builder setOutputStream(OutputStream baos) {
            this.baos = baos;
            return this;
        }
        
        public DictionaryWriter build() throws IOException {                  
            return new DictionaryWriter(this);
        }
    }    
}
