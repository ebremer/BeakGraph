package com.ebremer.beakgraph.hdf5.writers;

import com.ebremer.beakgraph.core.DictionaryWriter;
import com.ebremer.beakgraph.core.EmptyDictionaryWriter;
import com.ebremer.beakgraph.hdf5.BitPackedSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.DataOutputBuffer;
import com.ebremer.beakgraph.core.lib.DataType;
import com.ebremer.beakgraph.core.Dictionary;
import com.ebremer.beakgraph.core.lib.GEO;
import com.ebremer.beakgraph.core.lib.NodeSearch;
import com.ebremer.beakgraph.core.lib.NodeSorter;
import com.ebremer.beakgraph.core.lib.Stats;
import com.ebremer.beakgraph.hdf5.Types;
import static com.ebremer.beakgraph.utils.UTIL.MinBits;
import io.jhdf.api.WritableGroup;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.vocabulary.XSD;

/**
 *
 * @author erbre
 */
public class MultiTypeDictionaryWriter implements DictionaryWriter, Dictionary, AutoCloseable {
    private final BitPackedUnSignedLongBuffer offsets;
    private final BitPackedSignedLongBuffer integers;
    private final BitPackedSignedLongBuffer longs;
    private final BitPackedSignedLongBuffer datatype;
    private DataOutputBuffer floats;
    private DataOutputBuffer doubles;
    private FCDWriter iri;
    private FCDWriter strings;  
    private String name;
    //private final ArrayList<Node> nodes;
    private final ArrayList<Node> sorted;
    private Set<Types> et;
    private final AtomicLong cc = new AtomicLong();
    
    private MultiTypeDictionaryWriter(Builder builder) throws FileNotFoundException, IOException {
        name = builder.getName();
        IO.println("Name           : " + builder.getName());
        IO.println("Nodes          : " + builder.getNodes().size());
        Stats stats = builder.getStats();
        et = builder.getEnabledTypes();
        System.out.print("Sorting nodes...");
        sorted = NodeSorter.parallelSort(builder.getNodes());
       // sorted.forEach(n->IO.println(n));
        System.out.println("Done.");        
        doubles = ( !et.contains( Types.DOUBLE ) || ( stats.numDouble == 0 ) ) ? null : new DataOutputBuffer( Path.of( "doubles" ));
        floats = ( !et.contains( Types.FLOAT ) || ( stats.numFloat == 0 ) ) ? null : new DataOutputBuffer( Path.of( "floats" ));
        offsets = new BitPackedUnSignedLongBuffer( Path.of( "offsets" ), null, 0, MinBits( builder.getNodes().size()) );
        integers = ( !et.contains( Types.INTEGER ) || ( stats.numInteger == 0 ) ) ? null : new BitPackedSignedLongBuffer( Path.of( "integers" ), null, 1 + MinBits( stats.maxInteger) );
        longs = ( !et.contains( Types.LONG ) || ( stats.numLong == 0) )? null : new BitPackedSignedLongBuffer( Path.of( "longs" ), null, 1 + MinBits( stats.maxLong ));
        datatype = new BitPackedSignedLongBuffer( Path.of( "datatype" ), null, MinBits(DataType.values().length) );
        iri = ( !et.contains( Types.IRI ) || ( stats.numIRI == 0)) ? null : new FCDWriter( Path.of( "iri" ), 64 );
        strings = ( !et.contains(Types.STRING ) || ( stats.numStrings == 0))? null : new FCDWriter( Path.of( "strings" ), 64 );
        sorted.forEach( n -> Add(n) );
        try {
            close();
        } catch (Exception ex) {
            Logger.getLogger(MultiTypeDictionaryWriter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void close() throws IOException, Exception {
        offsets.prepareForReading();
        iri.close();
        if (integers!=null) {
            integers.prepareForReading();
        }
        if (longs!=null) {
        longs.prepareForReading();
        }
        datatype.prepareForReading();
        if (floats!=null) {
            floats.close();
        }
        if (doubles!=null) {
            doubles.close();
        }
        if (strings!=null) {
            strings.close();
        }
        IO.println("***************** "+name+"  Sorted : "+sorted.size());
    }
    
    @Override
    public long getNumberOfNodes() {
        return sorted.size();
    }
    
    @Override
    public long locate(Node element) {
        int pos = NodeSearch.findPosition(sorted, element);
        if ( pos < 0 ) {
            return -1;
        }
        return pos + 1;
    }
        
    private void Add(Node node) {        
        if (node.isBlank()) {
            datatype.writeInteger(DataType.BNODE.ordinal());
            offsets.writeInteger(0);            
        } else if (node.isURI()) {    
            try {             
                offsets.writeInteger((int) iri.getNumEntries());
                datatype.writeInteger(DataType.IRI.ordinal());                
                iri.add(node.toString());
            } catch (IOException ex) {
                Logger.getLogger(MultiTypeDictionaryWriter.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else if (node.isLiteral()) {
            String dt = node.getLiteralDatatypeURI();
            if (dt.equals(XSD.xlong.getURI())) {
                offsets.writeInteger((int) longs.getNumEntries());
                datatype.writeInteger(DataType.LONG.ordinal());
                longs.writeLong(((Number) node.getLiteralValue()).longValue());
            } else if (dt.equals(XSD.xint.getURI())) {
                offsets.writeInteger((int) integers.getNumEntries());
                integers.writeInteger(((Number) node.getLiteralValue()).intValue());
                datatype.writeInteger(DataType.INTEGER.ordinal());
            } else if (dt.equals(XSD.integer.getURI())) {
                offsets.writeInteger((int) integers.getNumEntries());
                integers.writeInteger(((Number) node.getLiteralValue()).intValue());
                datatype.writeInteger(DataType.INTEGER.ordinal());
            } else if (dt.equals(XSD.xdouble.getURI())) {
                if (node.getLiteralValue() instanceof Double x) {
                    try {
                        offsets.writeInteger((int) doubles.getNumEntries());
                        doubles.writeDouble(x);
                        datatype.writeInteger(DataType.DOUBLE.ordinal());
                    } catch (IOException ex) {
                        Logger.getLogger(MultiTypeDictionaryWriter.class.getName()).log(Level.SEVERE, null, ex);
                    }
                } else {
                    throw new Error("HELP ME : "+node);
                }
            } else if (dt.equals(XSD.xfloat.getURI())) {
                if (node.getLiteralValue() instanceof Float x) {
                    try {
                        offsets.writeInteger((int) floats.getNumEntries());
                        floats.writeFloat(x);
                        datatype.writeInteger(DataType.FLOAT.ordinal());
                    } catch (IOException ex) {
                        Logger.getLogger(MultiTypeDictionaryWriter.class.getName()).log(Level.SEVERE, null, ex);
                    }
                } else {
                    throw new Error("HELP ME : "+node);
                }
            } else if (dt.equals(GEO.wktLiteral.getURI())) {
                String x = node.getLiteralLexicalForm();
                offsets.writeInteger((int) strings.getNumEntries());
                datatype.writeInteger(DataType.STRING.ordinal());
                try {
                    strings.add(x);
                } catch (IOException ex) {
                    System.getLogger(MultiTypeDictionaryWriter.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
                }
            } else if (dt.equals(XSD.dateTime.getURI())) {
                String lex = node.getLiteralLexicalForm();
                int t = lex.indexOf('T');
                String x = (t > 0) ? lex.substring(0, t) : lex;
                try {
                    offsets.writeInteger((int) strings.getNumEntries());
                    datatype.writeInteger(DataType.STRING.ordinal());
                    strings.add(x);
                } catch (IOException ex) {
                    Logger.getLogger(MultiTypeDictionaryWriter.class.getName()).log(Level.SEVERE, null, ex);
                }
            } else if (dt.equals(XSD.xboolean.getURI())) {                             
                if (node.getLiteralValue() instanceof String x) {
                    try {
                        offsets.writeInteger((int) strings.getNumEntries());
                        datatype.writeInteger(DataType.STRING.ordinal());
                        strings.add(x);
                    } catch (IOException ex) {
                        Logger.getLogger(MultiTypeDictionaryWriter.class.getName()).log(Level.SEVERE, null, ex);
                    }
                } else {
                    throw new Error("HELP ME : "+node);
                }
            } else if (dt.equals(XSD.xstring.getURI())) {                             
                if (node.getLiteralValue() instanceof String x) {
                    try {
                        offsets.writeInteger((int) strings.getNumEntries());
                        datatype.writeInteger(DataType.STRING.ordinal());
                        strings.add(x);
                    } catch (IOException ex) {
                        Logger.getLogger(MultiTypeDictionaryWriter.class.getName()).log(Level.SEVERE, null, ex);
                    }
                } else {
                    throw new Error("HELP ME : "+node);
                }
            } else {
                throw new Error("What is : "+node+" "+node.getLiteralDatatypeURI());
            }
        } else {
            throw new Error("WTF : "+node);
        }
        cc.incrementAndGet();
    }

    public static String readCString(byte[] data, int offset) {
        int end = offset;
        while (end < data.length && data[end] != 0) {
            end++;
        }
        return new String(data, offset, end - offset, Charset.forName("UTF-8"));
    }

    @Override
    public List<Node> getNodes() {
        return sorted;
    }

    @Override
    public void Add( WritableGroup group ) {
        WritableGroup xx = group.putGroup( name );
        if ( offsets != null ) offsets.Add( xx );
        if ( integers != null ) integers.Add( xx );
        if ( longs != null ) longs.Add( xx );
        if ( floats != null ) floats.Add( xx );
        if ( doubles != null ) doubles.Add( xx );
        if (( iri != null )&&( iri.getNumEntries()>0 )) iri.Add( xx );
        if ( strings !=null ) strings.Add( xx );
        if ( datatype.getNumEntries() > 0 ) datatype.Add( xx );
    }

    @Override
    public Stream<Node> streamNodes() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Node extract(long id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long search(Node element) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    public static class Builder {
        private Set<Node> nodes = new HashSet<>();
        private String name;
        private OutputStream baos;
        private Stats stats;
        private Set<Types> et = new HashSet<>();
        private long offset = 0;
        
        public OutputStream getOutputStream() {
            return baos;
        }
        
        public Builder enable(Types... types) {        
            et.addAll(Arrays.asList(types));
            return this;
        }

        public Set<Types> getEnabledTypes() {
            return et;
        }
        
        public Set<Node> getNodes() {
            return nodes;
        }

        public Stats getStats() {
            return this.stats;
        }
        
        public Builder setStats(Stats stats) {
            this.stats = stats;
            return this;
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
            IO.println("Building : "+name);
            if (nodes.isEmpty()) {
                return new EmptyDictionaryWriter();
            }
            return new MultiTypeDictionaryWriter(this);
        }
    }    
}
