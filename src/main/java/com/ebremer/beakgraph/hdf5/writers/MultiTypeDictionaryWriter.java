package com.ebremer.beakgraph.hdf5.writers;

import com.ebremer.beakgraph.core.DictionaryWriter;
import com.ebremer.beakgraph.core.EmptyDictionaryWriter;
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
import org.apache.jena.vocabulary.XSD;

/**
 *
 * @author erbre
 */
public class MultiTypeDictionaryWriter implements DictionaryWriter, Dictionary, AutoCloseable {
    private final BitPackedUnSignedLongBuffer offsets;
    private final BitPackedUnSignedLongBuffer typedLiterals;
    private final BitPackedUnSignedLongBuffer integers;
    private final BitPackedUnSignedLongBuffer longs;
    private final BitPackedUnSignedLongBuffer nativedatatypes;
    private DataOutputBuffer floats;
    private DataOutputBuffer doubles;
    private final FCDWriter iri;
    private HashMap<String,Long> dataTypesLookUp = new HashMap<>();
    private final FCDWriter typedLiteralsDictionary;
    private final FCDWriter strings;  
    private String name;
    private final ArrayList<Node> sorted;
    private Set<Types> et;
    private int fcdBlockSize = 32;
    private final AtomicLong cc = new AtomicLong();
    private final boolean literalsPresent;
    
    private MultiTypeDictionaryWriter(Builder builder) throws FileNotFoundException, IOException {
        name = builder.getName();
        IO.println("Name           : " + builder.getName());
        IO.println("Nodes          : " + builder.getNodes().size());
        Stats stats = builder.getStats();
        et = builder.getEnabledTypes();
        System.out.print("Sorting nodes...");
        sorted = NodeSorter.parallelSort(builder.getNodes());
        System.out.println("Done.");
        doubles = ( !et.contains( Types.DOUBLE ) || ( stats.numDouble == 0 ) ) ? null : new DataOutputBuffer( Path.of( "doubles" ));
        floats = ( !et.contains( Types.FLOAT ) || ( stats.numFloat == 0 ) ) ? null : new DataOutputBuffer( Path.of( "floats" ));
        offsets = new BitPackedUnSignedLongBuffer( Path.of( "offsets" ), null, 0, 1 + MinBits( builder.getNodes().size()) );
        if (    et.contains( Types.DOUBLE ) ||
                et.contains( Types.FLOAT ) ||
                et.contains( Types.INTEGER ) ||
                et.contains( Types.LONG ) ||
                et.contains( Types.STRING )) {
                    literalsPresent = true;
                    typedLiteralsDictionary = new FCDWriter( Path.of( "typedLiteralsDictionary" ), fcdBlockSize );
                    typedLiterals = new BitPackedUnSignedLongBuffer( Path.of( "typedLiterals" ), null, 0, 1 + MinBits( builder.getTypedLiterals().size()) );
                    IO.println("THE dataTypesDictionary : "+typedLiteralsDictionary.ID);
                    //typedLiteralsDictionary.add("NAL");
                    //dataTypesLookUp.put("NAL", typedLiteralsDictionary.getNumEntries());
                    builder
                        .getTypedLiterals().stream()
                        .sorted()
                        .toList()
                        .forEach(s->{
                            try {
                                //IO.println(typedLiteralsDictionary.ID+" ==== ADD Literal DATA TYPE : "+s);
                                typedLiteralsDictionary.add(s);
                                //IO.println( typedLiteralsDictionary.ID + " ====> typedLiteralsDictionary SIZE : " + typedLiteralsDictionary.getNumEntries() );
                                dataTypesLookUp.put(s, typedLiteralsDictionary.getNumEntries());
                            } catch (IOException ex) {
                                IO.println("Oh hell no : "+ex.getMessage());
                            }
                        });
                    dataTypesLookUp.forEach((k,v)->IO.println("DT LOOKUP : "+k+" -----> "+v));
                } else {
                    typedLiteralsDictionary = null;
                    typedLiterals = null;
                    literalsPresent = false;
                }        

        integers = ( !et.contains( Types.INTEGER ) || ( stats.numInteger == 0 ) ) ? null : new BitPackedUnSignedLongBuffer( Path.of( "integers" ), null, 0, 1 + MinBits( stats.maxInteger) );
        longs = ( !et.contains( Types.LONG ) || ( stats.numLong == 0) )? null : new BitPackedUnSignedLongBuffer( Path.of( "longs" ), null, 0, 1 + MinBits( stats.maxLong ));
        nativedatatypes = new BitPackedUnSignedLongBuffer( Path.of( "datatypes" ), null, 0, 1 + MinBits(DataType.values().length) );
        
        iri = ( !et.contains( Types.IRI ) || ( stats.numIRI == 0)) ? null : new FCDWriter( Path.of( "iri" ), fcdBlockSize );
        strings = ( !et.contains(Types.STRING ) || ( stats.numStrings == 0))? null : new FCDWriter( Path.of( "strings" ), fcdBlockSize );        
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
        if (typedLiterals!=null) {
            typedLiterals.prepareForReading();
        }
        iri.close();        
        if (integers!=null) {
            integers.prepareForReading();
        }
        if (longs!=null) {
            longs.prepareForReading();
        }
        nativedatatypes.prepareForReading();
        if (floats!=null) {
            floats.close();
        }
        if (doubles!=null) {
            doubles.close();
        }
        if (typedLiteralsDictionary!=null) {
            typedLiteralsDictionary.close();
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
            IO.println("CANNOT FIND : " + element );
            return -1;
        }
        return pos + 1;
    }
        
    private void Add(Node node) {        
        if (node.isBlank()) {
            nativedatatypes.writeInteger(DataType.BNODE.ordinal());
            offsets.writeInteger(0);
            if (literalsPresent) typedLiterals.writeInteger(0);
        } else if (node.isURI()) {    
            try {             
                offsets.writeInteger((int) iri.getNumEntries());
                nativedatatypes.writeInteger(DataType.IRI.ordinal());                
                iri.add(node.toString());
                if (literalsPresent) typedLiterals.writeInteger(0);
            } catch (IOException ex) {
                Logger.getLogger(MultiTypeDictionaryWriter.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else if (node.isLiteral()) {
            String dt = node.getLiteralDatatypeURI();
            long ghj = dataTypesLookUp.get(dt);
            typedLiterals.writeInteger((int)ghj);            
            if (dt.equals(XSD.xlong.getURI())) {
                offsets.writeInteger((int) longs.getNumEntries());
                nativedatatypes.writeInteger(DataType.LONG.ordinal());
                longs.writeLong(((Number) node.getLiteralValue()).longValue());
            } else if (dt.equals(XSD.xint.getURI())) {
                offsets.writeInteger((int) integers.getNumEntries());
                integers.writeInteger(((Number) node.getLiteralValue()).intValue());
                nativedatatypes.writeInteger(DataType.INTEGER.ordinal());
            } else if (dt.equals(XSD.integer.getURI())) {
                offsets.writeInteger((int) integers.getNumEntries());
                integers.writeInteger(((Number) node.getLiteralValue()).intValue());
                nativedatatypes.writeInteger(DataType.INTEGER.ordinal());
            } else if (dt.equals(XSD.xdouble.getURI())) {
                if (node.getLiteralValue() instanceof Double x) {
                    try {
                        offsets.writeInteger((int) doubles.getNumEntries());
                        doubles.writeDouble(x);
                        nativedatatypes.writeInteger(DataType.DOUBLE.ordinal());
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
                        nativedatatypes.writeInteger(DataType.FLOAT.ordinal());
                    } catch (IOException ex) {
                        Logger.getLogger(MultiTypeDictionaryWriter.class.getName()).log(Level.SEVERE, null, ex);
                    }
                } else {
                    throw new Error("HELP ME : "+node);
                }
            } else if (dt.equals(GEO.wktLiteral.getURI())) {
                String x = node.getLiteralLexicalForm();
                offsets.writeInteger((int) strings.getNumEntries());
                nativedatatypes.writeInteger(DataType.STRING.ordinal());
                try {
                    strings.add(x);
                } catch (IOException ex) {
                    System.getLogger(MultiTypeDictionaryWriter.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
                }
            } else if (dt.equals(XSD.dateTime.getURI())) {
                String lex = node.getLiteralLexicalForm();
                //int t = lex.indexOf('T');
                //String x = (t > 0) ? lex.substring(0, t) : lex;
                try {
                    offsets.writeInteger((int) strings.getNumEntries());
                    nativedatatypes.writeInteger(DataType.STRING.ordinal());
                    strings.add(lex);
                } catch (IOException ex) {
                    //IO.println("BAAAAAAAAAAAAAAAAAAAAAAAADDDDDDDDDDDDDDDDDDD!!!!");
                    Logger.getLogger(MultiTypeDictionaryWriter.class.getName()).log(Level.SEVERE, null, ex);
                }
            } else if (dt.equals(XSD.xboolean.getURI())) {                             
                if (node.getLiteralValue() instanceof String x) {
                    try {
                        offsets.writeInteger((int) strings.getNumEntries());
                        nativedatatypes.writeInteger(DataType.STRING.ordinal());
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
                        nativedatatypes.writeInteger(DataType.STRING.ordinal());
                        strings.add(x);
                    } catch (IOException ex) {
                        Logger.getLogger(MultiTypeDictionaryWriter.class.getName()).log(Level.SEVERE, null, ex);
                    }
                } else {
                    IO.println("HELP ME : "+node);
                    throw new Error("HELP ME : "+node);
                }
            } else {
                throw new Error("What is : "+node+" "+node.getLiteralDatatypeURI());
            }
        } else {
            //IO.println("WTF : "+node);
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
        if ( typedLiterals != null ) {
            typedLiterals.Add( xx );
        }
        if ( offsets != null ) offsets.Add( xx );        
        if ( typedLiteralsDictionary != null ) {
            typedLiteralsDictionary.Add( xx );
        }
        if ( integers != null ) integers.Add( xx );
        if ( longs != null ) longs.Add( xx );
        if ( floats != null ) floats.Add( xx );
        if ( doubles != null ) doubles.Add( xx );
        if (( iri != null )&&( iri.getNumEntries()>0 )) iri.Add( xx );
        if ( strings != null ) strings.Add( xx );
        if ( nativedatatypes.getNumEntries() > 0 ) nativedatatypes.Add( xx );
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
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public static class Builder {
        private Set<Node> nodes = new HashSet<>();
        private String name;
        private OutputStream baos;
        private Stats stats;
        private Set<Types> et = new HashSet<>();
        private Set<String> typedLiterals = new HashSet<>();
        
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

        public Set<String> getTypedLiterals() {
            return this.typedLiterals;
        }
        
        public Builder setDataTypes(Set<String> typedLiterals) {
            this.typedLiterals = typedLiterals;
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
