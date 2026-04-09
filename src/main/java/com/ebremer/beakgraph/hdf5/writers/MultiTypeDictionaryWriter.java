package com.ebremer.beakgraph.hdf5.writers;

import com.ebremer.beakgraph.core.DictionaryWriter;
import com.ebremer.beakgraph.core.EmptyDictionaryWriter;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.DataOutputBuffer;
import com.ebremer.beakgraph.core.lib.DataType;
import com.ebremer.beakgraph.core.Dictionary;
import com.ebremer.beakgraph.core.lib.NodeSearch;
import com.ebremer.beakgraph.core.lib.NodeSorter;
import com.ebremer.beakgraph.core.lib.Stats;
import com.ebremer.beakgraph.hdf5.Types;
import static com.ebremer.beakgraph.utils.UTIL.MinBits;
import io.jhdf.api.WritableGroup;
import java.io.FileNotFoundException;
import java.io.IOException;
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
 * MultiType Dictionary Writer
 * Implements a Monolithic Entity Dictionary (G, S, O) + Isolated Predicates (P) + Isolated Literals (O).
 * * @author erbre
 */
public class MultiTypeDictionaryWriter implements DictionaryWriter, Dictionary, AutoCloseable {
    private static final Logger logger = Logger.getLogger(MultiTypeDictionaryWriter.class.getName());
    
    private final BitPackedUnSignedLongBuffer offsets;
    private final BitPackedUnSignedLongBuffer typedLiterals;
    private final BitPackedUnSignedLongBuffer integers;
    private final BitPackedUnSignedLongBuffer longs;
    private final BitPackedUnSignedLongBuffer nativedatatypes;
    private DataOutputBuffer floats;
    private DataOutputBuffer doubles;
    private final FCDWriter iri;
    private HashMap<String, Long> dataTypesLookUp = new HashMap<>();
    private final FCDWriter typedLiteralsDictionary;
    private final FCDWriter strings;  
    private String name;
    private final ArrayList<Node> sorted;
    private Set<Types> et;
    private int fcdBlockSize = 16;
    private final AtomicLong cc = new AtomicLong();
    private final boolean literalsPresent;
    
    protected MultiTypeDictionaryWriter(Builder builder) throws FileNotFoundException, IOException {
        this.name = builder.getName();
        IO.println("Building Dictionary: " + name);
        IO.println("Total Nodes        : " + builder.getNodes().size());
        
        Stats stats = builder.getStats();
        this.et = builder.getEnabledTypes();
        
        // --- STEP 1: Strict Total Ordering ---
        // We must sort using NodeComparator to ensure BNodes < URIs < Literals
        // and that Literals are ordered mathematically (e.g. 2 < 10)
        System.out.print("Sorting nodes...");
        sorted = NodeSorter.parallelSort(builder.getNodes());
        System.out.println("Done.");
        
        // --- STEP 2: Initialize Buffers ---
        this.doubles = (!et.contains(Types.DOUBLE) || (stats.numDouble == 0)) ? null : new DataOutputBuffer(Path.of("doubles"));
        this.floats = (!et.contains(Types.FLOAT) || (stats.numFloat == 0)) ? null : new DataOutputBuffer(Path.of("floats"));
        this.offsets = new BitPackedUnSignedLongBuffer(Path.of("offsets"), null, 0, 1 + MinBits(builder.getNodes().size()));
        
        if (et.contains(Types.DOUBLE) || et.contains(Types.FLOAT) || et.contains(Types.INTEGER) || 
            et.contains(Types.LONG) || et.contains(Types.STRING)) {
            this.literalsPresent = true;
            this.typedLiteralsDictionary = new FCDWriter(Path.of("typedLiteralsDictionary"), fcdBlockSize);
            this.typedLiterals = new BitPackedUnSignedLongBuffer(Path.of("typedLiterals"), null, 0, 1 + MinBits(builder.getTypedLiterals().size()));            
            builder.getTypedLiterals().stream()
                .sorted()
                .toList()
                .forEach(s -> {
                    try {
                        typedLiteralsDictionary.add(s);
                        dataTypesLookUp.put(s, typedLiteralsDictionary.getNumEntries());
                    } catch (IOException ex) {
                        logger.log(Level.SEVERE, "Failed to add datatype to dictionary", ex);
                    }
                });
        } else {
            this.typedLiteralsDictionary = null;
            this.typedLiterals = null;
            this.literalsPresent = false;
        }
                
        this.integers = (!et.contains(Types.INTEGER) || (stats.numInteger == 0)) ? null : new BitPackedUnSignedLongBuffer(Path.of("integers"), null, 0, 1 + MinBits(stats.maxInteger));
        this.longs = (!et.contains(Types.LONG) || (stats.numLong == 0)) ? null : new BitPackedUnSignedLongBuffer(Path.of("longs"), null, 0, 1 + MinBits(stats.maxLong));
        this.nativedatatypes = new BitPackedUnSignedLongBuffer(Path.of("datatypes"), null, 0, 1 + MinBits(DataType.values().length));
        this.iri = (!et.contains(Types.IRI) || (stats.numIRI == 0)) ? null : new FCDWriter(Path.of("iri"), fcdBlockSize);
        this.strings = (!et.contains(Types.STRING) || (stats.numStrings == 0)) ? null : new FCDWriter(Path.of("strings"), fcdBlockSize);        
        
        // --- STEP 3: Encode Data ---
        this.sorted.forEach(this::addNodeInternal);                                                                   
        
        try {
            close();
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "Error during dictionary buffer finalization", ex);
        }
    }

    private void addNodeInternal(Node node) {        
        if (node.isBlank()) {
            // Rank-based BNodes: We write 0 for offset and regenerate label from ID during read
            nativedatatypes.writeInteger(DataType.BNODE.ordinal());
            offsets.writeInteger(0);
            if (literalsPresent) typedLiterals.writeInteger(0);
        } 
        else if (node.isURI()) {    
            try {              
                offsets.writeInteger((int) iri.getNumEntries());
                nativedatatypes.writeInteger(DataType.IRI.ordinal());                
                iri.add(node.toString());
                if (literalsPresent) typedLiterals.writeInteger(0);
            } catch (IOException ex) {
                logger.log(Level.SEVERE, null, ex);
            }
        } 
        else if (node.isLiteral()) {
            String dt = node.getLiteralDatatypeURI();
            long dtId = dataTypesLookUp.getOrDefault(dt, 0L);
            typedLiterals.writeInteger((int) dtId);
            Object val = node.getLiteralValue();
            if (dt.equals(XSD.xlong.getURI())) {
                offsets.writeInteger((int) longs.getNumEntries());
                nativedatatypes.writeInteger(DataType.LONG.ordinal());
                long l = (val instanceof Number n) ? n.longValue() : Long.parseLong(node.getLiteralLexicalForm());
                longs.writeLong(l);
            } 
            else if (dt.equals(XSD.xint.getURI()) || dt.equals(XSD.integer.getURI())) {
                offsets.writeInteger((int) integers.getNumEntries());
                nativedatatypes.writeInteger(DataType.INTEGER.ordinal());
                int i = (val instanceof Number n) ? n.intValue() : Integer.parseInt(node.getLiteralLexicalForm());
                integers.writeInteger(i);
            } 
            else if (dt.equals(XSD.xdouble.getURI())) {
                offsets.writeInteger((int) doubles.getNumEntries());
                nativedatatypes.writeInteger(DataType.DOUBLE.ordinal());
                try {
                    double d = (val instanceof Number n) ? n.doubleValue() : Double.parseDouble(node.getLiteralLexicalForm());
                    doubles.writeDouble(d);
                } catch (IOException ex) { logger.log(Level.SEVERE, null, ex); }
            } 
            else if (dt.equals(XSD.xfloat.getURI())) {
                offsets.writeInteger((int) floats.getNumEntries());
                nativedatatypes.writeInteger(DataType.FLOAT.ordinal());
                try {
                    float f = (val instanceof Number n) ? n.floatValue() : Float.parseFloat(node.getLiteralLexicalForm());
                    floats.writeFloat(f);
                } catch (IOException ex) { logger.log(Level.SEVERE, null, ex); }
            } 
            else {
                // Fallback for strings, booleans, dates, and custom types
                String lex = node.getLiteralLexicalForm();
                offsets.writeInteger((int) strings.getNumEntries());
                nativedatatypes.writeInteger(DataType.STRING.ordinal());
                try {
                    strings.add(lex);
                } catch (IOException ex) { logger.log(Level.SEVERE, null, ex); }
            }
        }
        cc.incrementAndGet();
    }

    @Override
    public void close() throws Exception {
        offsets.prepareForReading();
        if (typedLiterals != null) typedLiterals.prepareForReading();
        if (iri != null) iri.close();        
        if (integers != null) integers.prepareForReading();
        if (longs != null) longs.prepareForReading();
        nativedatatypes.prepareForReading();
        if (floats != null) floats.close();
        if (doubles != null) doubles.close();
        if (typedLiteralsDictionary != null) typedLiteralsDictionary.close();
        if (strings != null) strings.close();
    }
    
    @Override public long getNumberOfNodes() { return sorted.size(); }
    
    @Override
    public long locate(Node element) {
        int pos = NodeSearch.findPosition(sorted, element);
        return (pos < 0) ? -1 : pos + 1; // 1-based IDs
    }

    @Override
    public void Add(WritableGroup group) {
        WritableGroup subGroup = group.putGroup(name);
        if (typedLiterals != null) typedLiterals.Add(subGroup);
        if (offsets != null) offsets.Add(subGroup);        
        if (typedLiteralsDictionary != null) typedLiteralsDictionary.Add(subGroup);
        if (integers != null) integers.Add(subGroup);
        if (longs != null) longs.Add(subGroup);
        if (floats != null) floats.Add(subGroup);
        if (doubles != null) doubles.Add(subGroup);
        if (iri != null && iri.getNumEntries() > 0) iri.Add(subGroup);
        if (strings != null) strings.Add(subGroup);
        if (nativedatatypes.getNumEntries() > 0) nativedatatypes.Add(subGroup);
    }

    @Override public List<Node> getNodes() { return sorted; }

    // --- UNSUPPORTED METHODS IN WRITER CONTEXT ---
    @Override public Stream<Node> streamNodes() { throw new UnsupportedOperationException(); }
    @Override public Node extract(long id) { throw new UnsupportedOperationException(); }
    @Override public long search(Node element) { throw new UnsupportedOperationException(); }

    public static class Builder {
        private Set<Node> nodes = new HashSet<>();
        private String name;
        private Stats stats;
        private Set<Types> et = new HashSet<>();
        private Set<String> typedLiterals = new HashSet<>();        
        public Builder enable(Types... types) { et.addAll(Arrays.asList(types)); return this; }
        public Builder setStats(Stats stats) { this.stats = stats; return this; }
        public Builder setNodes(Set<Node> nodes) { this.nodes = nodes; return this; }
        public Builder setDataTypes(Set<String> typedLiterals) { this.typedLiterals = typedLiterals; return this; }
        public Builder setName(String name) { this.name = name; return this; }
        public String getName() { return name; }
        public Set<Node> getNodes() { return nodes; }
        public Stats getStats() { return stats; }
        public Set<Types> getEnabledTypes() { return et; }
        public Set<String> getTypedLiterals() { return typedLiterals; }
        
        public DictionaryWriter build() throws IOException {
            if (nodes.isEmpty()) return new EmptyDictionaryWriter();
            return new MultiTypeDictionaryWriter(this);
        }
    }    
}
