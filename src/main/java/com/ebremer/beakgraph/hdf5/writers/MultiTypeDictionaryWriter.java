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
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.ebremer.beakgraph.utils.UTIL.isRelativeIRI;
import org.apache.jena.graph.Node;
import org.apache.jena.vocabulary.XSD;

/**
 * MultiType Dictionary Writer
 * Implements a Monolithic Entity Dictionary (G, S, O) + Isolated Predicates (P) + Isolated Literals (O).
 * * @author erbre
 */
public class MultiTypeDictionaryWriter implements DictionaryWriter, Dictionary, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(MultiTypeDictionaryWriter.class);
    
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
    // rdf:langString support: a dictionary of distinct language tags (e.g. "en",
    // "fr-CA") plus a per-node id buffer (1-based id into `langs`, 0 = no tag).
    // Both are null when the source has no language-tagged literals.
    private final FCDWriter langs;
    private final BitPackedUnSignedLongBuffer langTags;
    private final HashMap<String, Long> langLookUp = new HashMap<>();
    private String name;
    private final ArrayList<Node> sorted;
    private Set<Types> et;
    private int fcdBlockSize = 16;
    private final AtomicLong cc = new AtomicLong();
    private final boolean literalsPresent;
    private boolean closed = false;
    
    protected MultiTypeDictionaryWriter(Builder builder) throws FileNotFoundException, IOException {
        this.name = builder.getName();
        logger.info("Building dictionary '{}' ({} nodes)", name, builder.getNodeCount());

        Stats stats = builder.getStats();
        this.et = builder.getEnabledTypes();

        // --- STEP 1: Strict Total Ordering ---
        // INFO bracketing: sorting tens of millions of nodes takes minutes with no
        // other output - this is the writer's longest silent phase. A caller that
        // already holds the NodeComparator-sorted list (the ultra writer shares one
        // sort between this dictionary and its node->id map) passes it via
        // setSortedNodes and the sort is skipped entirely.
        if (builder.getSortedNodes() != null) {
            sorted = builder.getSortedNodes();
        } else {
            logger.info("Sorting {} nodes for dictionary '{}'...", builder.getNodes().size(), name);
            long sortStart = System.nanoTime();
            sorted = NodeSorter.parallelSort(builder.getNodes());
            logger.info("Sorted dictionary '{}' in {} s", name, (System.nanoTime() - sortStart) / 1_000_000_000L);
        }

        // --- STEP 2: Initialize Buffers ---
        // BitPackedUnSignedLongBuffer constructors do not throw; assign finals directly.
        this.offsets = new BitPackedUnSignedLongBuffer(Path.of("offsets"), null, 0, 1 + MinBits(builder.getNodeCount()));
        this.nativedatatypes = new BitPackedUnSignedLongBuffer(Path.of("datatypes"), null, 0, 1 + MinBits(DataType.values().length));
        // Signed-safe widths: when min is negative, use a fixed width (32 or 64) so the two's-complement
        // bit pattern survives the unsigned mask round-trip in BitPackedUnSignedLongBuffer.
        int intWidth = (stats.minInteger < 0) ? 32 : (1 + MinBits(stats.maxInteger));
        int longWidth = (stats.minLong < 0) ? 64 : (1 + MinBits(stats.maxLong));
        // The bit-packed buffer supports widths 1..57 and 64 only, and this width is
        // value-derived: a legal xsd:long in [2^56, 2^62) lands in 58..63. Round up.
        if (longWidth > 57) longWidth = 64;
        this.integers = (!et.contains(Types.INTEGER) || (stats.numInteger == 0)) ? null : new BitPackedUnSignedLongBuffer(Path.of("integers"), null, 0, intWidth);
        this.longs = (!et.contains(Types.LONG) || (stats.numLong == 0)) ? null : new BitPackedUnSignedLongBuffer(Path.of("longs"), null, 0, longWidth);

        // FCDWriter and DataOutputBuffer constructors may throw IOException.
        // Use temp variables so that already-opened handles can be closed on failure,
        // preventing file handle leaks if initialization fails partway through.
        FCDWriter tempTypedLiteralsDictionary = null;
        BitPackedUnSignedLongBuffer tempTypedLiterals = null;
        boolean tempLiteralsPresent = false;
        FCDWriter tempIri = null;
        FCDWriter tempStrings = null;
        FCDWriter tempLangs = null;
        BitPackedUnSignedLongBuffer tempLangTags = null;
        try {
            this.doubles = (!et.contains(Types.DOUBLE) || (stats.numDouble == 0)) ? null : new DataOutputBuffer(Path.of("doubles"));
            this.floats  = (!et.contains(Types.FLOAT)  || (stats.numFloat  == 0)) ? null : new DataOutputBuffer(Path.of("floats"));

            if (et.contains(Types.DOUBLE) || et.contains(Types.FLOAT) || et.contains(Types.INTEGER) ||
                et.contains(Types.LONG) || et.contains(Types.STRING)) {
                tempLiteralsPresent = true;
                tempTypedLiteralsDictionary = new FCDWriter(Path.of("typedLiteralsDictionary"), fcdBlockSize);
                tempTypedLiterals = new BitPackedUnSignedLongBuffer(Path.of("typedLiterals"), null, 0, 1 + MinBits(builder.getTypedLiterals().size()));
                final FCDWriter fcdTLD = tempTypedLiteralsDictionary;
                builder.getTypedLiterals().stream()
                    .sorted()
                    .toList()
                    .forEach(s -> {
                        try {
                            fcdTLD.add(s);
                            dataTypesLookUp.put(s, fcdTLD.getNumEntries());
                        } catch (IOException ex) {
                            // A missing datatype entry shifts every later datatype id.
                            throw new UncheckedIOException("Failed to add datatype to dictionary: " + s, ex);
                        }
                    });
            }

            tempIri     = (!et.contains(Types.IRI)    || (stats.numIRI     == 0)) ? null : new FCDWriter(Path.of("iri"),     fcdBlockSize);
            tempStrings = (!et.contains(Types.STRING)  || (stats.numStrings == 0)) ? null : new FCDWriter(Path.of("strings"), fcdBlockSize);

            // Build the language-tag dictionary from the distinct tags present
            // among the literals. Skipped entirely when there are none, so files
            // without language-tagged strings carry no langs/langTags datasets.
            if (tempLiteralsPresent) {
                java.util.TreeSet<String> langSet = new java.util.TreeSet<>();
                for (Node n : sorted) {
                    if (n.isLiteral()) {
                        String lang = n.getLiteralLanguage();
                        if (lang != null && !lang.isEmpty()) langSet.add(lang);
                    }
                }
                if (!langSet.isEmpty()) {
                    tempLangs = new FCDWriter(Path.of("langs"), fcdBlockSize);
                    for (String lang : langSet) {
                        tempLangs.add(lang);
                        langLookUp.put(lang, tempLangs.getNumEntries()); // 1-based id
                    }
                    tempLangTags = new BitPackedUnSignedLongBuffer(Path.of("langTags"), null, 0, 1 + MinBits(langSet.size()));
                }
            }

        } catch (IOException ex) {
            closeQuietly(tempTypedLiteralsDictionary);
            closeQuietly(tempIri);
            closeQuietly(tempStrings);
            closeQuietly(tempLangs);
            closeQuietly(this.doubles);
            closeQuietly(this.floats);
            throw ex;
        }

        // Commit finals now that all allocations succeeded
        this.typedLiteralsDictionary = tempTypedLiteralsDictionary;
        this.typedLiterals           = tempTypedLiterals;
        this.literalsPresent         = tempLiteralsPresent;
        this.iri                     = tempIri;
        this.strings                 = tempStrings;
        this.langs                   = tempLangs;
        this.langTags                = tempLangTags;

        // --- STEP 3: Encode Data ---
        this.sorted.forEach(this::addNodeInternal);

        try {
            close();
        } catch (Exception ex) {
            // A failed finalization means incomplete buffers; writing them out
            // would produce a corrupt dictionary. Abort the build instead.
            throw new IllegalStateException("Dictionary buffer finalization failed for '" + name + "'", ex);
        }
    }

    private static void closeQuietly(AutoCloseable resource) {
        if (resource != null) {
            try { resource.close(); } catch (Exception ignored) {}
        }
    }

    private void addNodeInternal(Node node) {
        if (node.isBlank()) {
            // Rank-based BNodes: We write 0 for offset and regenerate label from ID during read
            nativedatatypes.writeInteger(DataType.BNODE.ordinal());
            offsets.writeLong(0);
            if (literalsPresent) typedLiterals.writeLong(0);
            if (langTags != null) langTags.writeLong(0);
        }
        else if (node.isURI()) {
            try {
                boolean relative = isRelativeIRI(node.getURI());
                offsets.writeLong(iri.getNumEntries());
                nativedatatypes.writeInteger((relative ? DataType.RELATIVE_IRI : DataType.IRI).ordinal());
                iri.add(node.getURI());
                if (literalsPresent) typedLiterals.writeLong(0);
                if (langTags != null) langTags.writeLong(0);
            } catch (IOException ex) {
                // Continuing after a failed iri.add() would leave the offsets and
                // datatypes buffers one entry ahead of the IRI dictionary, silently
                // corrupting every node after this one. Abort the build instead.
                throw new UncheckedIOException("Failed to add IRI to dictionary: " + node, ex);
            }
        }
        else if (node.isLiteral()) {
            String dt = node.getLiteralDatatypeURI();
            long dtId = dataTypesLookUp.getOrDefault(dt, 0L);
            if (literalsPresent) typedLiterals.writeLong(dtId);
            if (langTags != null) {
                String lang = node.getLiteralLanguage();
                langTags.writeLong((lang == null || lang.isEmpty()) ? 0L : langLookUp.getOrDefault(lang, 0L));
            }
            // An ill-typed literal ("abc"^^xsd:int) has no parseable value but is a
            // valid RDF term: route it to the strings branch below (term-exact, with
            // its datatype IRI) instead of aborting the build here. ProcessQuad
            // counts those same terms toward numStrings, so the buffer exists.
            Object val;
            try {
                val = node.getLiteralValue();
            } catch (RuntimeException ex) {
                val = null;
            }
            if (dt.equals(XSD.xlong.getURI()) && longs != null && val instanceof Number num) {
                offsets.writeLong(longs.getNumEntries());
                nativedatatypes.writeInteger(DataType.LONG.ordinal());
                longs.writeLong(num.longValue());
            }
            else if (dt.equals(XSD.xint.getURI()) && integers != null && val instanceof Number num) {
                // Only xsd:int is bit-packed (32-bit). xsd:integer is unbounded and is
                // stored via the strings branch below so its value and datatype survive.
                offsets.writeLong(integers.getNumEntries());
                nativedatatypes.writeInteger(DataType.INTEGER.ordinal());
                integers.writeInteger(num.intValue());
            }
            else if (dt.equals(XSD.xdouble.getURI()) && doubles != null && val instanceof Number num) {
                offsets.writeLong(doubles.getNumEntries());
                nativedatatypes.writeInteger(DataType.DOUBLE.ordinal());
                try {
                    doubles.writeDouble(num.doubleValue());
                } catch (IOException ex) {
                    // See the IRI case: a skipped value desynchronises the dictionary.
                    throw new UncheckedIOException("Failed to add double literal to dictionary", ex);
                }
            }
            else if (dt.equals(XSD.xfloat.getURI()) && floats != null && val instanceof Number num) {
                offsets.writeLong(floats.getNumEntries());
                nativedatatypes.writeInteger(DataType.FLOAT.ordinal());
                try {
                    floats.writeFloat(num.floatValue());
                } catch (IOException ex) {
                    throw new UncheckedIOException("Failed to add float literal to dictionary", ex);
                }
            }
            else if (strings != null) {
                // Fallback for strings, booleans, dates, and custom types
                String lex = node.getLiteralLexicalForm();
                offsets.writeLong(strings.getNumEntries());
                nativedatatypes.writeInteger(DataType.STRING.ordinal());
                try {
                    strings.add(lex);
                } catch (IOException ex) {
                    throw new UncheckedIOException("Failed to add string literal to dictionary", ex);
                }
            }
            else {
                // Unreachable in normal operation: every string-stored datatype is
                // counted in stats.numStrings (PositionalDictionaryWriterBuilder.ProcessQuad),
                // which forces the strings buffer to be allocated above. Reaching here
                // means a stats/allocation mismatch. Fail loudly rather than skip the
                // node, which would leave offsets/datatypes one entry short and corrupt
                // every subsequent node in the dictionary.
                throw new IllegalStateException(
                    "No writer buffer for literal datatype " + dt + " (strings buffer not allocated); "
                  + "refusing to write a misaligned dictionary entry.");
            }
        }
        else {
            // A node that is neither blank, URI nor literal (e.g. an RDF-star triple
            // term) would write NO buffer entries at all, leaving offsets/datatypes
            // one entry short of the sorted node list - every id after it silently
            // shifts. Fail the build loudly instead.
            throw new IllegalStateException("Unsupported node kind in dictionary '" + name + "': " + node);
        }
        long c = cc.incrementAndGet();
        if (c % 1_000_000 == 0) {
            logger.info("Dictionary '{}': encoded {} / {} nodes", name, c, sorted.size());
        }
    }

    @Override
    public void close() throws Exception {
        if (closed) return;
        closed = true;
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
        if (langs != null) langs.close();
        if (langTags != null) langTags.prepareForReading();
    }
    
    @Override public long getNumberOfNodes() { return sorted.size(); }
    
    @Override
    public long locate(Node element) {
        int pos = NodeSearch.findPosition(sorted, element);
        return (pos < 0) ? -1 : pos + 1; // 1-based IDs
    }

    @Override
    public void add(WritableGroup group) {
        WritableGroup subGroup = group.putGroup(name);
        if (typedLiterals != null) typedLiterals.add(subGroup);
        if (offsets != null) offsets.add(subGroup);        
        if (typedLiteralsDictionary != null) typedLiteralsDictionary.add(subGroup);
        if (integers != null) integers.add(subGroup);
        if (longs != null) longs.add(subGroup);
        if (floats != null) floats.add(subGroup);
        if (doubles != null) doubles.add(subGroup);
        if (iri != null && iri.getNumEntries() > 0) iri.add(subGroup);
        if (strings != null) strings.add(subGroup);
        if (langs != null && langs.getNumEntries() > 0) langs.add(subGroup);
        if (langTags != null) langTags.add(subGroup);
        if (nativedatatypes.getNumEntries() > 0) nativedatatypes.add(subGroup);
    }

    @Override public List<Node> getNodes() { return sorted; }

    // --- UNSUPPORTED METHODS IN WRITER CONTEXT ---
    @Override public Stream<Node> streamNodes() { throw new UnsupportedOperationException(); }
    @Override public Node extract(long id) { throw new UnsupportedOperationException(); }
    @Override public long search(Node element) { throw new UnsupportedOperationException(); }

    public static class Builder {
        private Set<Node> nodes = new HashSet<>();
        private ArrayList<Node> sortedNodes;
        private String name;
        private Stats stats;
        private Set<Types> et = new HashSet<>();
        private Set<String> typedLiterals = new HashSet<>();
        public Builder enable(Types... types) { et.addAll(Arrays.asList(types)); return this; }
        public Builder setStats(Stats stats) { this.stats = stats; return this; }
        public Builder setNodes(Set<Node> nodes) { this.nodes = nodes; return this; }
        /**
         * Supplies the node list ALREADY in {@code NodeComparator} order, skipping
         * the internal sort; takes precedence over {@link #setNodes}. The caller
         * owns the ordering contract - a mis-sorted list corrupts every id.
         */
        public Builder setSortedNodes(ArrayList<Node> sortedNodes) { this.sortedNodes = sortedNodes; return this; }
        public Builder setDataTypes(Set<String> typedLiterals) { this.typedLiterals = typedLiterals; return this; }
        public Builder setName(String name) { this.name = name; return this; }
        public String getName() { return name; }
        public Set<Node> getNodes() { return nodes; }
        public ArrayList<Node> getSortedNodes() { return sortedNodes; }
        public int getNodeCount() { return sortedNodes != null ? sortedNodes.size() : nodes.size(); }
        public Stats getStats() { return stats; }
        public Set<Types> getEnabledTypes() { return et; }
        public Set<String> getTypedLiterals() { return typedLiterals; }

        public DictionaryWriter build() throws IOException {
            if (getNodeCount() == 0) return new EmptyDictionaryWriter();
            return new MultiTypeDictionaryWriter(this);
        }
    }
}
