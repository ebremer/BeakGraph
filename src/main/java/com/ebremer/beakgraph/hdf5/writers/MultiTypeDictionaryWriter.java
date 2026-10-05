package com.ebremer.beakgraph.hdf5.writers;

import java.util.Comparator;
import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.DictionaryWriter;
import com.ebremer.beakgraph.core.EmptyDictionaryWriter;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.DataOutputBuffer;
import com.ebremer.beakgraph.core.lib.DataType;
import com.ebremer.beakgraph.core.Dictionary;
import com.ebremer.beakgraph.core.lib.NodeSearch;
import com.ebremer.beakgraph.core.lib.NodeSorter;
import com.ebremer.beakgraph.core.lib.Stats;
import com.ebremer.beakgraph.hdf5.DictionarySection;
import static com.ebremer.beakgraph.utils.UTIL.MinBits;
import io.jhdf.api.WritableGroup;
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
import org.apache.jena.graph.Node;

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
    // RDF 1.2 triple terms (format v5): fixed-stride component store - entries
    // [3k, 3k+2] hold the (s, p, o) ids of the triple term whose offsets value
    // is k. Null unless this section is triple-term-enabled and some exist.
    private final BitPackedUnSignedLongBuffer tripleTerms;
    private final TripleTermEncoder tripleTermEncoder;
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
    // rdf:dirLangString support (format v4): a per-node base-direction buffer
    // (0 = none, 1 = ltr, 2 = rtl). The domain is closed, so no dictionary is
    // needed. Null when the source has no base-direction literals - absence
    // means "no directions", which is what keeps v3 files readable unchanged.
    private final BitPackedUnSignedLongBuffer langDirs;
    private final HashMap<String, Long> langLookUp = new HashMap<>();
    private String name;
    private final ArrayList<Node> sorted;
    private Set<DataType> et;
    private int fcdBlockSize = Params.FCD_BLOCK_SIZE;
    private final AtomicLong cc = new AtomicLong();
    private final boolean literalsPresent;
    private boolean closed = false;
    
    protected MultiTypeDictionaryWriter(Builder builder) throws IOException {
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
        requireStrictlyAscending(sorted, name);

        // --- STEP 2: Initialize Buffers ---
        // BitPackedUnSignedLongBuffer constructors do not throw; assign finals directly.
        this.offsets = new BitPackedUnSignedLongBuffer(Path.of("offsets"), 1 + MinBits(builder.getNodeCount()));
        this.nativedatatypes = new BitPackedUnSignedLongBuffer(Path.of("datatypes"), 1 + MinBits(DataType.values().length));
        // Signed-safe widths (Stats.integerWidth / longWidth): when min is negative, a fixed
        // width (32 or 64) so the two's-complement bit pattern survives the unsigned mask
        // round-trip in BitPackedUnSignedLongBuffer; a legal xsd:long in [2^56, 2^62) would
        // land in 58..63, which the buffer does not support, so that rounds up to 64.
        this.integers = (!et.contains(DataType.INTEGER) || (stats.numInteger == 0)) ? null : new BitPackedUnSignedLongBuffer(Path.of("integers"), stats.integerWidth());
        this.longs = (!et.contains(DataType.LONG) || (stats.numLong == 0)) ? null : new BitPackedUnSignedLongBuffer(Path.of("longs"), stats.longWidth());

        // Triple-term component store: width sized to the largest id any
        // component can carry (the object space: entities + this section).
        boolean wantTripleTerms = et.contains(DataType.TRIPLE_TERM) && stats.numTripleTerms > 0;
        if (wantTripleTerms && builder.getTripleTermEncoder() == null) {
            // Encoding would otherwise fail per-node, deep in the sorted walk.
            throw new IllegalStateException(
                    "Dictionary '" + name + "' has " + stats.numTripleTerms
                  + " triple terms but no TripleTermEncoder was supplied");
        }
        int ttWidth = 1 + MinBits(builder.getTripleTermComponentIdBound());
        if (ttWidth > 57) ttWidth = 64;
        this.tripleTerms = wantTripleTerms ? new BitPackedUnSignedLongBuffer(Path.of("tripleTerms"), ttWidth) : null;
        this.tripleTermEncoder = builder.getTripleTermEncoder();

        // Only FCDWriter.add(String) can fail in this block (it declares IOException;
        // the in-memory buffer constructors cannot, BG-89). Temp variables so a
        // writer built earlier in the block is closed if a later step fails.
        FCDWriter tempTypedLiteralsDictionary = null;
        BitPackedUnSignedLongBuffer tempTypedLiterals = null;
        boolean tempLiteralsPresent = false;
        FCDWriter tempIri = null;
        FCDWriter tempStrings = null;
        FCDWriter tempLangs = null;
        BitPackedUnSignedLongBuffer tempLangTags = null;
        BitPackedUnSignedLongBuffer tempLangDirs = null;
        try {
            this.doubles = (!et.contains(DataType.DOUBLE) || (stats.numDouble == 0)) ? null : new DataOutputBuffer(Path.of("doubles"));
            this.floats  = (!et.contains(DataType.FLOAT)  || (stats.numFloat  == 0)) ? null : new DataOutputBuffer(Path.of("floats"));

            if (et.contains(DataType.DOUBLE) || et.contains(DataType.FLOAT) || et.contains(DataType.INTEGER) ||
                et.contains(DataType.LONG) || et.contains(DataType.STRING)) {
                tempLiteralsPresent = true;
                tempTypedLiteralsDictionary = new FCDWriter(Path.of("typedLiteralsDictionary"), fcdBlockSize);
                tempTypedLiterals = new BitPackedUnSignedLongBuffer(Path.of("typedLiterals"), 1 + MinBits(builder.getTypedLiterals().size()));
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

            tempIri     = (!et.contains(DataType.IRI)    || (stats.numIRI     == 0)) ? null : new FCDWriter(Path.of("iri"),     fcdBlockSize);
            tempStrings = (!et.contains(DataType.STRING)  || (stats.numStrings == 0)) ? null : new FCDWriter(Path.of("strings"), fcdBlockSize);

            // Build the language-tag dictionary from the distinct tags present
            // among the literals. Skipped entirely when there are none, so files
            // without language-tagged strings carry no langs/langTags datasets.
            if (tempLiteralsPresent) {
                java.util.TreeSet<String> langSet = new java.util.TreeSet<>();
                boolean anyDirection = false;
                for (Node n : sorted) {
                    if (n.isLiteral()) {
                        String lang = n.getLiteralLanguage();
                        if (lang != null && !lang.isEmpty()) langSet.add(lang);
                        if (n.getLiteralBaseDirection() != null) anyDirection = true;
                    }
                }
                if (!langSet.isEmpty()) {
                    tempLangs = new FCDWriter(Path.of("langs"), fcdBlockSize);
                    for (String lang : langSet) {
                        tempLangs.add(lang);
                        langLookUp.put(lang, tempLangs.getNumEntries()); // 1-based id
                    }
                    tempLangTags = new BitPackedUnSignedLongBuffer(Path.of("langTags"), 1 + MinBits(langSet.size()));
                }
                if (anyDirection) {
                    tempLangDirs = new BitPackedUnSignedLongBuffer(Path.of("langDirs"), 1 + MinBits(2));
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
        this.langDirs                = tempLangDirs;

        // --- STEP 3: Encode Data ---
        this.sorted.forEach(this::addNodeInternal);

        close();   // finalizes the in-memory buffers; nothing here can fail
    }

    private static void closeQuietly(AutoCloseable resource) {
        if (resource != null) {
            try { resource.close(); } catch (Exception ignored) {}
        }
    }

    // Built on first use, once every buffer field is final (BG-298: one
    // encoder for the RAM and the streaming dictionary writers).
    private DictionaryNodeEncoder encoder;

    private void addNodeInternal(Node node) {
        if (encoder == null) {
            encoder = new DictionaryNodeEncoder(name, sorted.size(), offsets, nativedatatypes, typedLiterals,
                    integers, longs, floats, doubles, iri, strings, langTags, langDirs,
                    dataTypesLookUp, langLookUp, literalsPresent,
                    (tripleTerms == null) ? null : tt -> {
                        // Component ids resolve NOW - the section's sort already fixed
                        // every rank, so nested terms resolve through this (partially
                        // encoded) section's own locate() (the "flat second pass").
                        long ordinal = tripleTerms.getNumEntries() / 3;
                        long[] c = tripleTermEncoder.encode(tt, this);
                        tripleTerms.writeLong(c[0]);
                        tripleTerms.writeLong(c[1]);
                        tripleTerms.writeLong(c[2]);
                        return ordinal;
                    });
        }
        encoder.encode(node);
        cc.incrementAndGet();
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        offsets.prepareForReading();
        if (typedLiterals != null) typedLiterals.prepareForReading();
        if (iri != null) iri.close();
        if (integers != null) integers.prepareForReading();
        if (longs != null) longs.prepareForReading();
        if (tripleTerms != null) tripleTerms.prepareForReading();
        nativedatatypes.prepareForReading();
        if (floats != null) floats.close();
        if (doubles != null) doubles.close();
        if (typedLiteralsDictionary != null) typedLiteralsDictionary.close();
        if (strings != null) strings.close();
        if (langs != null) langs.close();
        if (langTags != null) langTags.prepareForReading();
        if (langDirs != null) langDirs.prepareForReading();
    }
    
    @Override public long getNumberOfNodes() { return sorted.size(); }
    
    /**
     * Ids are ranks, so two terms the comparator cannot separate would share
     * one id and every index row touching either would be silently wrong -
     * the dictionaries are the one place a comparator gap (a duplicate in a
     * caller-supplied sorted list, or a NodeCmp regression like the dirLang
     * one NodeComparatorDirLangTest guards) can be caught for every engine at
     * once. One pass of adjacent compares, through the same memoizing
     * comparator the sort used (BG-104).
     */
    private static void requireStrictlyAscending(List<Node> sorted, String name) {
        if (sorted.size() < 2) {
            return;
        }
        Comparator<Node> order = NodeSorter.sortComparator(sorted.size());
        for (int i = 1; i < sorted.size(); i++) {
            int c = order.compare(sorted.get(i - 1), sorted.get(i));
            if (c == 0) {
                throw new IllegalStateException("Dictionary '" + name + "': NodeComparator answers 0 for two entries, "
                        + "which would share one id: " + sorted.get(i - 1) + " vs " + sorted.get(i));
            }
            if (c > 0) {
                throw new IllegalStateException("Dictionary '" + name + "': entries are not in NodeComparator order at "
                        + i + ": " + sorted.get(i - 1) + " > " + sorted.get(i));
            }
        }
    }

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
        // Entry-count gate (like iri/langs below): a literals section whose only
        // rows are triple terms has NO datatype IRIs, and jHDF cannot write an
        // empty dataset - absence already means "none" to the reader.
        if (typedLiteralsDictionary != null && typedLiteralsDictionary.getNumEntries() > 0) typedLiteralsDictionary.add(subGroup);
        if (integers != null) integers.add(subGroup);
        if (longs != null) longs.add(subGroup);
        if (tripleTerms != null) tripleTerms.add(subGroup);
        if (floats != null) floats.add(subGroup);
        if (doubles != null) doubles.add(subGroup);
        if (iri != null && iri.getNumEntries() > 0) iri.add(subGroup);
        if (strings != null) strings.add(subGroup);
        if (langs != null && langs.getNumEntries() > 0) langs.add(subGroup);
        if (langTags != null) langTags.add(subGroup);
        if (langDirs != null) langDirs.add(subGroup);
        if (nativedatatypes.getNumEntries() > 0) nativedatatypes.add(subGroup);
    }

    @Override public List<Node> getNodes() { return sorted; }

    // --- UNSUPPORTED METHODS IN WRITER CONTEXT ---
    @Override public Stream<Node> streamNodes() { throw new UnsupportedOperationException(); }
    @Override public Node extract(long id) { throw new UnsupportedOperationException(); }
    @Override public long search(Node element) { throw new UnsupportedOperationException(); }

    /**
     * Resolves a triple term's component ids at encode time: s in the entity
     * space, p in the predicate space, o in the OBJECT space (entity id, or
     * maxEntityId + section id for literals and nested triple terms).
     * {@code ownSection} is the section being encoded - its sort already fixed
     * every rank, so literal and nested-triple-term objects resolve through it.
     * Implementations MUST throw on an unresolvable component (a build
     * invariant violation), never return an id < 1.
     */
    public interface TripleTermEncoder {
        long[] encode(Node tripleTerm, Dictionary ownSection);
    }

    public static class Builder {
        private Set<Node> nodes = new HashSet<>();
        private ArrayList<Node> sortedNodes;
        private String name;
        private Stats stats;
        private Set<DataType> et = new HashSet<>();
        private Set<String> typedLiterals = new HashSet<>();
        private TripleTermEncoder tripleTermEncoder;
        private long tripleTermComponentIdBound = 0;
        /**
         * The dictionary section this writer builds: fixes the term kinds it
         * routes (SPECIFICATIONS.md §7.2) and, unless set, its group name. The
         * former per-engine enable lists (a second Types enum) were hand-copied in
         * four places and used a second enum whose ordinals disagreed with
         * the on-disk {@link DataType} (BG-90).
         */
        public Builder section(DictionarySection section) {
            et.addAll(section.routes());
            if (name == null) name = section.groupName();
            return this;
        }
        public Builder setTripleTermEncoder(TripleTermEncoder e) { this.tripleTermEncoder = e; return this; }
        /** Upper bound on any component id (the object-space size); sizes the tripleTerms store's bit width. */
        public Builder setTripleTermComponentIdBound(long bound) { this.tripleTermComponentIdBound = bound; return this; }
        public TripleTermEncoder getTripleTermEncoder() { return tripleTermEncoder; }
        public long getTripleTermComponentIdBound() { return tripleTermComponentIdBound; }
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
        public Set<DataType> getEnabledTypes() { return et; }
        public Set<String> getTypedLiterals() { return typedLiterals; }

        public DictionaryWriter build() throws IOException {
            if (getNodeCount() == 0) return new EmptyDictionaryWriter();
            return new MultiTypeDictionaryWriter(this);
        }
    }
}
