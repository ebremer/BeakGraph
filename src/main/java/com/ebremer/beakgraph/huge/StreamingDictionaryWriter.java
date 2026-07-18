package com.ebremer.beakgraph.huge;

import com.ebremer.beakgraph.core.lib.DataType;
import com.ebremer.beakgraph.core.lib.Stats;
import com.ebremer.beakgraph.hdf5.Types;
import static com.ebremer.beakgraph.utils.UTIL.MinBits;
import static com.ebremer.beakgraph.utils.UTIL.isRelativeIRI;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.TextDirection;
import org.apache.jena.vocabulary.XSD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming twin of
 * {@link com.ebremer.beakgraph.hdf5.writers.MultiTypeDictionaryWriter}: encodes
 * an already-sorted, already-distinct node stream into the identical dictionary
 * group layout (offsets / datatypes / typedLiterals / value stores / FCDs /
 * langs), with every buffer disk-backed. Where the RAM writer sorts a HashSet
 * of all nodes in memory, this writer receives the stream from an external
 * sort-merge, so dictionary size is bounded by disk.
 *
 * <p>The per-node encoding logic (buffer choice, offset bookkeeping, error
 * stance) is a line-for-line mirror of {@code MultiTypeDictionaryWriter.addNodeInternal};
 * any change there needs a matching change here to keep the two writers
 * producing identical files.
 *
 * @author Erich Bremer
 */
final class StreamingDictionaryWriter implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(StreamingDictionaryWriter.class);
    private static final int FCD_BLOCK_SIZE = 16;

    private final String name;
    private final long nodeCount;
    private final SpillBitPackedBuffer offsets;
    private final SpillBitPackedBuffer nativedatatypes;
    private final SpillBitPackedBuffer typedLiterals;
    private final SpillBitPackedBuffer integers;
    private final SpillBitPackedBuffer longs;
    private final SpillDataBuffer floats;
    private final SpillDataBuffer doubles;
    private final SpillFCDWriter iri;
    private final SpillFCDWriter strings;
    private final SpillFCDWriter typedLiteralsDictionary;
    private final SpillFCDWriter langs;
    private final SpillBitPackedBuffer langTags;
    // rdf:dirLangString (format v4): per-node base direction, 0=none 1=ltr 2=rtl.
    // Mirror of MultiTypeDictionaryWriter.langDirs; null when no directional
    // literal exists, so v3-shaped files stay byte-identical.
    private final SpillBitPackedBuffer langDirs;
    // RDF 1.2 triple terms (format v5): the encode pass writes each row's
    // ordinal into offsets and spills component references through ttSupport;
    // the resolved fixed-stride store arrives afterwards via
    // setTripleTermsBuffer (component ids only exist once the dictionary
    // files are complete - PLAN Part IV §IV.8).
    private final HugeTripleTerms ttSupport;
    private SpillBitPackedBuffer tripleTerms;
    private final HashMap<String, Long> dataTypesLookUp = new HashMap<>();
    private final HashMap<String, Long> langLookUp = new HashMap<>();
    private final boolean literalsPresent;
    private long encoded = 0;

    /**
     * @param nodeCount  the exact number of distinct nodes {@link #encode} will
     *                   deliver; fixes the offsets bit width, exactly like the
     *                   RAM writer's {@code builder.getNodes().size()}
     * @param dataTypes  distinct literal datatype IRIs (natural String order),
     *                   empty for dictionaries without literals
     * @param langSet    distinct language tags among the literals, natural order
     * @param anyLangDir true when at least one literal carries a base direction
     *                   (rdf:dirLangString); allocates the langDirs column
     * @param ttSupport  triple-term component machinery, non-null only for the
     *                   literals section of a store containing triple terms
     */
    StreamingDictionaryWriter(Path workDir, String name, long nodeCount, Stats stats,
                              Set<Types> et, SortedSet<String> dataTypes, SortedSet<String> langSet,
                              boolean anyLangDir, HugeTripleTerms ttSupport)
            throws IOException {
        this.ttSupport = ttSupport;
        this.name = name;
        this.nodeCount = nodeCount;
        logger.info("Building dictionary '{}' ({} nodes, disk-backed)", name, nodeCount);

        // Per-dictionary temp subdirectory: FCD writers use their name both for
        // temp files and for the HDF5 group they emit, so the isolation must
        // come from the directory, not a name prefix.
        Path dictDir = Files.createDirectories(workDir.resolve("dict." + name));
        this.offsets = new SpillBitPackedBuffer(dictDir.resolve("offsets"), 1 + MinBits(nodeCount));
        this.nativedatatypes = new SpillBitPackedBuffer(dictDir.resolve("datatypes"),
                1 + MinBits(DataType.values().length));

        this.integers = (!et.contains(Types.INTEGER) || (stats.numInteger == 0)) ? null
                : new SpillBitPackedBuffer(dictDir.resolve("integers"),
                        (stats.minInteger < 0) ? 32 : (1 + MinBits(stats.maxInteger)));
        int longWidth = 0;
        if (et.contains(Types.LONG) && stats.numLong > 0) {
            longWidth = (stats.minLong < 0) ? 64 : (1 + MinBits(stats.maxLong));
            // Same rounding as the RAM writer: the bit-packed format supports
            // widths 1..57 and 64 only.
            if (longWidth > 57) longWidth = 64;
        }
        this.longs = (longWidth == 0) ? null
                : new SpillBitPackedBuffer(dictDir.resolve("longs"), longWidth);

        this.doubles = (!et.contains(Types.DOUBLE) || (stats.numDouble == 0)) ? null
                : new SpillDataBuffer(dictDir.resolve("doubles"));
        this.floats = (!et.contains(Types.FLOAT) || (stats.numFloat == 0)) ? null
                : new SpillDataBuffer(dictDir.resolve("floats"));

        this.literalsPresent = et.contains(Types.DOUBLE) || et.contains(Types.FLOAT)
                || et.contains(Types.INTEGER) || et.contains(Types.LONG) || et.contains(Types.STRING);
        if (literalsPresent) {
            this.typedLiteralsDictionary = new SpillFCDWriter(dictDir, "typedLiteralsDictionary", FCD_BLOCK_SIZE);
            this.typedLiterals = new SpillBitPackedBuffer(dictDir.resolve("typedLiterals"),
                    1 + MinBits(dataTypes.size()));
            for (String dt : dataTypes) {
                typedLiteralsDictionary.add(dt);
                dataTypesLookUp.put(dt, typedLiteralsDictionary.getNumEntries());
            }
        } else {
            this.typedLiteralsDictionary = null;
            this.typedLiterals = null;
        }

        this.iri = (!et.contains(Types.IRI) || (stats.numIRI == 0)) ? null
                : new SpillFCDWriter(dictDir, "iri", FCD_BLOCK_SIZE);
        this.strings = (!et.contains(Types.STRING) || (stats.numStrings == 0)) ? null
                : new SpillFCDWriter(dictDir, "strings", FCD_BLOCK_SIZE);

        if (literalsPresent && !langSet.isEmpty()) {
            this.langs = new SpillFCDWriter(dictDir, "langs", FCD_BLOCK_SIZE);
            for (String lang : langSet) {
                langs.add(lang);
                langLookUp.put(lang, langs.getNumEntries()); // 1-based id
            }
            this.langTags = new SpillBitPackedBuffer(dictDir.resolve("langTags"),
                    1 + MinBits(langSet.size()));
        } else {
            this.langs = null;
            this.langTags = null;
        }
        this.langDirs = (literalsPresent && anyLangDir)
                ? new SpillBitPackedBuffer(dictDir.resolve("langDirs"), 1 + MinBits(2))
                : null;
    }

    /** Encodes the sorted distinct node stream; must deliver exactly {@code nodeCount} nodes. */
    void encode(Iterator<Node> sortedDistinct) throws IOException {
        while (sortedDistinct.hasNext()) {
            addNodeInternal(sortedDistinct.next());
        }
        if (encoded != nodeCount) {
            throw new IllegalStateException("Dictionary '" + name + "': expected " + nodeCount
                    + " nodes but encoded " + encoded);
        }
        completeAll();
    }

    private void addNodeInternal(Node node) {
        if (node.isBlank()) {
            // Rank-based BNodes: offset 0, label regenerated from ID on read.
            nativedatatypes.writeInteger(DataType.BNODE.ordinal());
            offsets.writeLong(0);
            if (literalsPresent) typedLiterals.writeLong(0);
            if (langTags != null) langTags.writeLong(0);
            if (langDirs != null) langDirs.writeLong(0);
        } else if (node.isURI()) {
            try {
                boolean relative = isRelativeIRI(node.getURI());
                offsets.writeLong(iri.getNumEntries());
                nativedatatypes.writeInteger((relative ? DataType.RELATIVE_IRI : DataType.IRI).ordinal());
                iri.add(node.getURI());
                if (literalsPresent) typedLiterals.writeLong(0);
                if (langTags != null) langTags.writeLong(0);
                if (langDirs != null) langDirs.writeLong(0);
            } catch (IOException ex) {
                throw new UncheckedIOException("Failed to add IRI to dictionary: " + node, ex);
            }
        } else if (node.isLiteral()) {
            String dt = node.getLiteralDatatypeURI();
            long dtId = dataTypesLookUp.getOrDefault(dt, 0L);
            if (literalsPresent) typedLiterals.writeLong(dtId);
            if (langTags != null) {
                String lang = node.getLiteralLanguage();
                langTags.writeLong((lang == null || lang.isEmpty()) ? 0L : langLookUp.getOrDefault(lang, 0L));
            }
            if (langDirs != null) {
                TextDirection dir = node.getLiteralBaseDirection();
                langDirs.writeLong((dir == null) ? 0L : (dir == TextDirection.LTR ? 1L : 2L));
            }
            Object val;
            try {
                val = node.getLiteralValue();
            } catch (RuntimeException ex) {
                val = null; // ill-typed literal: strings branch below
            }
            if (dt.equals(XSD.xlong.getURI()) && longs != null && val instanceof Number num) {
                offsets.writeLong(longs.getNumEntries());
                nativedatatypes.writeInteger(DataType.LONG.ordinal());
                longs.writeLong(num.longValue());
            } else if (dt.equals(XSD.xint.getURI()) && integers != null && val instanceof Number num) {
                offsets.writeLong(integers.getNumEntries());
                nativedatatypes.writeInteger(DataType.INTEGER.ordinal());
                integers.writeInteger(num.intValue());
            } else if (dt.equals(XSD.xdouble.getURI()) && doubles != null && val instanceof Number num) {
                offsets.writeLong(doubles.getNumEntries());
                nativedatatypes.writeInteger(DataType.DOUBLE.ordinal());
                try {
                    doubles.writeDouble(num.doubleValue());
                } catch (IOException ex) {
                    throw new UncheckedIOException("Failed to add double literal to dictionary", ex);
                }
            } else if (dt.equals(XSD.xfloat.getURI()) && floats != null && val instanceof Number num) {
                offsets.writeLong(floats.getNumEntries());
                nativedatatypes.writeInteger(DataType.FLOAT.ordinal());
                try {
                    floats.writeFloat(num.floatValue());
                } catch (IOException ex) {
                    throw new UncheckedIOException("Failed to add float literal to dictionary", ex);
                }
            } else if (strings != null) {
                String lex = node.getLiteralLexicalForm();
                offsets.writeLong(strings.getNumEntries());
                nativedatatypes.writeInteger(DataType.STRING.ordinal());
                try {
                    strings.add(lex);
                } catch (IOException ex) {
                    throw new UncheckedIOException("Failed to add string literal to dictionary", ex);
                }
            } else {
                throw new IllegalStateException(
                    "No writer buffer for literal datatype " + dt + " (strings buffer not allocated); "
                  + "refusing to write a misaligned dictionary entry.");
            }
        } else if (node.isTripleTerm()) {
            if (ttSupport == null) {
                throw new IllegalStateException(
                        "Triple term reached dictionary '" + name + "' without triple-term support: " + node);
            }
            try {
                offsets.writeLong(ttSupport.onTripleTerm(node));
            } catch (IOException ex) {
                throw new UncheckedIOException("Failed to spill triple-term components: " + node, ex);
            }
            nativedatatypes.writeInteger(DataType.TRIPLE_TERM.ordinal());
            if (literalsPresent) typedLiterals.writeLong(0);
            if (langTags != null) langTags.writeLong(0);
            if (langDirs != null) langDirs.writeLong(0);
        } else {
            throw new IllegalStateException("Unsupported node kind in dictionary '" + name + "': " + node);
        }
        encoded++;
        if (encoded % 1_000_000 == 0) {
            logger.info("Dictionary '{}': encoded {} / {} nodes", name, encoded, nodeCount);
        }
    }

    private void completeAll() throws IOException {
        offsets.complete();
        nativedatatypes.complete();
        if (typedLiterals != null) typedLiterals.complete();
        if (integers != null) integers.complete();
        if (longs != null) longs.complete();
        if (floats != null) floats.complete();
        if (doubles != null) doubles.complete();
        if (langTags != null) langTags.complete();
        if (langDirs != null) langDirs.complete();
    }

    long getNumberOfNodes() {
        return nodeCount;
    }

    /**
     * Installs the resolved fixed-stride component store (the pipeline runs the
     * reference join once the dictionary files are complete). Must be called
     * before {@link #transferTo} for any section that encoded triple terms.
     */
    void setTripleTermsBuffer(SpillBitPackedBuffer resolved) {
        this.tripleTerms = resolved;
    }

    /** Writes this dictionary as a subgroup, mirroring MultiTypeDictionaryWriter.add(). */
    void transferTo(StreamingHdf5Group group) throws IOException {
        if (ttSupport != null && ttSupport.count() > 0 && tripleTerms == null) {
            throw new IllegalStateException("Dictionary '" + name + "' encoded " + ttSupport.count()
                    + " triple terms but the resolved component store was never installed");
        }
        StreamingHdf5Group subGroup = group.putGroup(name);
        if (typedLiterals != null) typedLiterals.transferTo(subGroup, "typedLiterals");
        offsets.transferTo(subGroup, "offsets");
        // Entry-count gate, mirroring the RAM writer: a literals section whose
        // only rows are triple terms has NO datatype IRIs, and absence already
        // means "none" to the reader.
        if (typedLiteralsDictionary != null && typedLiteralsDictionary.getNumEntries() > 0) {
            typedLiteralsDictionary.transferTo(subGroup);
        }
        if (integers != null) integers.transferTo(subGroup, "integers");
        if (longs != null) longs.transferTo(subGroup, "longs");
        if (tripleTerms != null) tripleTerms.transferTo(subGroup, "tripleTerms");
        if (floats != null) floats.transferTo(subGroup, "floats");
        if (doubles != null) doubles.transferTo(subGroup, "doubles");
        if (iri != null && iri.getNumEntries() > 0) iri.transferTo(subGroup);
        if (strings != null) strings.transferTo(subGroup);
        if (langs != null && langs.getNumEntries() > 0) langs.transferTo(subGroup);
        if (langTags != null) langTags.transferTo(subGroup, "langTags");
        if (langDirs != null) langDirs.transferTo(subGroup, "langDirs");
        if (nativedatatypes.getNumEntries() > 0) nativedatatypes.transferTo(subGroup, "datatypes");
    }

    @Override
    public void close() throws IOException {
        offsets.close();
        nativedatatypes.close();
        if (tripleTerms != null) tripleTerms.close();
        if (typedLiterals != null) typedLiterals.close();
        if (integers != null) integers.close();
        if (longs != null) longs.close();
        if (floats != null) floats.close();
        if (doubles != null) doubles.close();
        if (iri != null) iri.close();
        if (strings != null) strings.close();
        if (typedLiteralsDictionary != null) typedLiteralsDictionary.close();
        if (langs != null) langs.close();
        if (langTags != null) langTags.close();
        if (langDirs != null) langDirs.close();
    }
}
