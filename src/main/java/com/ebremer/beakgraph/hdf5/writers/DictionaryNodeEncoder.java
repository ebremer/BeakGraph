package com.ebremer.beakgraph.hdf5.writers;

import com.ebremer.beakgraph.core.lib.DataType;
import com.ebremer.beakgraph.hdf5.DictionarySinks.LongSink;
import com.ebremer.beakgraph.hdf5.DictionarySinks.RealSink;
import com.ebremer.beakgraph.hdf5.DictionarySinks.StringSink;
import static com.ebremer.beakgraph.utils.UTIL.isRelativeIRI;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.TextDirection;
import org.apache.jena.vocabulary.XSD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * THE per-node dictionary encoding: which buffer a node lands in, what the
 * offsets / datatypes / typedLiterals / langTags / langDirs columns record
 * for it, and the error stance when a buffer is missing. Written once over
 * the {@link com.ebremer.beakgraph.hdf5.DictionarySinks} interfaces and used
 * by both {@link MultiTypeDictionaryWriter} (in memory, every RAM engine) and
 * the huge writer's {@code StreamingDictionaryWriter} (disk-backed), whose
 * {@code addNodeInternal} bodies were line-for-line mirrors that a change to
 * one had to be copied into the other (BG-298). Triple terms differ only in
 * where their component ids come from, which the {@link TripleTermHook}
 * abstracts.
 *
 * @author Erich Bremer
 */
public final class DictionaryNodeEncoder {

    private static final Logger logger = LoggerFactory.getLogger(DictionaryNodeEncoder.class);

    /**
     * Supplies the {@code offsets} value of a triple term and records its
     * component ids (the RAM writer resolves them now against its own
     * partially encoded section; the huge writer spills references and
     * resolves them later).
     */
    @FunctionalInterface
    public interface TripleTermHook {
        long offsetFor(Node tripleTerm) throws IOException;
    }

    private final String name;
    private final long total;
    private final LongSink offsets;
    private final LongSink datatypes;
    private final LongSink typedLiterals;
    private final LongSink integers;
    private final LongSink longs;
    private final RealSink floats;
    private final RealSink doubles;
    private final StringSink iri;
    private final StringSink strings;
    private final LongSink langTags;
    private final LongSink langDirs;
    private final Map<String, Long> dataTypesLookUp;
    private final Map<String, Long> langLookUp;
    private final boolean literalsPresent;
    private final TripleTermHook tripleTerms;
    private long encoded = 0;

    /**
     * @param total          expected node count (progress logging only)
     * @param typedLiterals  null unless {@code literalsPresent}
     * @param integers       null when the section stores no xsd:int values (likewise longs / floats / doubles)
     * @param iri            null when the section stores no IRIs; strings null when nothing is string-stored
     * @param langTags       null when no literal carries a language tag; langDirs null when none carries a direction
     * @param tripleTerms    null when this section cannot hold triple terms
     */
    public DictionaryNodeEncoder(String name, long total,
                                 LongSink offsets, LongSink datatypes, LongSink typedLiterals,
                                 LongSink integers, LongSink longs, RealSink floats, RealSink doubles,
                                 StringSink iri, StringSink strings, LongSink langTags, LongSink langDirs,
                                 Map<String, Long> dataTypesLookUp, Map<String, Long> langLookUp,
                                 boolean literalsPresent, TripleTermHook tripleTerms) {
        this.name = name;
        this.total = total;
        this.offsets = offsets;
        this.datatypes = datatypes;
        this.typedLiterals = typedLiterals;
        this.integers = integers;
        this.longs = longs;
        this.floats = floats;
        this.doubles = doubles;
        this.iri = iri;
        this.strings = strings;
        this.langTags = langTags;
        this.langDirs = langDirs;
        this.dataTypesLookUp = dataTypesLookUp;
        this.langLookUp = langLookUp;
        this.literalsPresent = literalsPresent;
        this.tripleTerms = tripleTerms;
    }

    /** Nodes encoded so far. */
    public long encoded() {
        return encoded;
    }

    /**
     * Encodes one node of the sorted, distinct section. Every node writes
     * exactly one entry into offsets / datatypes (and the optional per-node
     * columns): a skipped node would leave every later id shifted, so every
     * failure aborts the build instead.
     */
    public void encode(Node node) {
        if (node.isBlank()) {
            // Rank-based BNodes: offset 0, label regenerated from the id on read.
            datatypes.writeInteger(DataType.BNODE.ordinal());
            offsets.writeLong(0);
            perNodeZeros();
        } else if (node.isURI()) {
            try {
                boolean relative = isRelativeIRI(node.getURI());
                offsets.writeLong(iri.getNumEntries());
                datatypes.writeInteger((relative ? DataType.RELATIVE_IRI : DataType.IRI).ordinal());
                iri.add(node.getURI());
                perNodeZeros();
            } catch (IOException ex) {
                // Continuing after a failed iri.add() would leave the offsets and
                // datatypes buffers one entry ahead of the IRI dictionary, silently
                // corrupting every node after this one. Abort the build instead.
                throw new UncheckedIOException("Failed to add IRI to dictionary: " + node, ex);
            }
        } else if (node.isLiteral()) {
            String dt = node.getLiteralDatatypeURI();
            long dtId = dataTypesLookUp.getOrDefault(dt, 0L);
            if (literalsPresent) typedLiterals.writeLong(dtId);
            String lang = node.getLiteralLanguage();
            boolean tagged = lang != null && !lang.isEmpty();
            if (langTags != null) {
                long langId = 0L;
                if (tagged) {
                    // A tag missing from the section's language dictionary
                    // used to be written as 0 ("no tag"), quietly turning
                    // "x"@en into a plain literal on read (BG-369).
                    Long id = langLookUp.get(lang);
                    if (id == null) {
                        throw new IllegalStateException("Language tag '" + lang + "' of " + node
                                + " is missing from dictionary '" + name + "' langs (stats/allocation mismatch)");
                    }
                    langId = id;
                }
                langTags.writeLong(langId);
            } else if (tagged) {
                throw new IllegalStateException("Dictionary '" + name + "' was allocated without language tags but "
                        + node + " carries '" + lang + "' (stats/allocation mismatch)");
            }
            if (langDirs != null) {
                TextDirection dir = node.getLiteralBaseDirection();
                langDirs.writeLong((dir == null) ? 0L : (dir == TextDirection.LTR ? 1L : 2L));
            }
            // An ill-typed literal ("abc"^^xsd:int) has no parseable value but is a
            // valid RDF term: route it to the strings branch below (term-exact, with
            // its datatype IRI) instead of aborting the build here. The stats pass
            // counts those same terms toward numStrings, so the buffer exists.
            Object val;
            try {
                val = node.getLiteralValue();
            } catch (RuntimeException ex) {
                val = null;
            }
            if (dt.equals(XSD.xlong.getURI()) && longs != null && val instanceof Number num) {
                offsets.writeLong(longs.getNumEntries());
                datatypes.writeInteger(DataType.LONG.ordinal());
                longs.writeLong(num.longValue());
            } else if (dt.equals(XSD.xint.getURI()) && integers != null && val instanceof Number num) {
                // Only xsd:int is bit-packed (32-bit). xsd:integer is unbounded and is
                // stored via the strings branch below so its value and datatype survive.
                offsets.writeLong(integers.getNumEntries());
                datatypes.writeInteger(DataType.INTEGER.ordinal());
                integers.writeInteger(num.intValue());
            } else if (dt.equals(XSD.xdouble.getURI()) && doubles != null && val instanceof Number num) {
                offsets.writeLong(doubles.getNumEntries());
                datatypes.writeInteger(DataType.DOUBLE.ordinal());
                try {
                    doubles.writeDouble(num.doubleValue());
                } catch (IOException ex) {
                    throw new UncheckedIOException("Failed to add double literal to dictionary", ex);
                }
            } else if (dt.equals(XSD.xfloat.getURI()) && floats != null && val instanceof Number num) {
                offsets.writeLong(floats.getNumEntries());
                datatypes.writeInteger(DataType.FLOAT.ordinal());
                try {
                    floats.writeFloat(num.floatValue());
                } catch (IOException ex) {
                    throw new UncheckedIOException("Failed to add float literal to dictionary", ex);
                }
            } else if (strings != null) {
                // Fallback for strings, booleans, dates, and custom types
                String lex = node.getLiteralLexicalForm();
                offsets.writeLong(strings.getNumEntries());
                datatypes.writeInteger(DataType.STRING.ordinal());
                try {
                    strings.add(lex);
                } catch (IOException ex) {
                    throw new UncheckedIOException("Failed to add string literal to dictionary", ex);
                }
            } else {
                // Unreachable in normal operation: every string-stored datatype is
                // counted in stats.numStrings, which forces the strings buffer to be
                // allocated. Reaching here means a stats/allocation mismatch.
                throw new IllegalStateException(
                    "No writer buffer for literal datatype " + dt + " (strings buffer not allocated); "
                  + "refusing to write a misaligned dictionary entry.");
            }
        } else if (node.isTripleTerm()) {
            if (tripleTerms == null) {
                // The stats pass never counted this term - a stats/allocation
                // mismatch, the same class of bug as the strings fallback guards.
                throw new IllegalStateException(
                        "Triple term reached dictionary '" + name + "' without triple-term support: " + node);
            }
            long offset;
            try {
                offset = tripleTerms.offsetFor(node);
            } catch (IOException ex) {
                throw new UncheckedIOException("Failed to record triple-term components: " + node, ex);
            }
            offsets.writeLong(offset);
            datatypes.writeInteger(DataType.TRIPLE_TERM.ordinal());
            perNodeZeros();
        } else {
            // A node that is neither blank, URI, literal nor triple term (e.g. a
            // variable) would write NO buffer entries at all, leaving
            // offsets/datatypes one entry short of the sorted node list.
            throw new IllegalStateException("Unsupported node kind in dictionary '" + name + "': " + node);
        }
        encoded++;
        if (encoded % 1_000_000 == 0) {
            logger.info("Dictionary '{}': encoded {} / {} nodes", name, encoded, total);
        }
    }

    /** The per-node literal columns for a non-literal: no datatype, no tag, no direction. */
    private void perNodeZeros() {
        if (literalsPresent) typedLiterals.writeLong(0);
        if (langTags != null) langTags.writeLong(0);
        if (langDirs != null) langDirs.writeLong(0);
    }
}
