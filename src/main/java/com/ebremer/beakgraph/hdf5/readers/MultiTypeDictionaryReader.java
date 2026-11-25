package com.ebremer.beakgraph.hdf5.readers;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.AbstractDictionary;
import com.ebremer.beakgraph.core.lib.DataType;
import com.ebremer.beakgraph.core.lib.NodeComparator;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.io.RandomAccessBytes;
import com.ebremer.beakgraph.store.StoreArray;
import com.ebremer.beakgraph.store.StoreGroup;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.jena.cdt.CompositeDatatypeBase;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.TextDirection;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.langtagx.LangTagX;
import org.apache.jena.sparql.expr.NodeValue;

public class MultiTypeDictionaryReader extends AbstractDictionary {
    private static final DataType[] DT_VALUES = DataType.values();
    private static final TypeMapper tm = TypeMapper.getInstance();
    private static final int TIER_SPACING = 1024;
    /**
     * Term -> raw search result (id, or negative insertion point). The store is
     * immutable, so both hits and misses are permanent - and query execution
     * re-locates the SAME concrete pattern terms once per input binding (each
     * incoming row constructs fresh iterators), which made the tiered binary
     * search the dominant join cost. Sized via -Dbeakgraph.dict.search.cache.size.
     */
    private static final long SEARCH_CACHE_SIZE = Long.getLong("beakgraph.dict.search.cache.size", 65_536L);
    private final BitPackedUnSignedLongBuffer offsets;
    private final BitPackedUnSignedLongBuffer integers;
    private final BitPackedUnSignedLongBuffer longs;
    private final BitPackedUnSignedLongBuffer datatype;
    private final BitPackedUnSignedLongBuffer typedLiterals;
    private final RandomAccessBytes floats;
    private final RandomAccessBytes doubles;
    private final FCDReader iri;
    private final FCDReader strings;
    private final FCDReader typedLiteralsDictionary;
    // rdf:langString support: dictionary of distinct language tags + per-node
    // 1-based id buffer (0 = no tag). Both null for files written before
    // language-tag support, so those reconstruct exactly as before.
    private final FCDReader langs;
    private final BitPackedUnSignedLongBuffer langTags;
    // rdf:dirLangString (format v4): per-node base direction, 0=none 1=ltr 2=rtl.
    // Null for files written before direction support - those reconstruct
    // exactly as before.
    private final BitPackedUnSignedLongBuffer langDirs;
    // RDF 1.2 triple terms (format v5): fixed-stride component store - entries
    // [3k, 3k+2] hold the (s, p, o) component ids of the triple term whose
    // offsets value is k. Null for files/sections without triple terms.
    private final BitPackedUnSignedLongBuffer tripleTerms;
    // Injected by PositionalDictionaryReader after every section exists: a
    // triple term's components live in DIFFERENT dictionaries (s: entities,
    // p: predicates, o: object space), which this per-section reader cannot
    // reach on its own (CHANGELOG.md "Format v5 design notes"). Volatile only for safe publication;
    // it is wired once, before any query can run.
    private volatile TripleTermResolver tripleTermResolver;
    // RDF-array sequences (format v7, SPECIFICATIONS.md §7.6b): sequence k's
    // element ids (object space, 0 for a slot) are
    // sequenceElements[starts[k], starts[k+1]). sequenceStarts is null for a
    // section without sequences; sequenceElements also when every one is ().
    private final BitPackedUnSignedLongBuffer sequenceStarts;
    private final BitPackedUnSignedLongBuffer sequenceElements;
    private final long numSlots;
    // -arrays: each sequence row's array class (null without one).
    private final BitPackedUnSignedLongBuffer sequenceArrays;
    // -seqindex (RDFARRY.md step 8): the element index, the transpose of the
    // two above: element k of indexElements (ascending, 0 a slot) is held by
    // the sequences indexSequences[indexStarts[k], indexStarts[k+1]). All
    // three null without one.
    private final BitPackedUnSignedLongBuffer indexElements;
    private final BitPackedUnSignedLongBuffer indexStarts;
    private final BitPackedUnSignedLongBuffer indexSequences;
    // -arrays-only (format v8, RDFARRY.md step 9): the sequences of the tensor and ragged classes have no
    // canonical elements, only their rows in /.BG/arrays.
    private final boolean arraysOnly;
    // Injected by PositionalDictionaryReader, as the triple-term resolver is:
    // an element lives anywhere in the object space.
    private volatile SequenceSupport sequenceSupport;
    private final long numEntries;
    // Language tags decoded once at open: the set is tiny, extract() paid an
    // FCD lookup per tagged row, and Jena re-formats every tag it is handed
    // (NodeFactory.createLiteralLang -> LangTagX.formatLanguageTag), so a
    // stored tag that is not already in formatted form (SPECIFICATIONS.md
    // §6.2 requires the formatted form) would be silently rewritten on read
    // into a term whose comparator order differs from the file's (BG-368).
    private final String[] langTable;

    /** Cross-dictionary materialization of a triple term's component ids. */
    public interface TripleTermResolver {
        Node resolve(long subjectId, long predicateId, long objectId);
    }

    /**
     * The object space a sequence's elements live in: an element id's term
     * ({@code extract}), and a term's id ({@code locate}, below 1 for a miss).
     */
    public interface SequenceSupport {
        Node extract(long objectId);
        long locate(Node element);
    }

    // Tiered index, built LAZILY on first search: building it in the
    // constructor performed numEntries/TIER_SPACING full extracts at every
    // file-open, paying for a search accelerator the caller might never use.
    // Volatile publication keeps concurrent first-searchers safe; the benign
    // race builds identical content over immutable data.
    private volatile TieredIndex tiered;

    // Weight mirrors SimpleNodeTable.weightOf: probe keys are caller-supplied
    // nodes, and a composite (cdt:) literal key retains its parsed value, so a
    // count-based bound could pin far more heap than the entry count implies.
    private final com.github.benmanes.caffeine.cache.Cache<Node, Long> searchCache =
            com.github.benmanes.caffeine.cache.Caffeine.newBuilder()
                    .maximumWeight(SEARCH_CACHE_SIZE)
                    .weigher((Node n, Long pos) -> weightOf(n))
                    .build();

    /** A probe key's cache weight: a long literal or a long sequence counts for several (SimpleNodeTable.weightOf). */
    private static int weightOf(Node n) {
        if (n.isLiteral()) {
            return 1 + (n.getLiteralLexicalForm().length() >>> 8);
        }
        if (n.isNodeSeq()) {
            return 1 + (n.getSeq().size() >>> 4);
        }
        return 1;
    }

    /**
     * Sampled ids/terms plus, for the literal tier terms, their NodeValues:
     * every literal search compares the target against ~log2(tier) of them,
     * and building the value from the term each time went through Jena's one
     * global NodeValue cache (a hash of the lexical form per probe; BG-244).
     */
    private record TieredIndex(long[] ids, Node[] nodes, IdentityHashMap<Node, NodeValue> values) {}
    private static final TieredIndex EMPTY_TIER = new TieredIndex(new long[0], new Node[0], new IdentityHashMap<>());

    public MultiTypeDictionaryReader(StoreGroup d) {
        setName(d.name());
        StoreArray offsetsDS = StoreProfile.requireArray(d, "offsets");
        this.numEntries = StoreProfile.longAttr(offsetsDS, Params.NUM_ENTRIES);
        this.offsets = StoreProfile.requirePacked(d, "offsets");
        this.datatype = StoreProfile.requirePacked(d, "datatypes");
        this.typedLiterals = StoreProfile.packed(d, "typedLiterals");
        this.doubles = StoreProfile.bytes(d, "doubles");
        this.floats = StoreProfile.bytes(d, "floats");
        this.integers = StoreProfile.packed(d, "integers");
        this.longs = StoreProfile.packed(d, "longs");
        this.tripleTerms = StoreProfile.packed(d, "tripleTerms");
        this.sequenceStarts = StoreProfile.packed(d, Params.SEQUENCE_STARTS);
        this.sequenceElements = StoreProfile.packed(d, Params.SEQUENCE_ELEMENTS);
        this.sequenceArrays = StoreProfile.packed(d, Params.SEQUENCE_ARRAYS);
        StoreArray classesDS = StoreProfile.array(d, Params.SEQUENCE_ARRAYS);
        this.arraysOnly = classesDS != null && classesDS.hasAttribute(Params.ARRAYS_ONLY)
                && StoreProfile.longAttr(classesDS, Params.ARRAYS_ONLY) == 1;
        this.indexElements = StoreProfile.packed(d, Params.SEQUENCE_INDEX_ELEMENTS);
        this.indexStarts = StoreProfile.packed(d, Params.SEQUENCE_INDEX_STARTS);
        this.indexSequences = StoreProfile.packed(d, Params.SEQUENCE_INDEX_SEQUENCES);
        StoreArray elementsDS = StoreProfile.array(d, Params.SEQUENCE_ELEMENTS);
        this.numSlots = (elementsDS != null && elementsDS.hasAttribute(Params.NUM_SLOTS))
                ? StoreProfile.longAttr(elementsDS, Params.NUM_SLOTS) : 0;

        StoreGroup stringsG = StoreProfile.group(d, "strings");
        this.strings = (stringsG != null) ? new FCDReader(stringsG) : null;
        StoreGroup typedLiteralsDictionaryG = StoreProfile.group(d, "typedLiteralsDictionary");
        this.typedLiteralsDictionary = (typedLiteralsDictionaryG != null) ? new FCDReader(typedLiteralsDictionaryG) : null;
        StoreGroup iriG = StoreProfile.group(d, "iri");
        this.iri = (iriG != null) ? new FCDReader(iriG) : null;
        StoreGroup langsG = StoreProfile.group(d, "langs");
        this.langs = (langsG != null) ? new FCDReader(langsG) : null;
        this.langTags = StoreProfile.packed(d, "langTags");
        this.langDirs = StoreProfile.packed(d, "langDirs");
        this.langTable = (langs == null) ? null : readLanguageTags(langs, getName());

        // Parallel columns must cover every row: a shorter one used to be read
        // past its end inside a query instead of failing the open (BG-81).
        requireRows("datatypes", datatype.getNumEntries());
        if (typedLiterals != null) requireRows("typedLiterals", typedLiterals.getNumEntries());
        if (langTags != null) requireRows("langTags", langTags.getNumEntries());
        if (langDirs != null) requireRows("langDirs", langDirs.getNumEntries());
        checkSequenceSuffix();
        checkSequenceIndex();
        checkTripleTermBand();
    }

    /**
     * The element index, when there is one, must have all three columns and
     * agree with the sequences in its counts: one more start than elements,
     * the first 0 and the last the sequence column's length, and no more
     * (element, sequence) pairs than the store has elements. What each
     * column holds, {@code -verify -deep} checks against the elements.
     */
    private void checkSequenceIndex() {
        if (indexElements == null && indexStarts == null && indexSequences == null) {
            return;
        }
        long elements = (sequenceElements == null) ? 0 : sequenceElements.getNumEntries();
        boolean consistent = indexElements != null && indexStarts != null && indexSequences != null
                && sequenceStarts != null
                && indexStarts.getNumEntries() == indexElements.getNumEntries() + 1
                && indexStarts.get(0) == 0
                && indexStarts.get(indexElements.getNumEntries()) == indexSequences.getNumEntries()
                && indexSequences.getNumEntries() <= elements;
        if (!consistent) {
            throw new IllegalStateException("Corrupt HDF5: dictionary '" + getName() + "': the element index ("
                    + Params.SEQUENCE_INDEX_ELEMENTS + ", " + Params.SEQUENCE_INDEX_STARTS + ", "
                    + Params.SEQUENCE_INDEX_SEQUENCES + ") does not match the sequence stores");
        }
    }

    private void requireRows(String column, long rows) {
        if (rows < numEntries) {
            throw new IllegalStateException("Corrupt HDF5: dictionary '" + getName() + "': " + column + " holds "
                    + rows + " rows but offsets declares " + numEntries + " entries");
        }
    }

    /**
     * The sequence suffix (SPECIFICATIONS.md §7.6b: the section's last
     * {@code sequenceStarts.numEntries - 1} rows) must agree with the
     * datatypes column, and the starts with the elements they delimit: the
     * first 0, the last the element count. As for triple terms (BG-82), every
     * band the reader derives comes from these counts alone.
     */
    private void checkSequenceSuffix() {
        if (sequenceStarts == null) {
            if (sequenceElements != null) {
                throw new IllegalStateException("Corrupt HDF5: dictionary '" + getName()
                        + "': sequenceElements without sequenceStarts (SPECIFICATIONS.md §7.6b)");
            }
            return;
        }
        long n = sequenceStarts.getNumEntries();
        long rows = n - 1;
        long elements = (sequenceElements == null) ? 0 : sequenceElements.getNumEntries();
        int seq = DataType.SEQUENCE.ordinal();
        boolean consistent = rows >= 1 && rows <= numEntries
                && datatype.get(numEntries - rows) == seq
                && (rows == numEntries || datatype.get(numEntries - rows - 1) != seq)
                && sequenceStarts.get(0) == 0
                && sequenceStarts.get(rows) == elements
                && numSlots >= 0 && numSlots <= elements;
        if (!consistent) {
            throw new IllegalStateException("Corrupt HDF5: dictionary '" + getName() + "': the sequence stores ("
                    + n + " starts, " + elements + " elements) do not match the SEQUENCE suffix of the datatypes column");
        }
    }

    /**
     * The triple-term band (SPECIFICATIONS.md §7.4, §7.6b: the
     * {@code tripleTerms.numEntries / 3} rows just before the sequences, or
     * the section's last rows when it holds none) must agree with the
     * datatypes column. The reader derives {@code firstTripleTermObjectId}
     * from that count alone, so a store or writer that disagreed would have
     * mis-clamped every triple-term scan and read component ids for plain
     * literals without any exception (BG-82).
     */
    private void checkTripleTermBand() {
        if (tripleTerms == null) {
            return;
        }
        long n = tripleTerms.getNumEntries();
        long rows = n / 3;
        long end = numEntries - sequenceRowCount();     // rows after the band: the sequences
        int tt = DataType.TRIPLE_TERM.ordinal();
        boolean consistent = n % 3 == 0 && rows >= 1 && rows <= end
                && datatype.get(end - rows) == tt
                && datatype.get(end - 1) == tt
                && (rows == end || datatype.get(end - rows - 1) != tt);
        if (!consistent) {
            throw new IllegalStateException("Corrupt HDF5: dictionary '" + getName() + "': the tripleTerms store ("
                    + n + " entries, " + rows + " terms) does not match the TRIPLE_TERM band of the datatypes column");
        }
    }

    /** The section's language tags, each checked to be a fixed point of Jena's RFC 5646 formatter. */
    static String[] readLanguageTags(FCDReader langs, String section) {
        int n = (int) langs.getNumEntries();
        String[] table = new String[n];
        for (int i = 0; i < n; i++) {
            table[i] = requireFormattedTag(langs.get(i), section);
        }
        return table;
    }

    /**
     * {@code tag} when it is exactly what Jena's formatter produces for it;
     * otherwise the file is corrupt: its ids were assigned over the raw tag
     * while the reader's search compares the re-formatted term, so ranks and
     * comparator order can disagree (SPECIFICATIONS.md §6.2).
     */
    static String requireFormattedTag(String tag, String section) {
        String formatted;
        try {
            formatted = LangTagX.formatLanguageTag(tag);
        } catch (RuntimeException e) {
            formatted = null;
        }
        if (!tag.equals(formatted)) {
            throw new IllegalStateException("Corrupt HDF5: language tag '" + tag + "' in dictionary '" + section
                    + "' is not in RFC 5646 formatted form (SPECIFICATIONS.md §6.2 requires "
                    + (formatted == null ? "a well-formed tag" : "'" + formatted + "'") + ")");
        }
        return tag;
    }

    private TieredIndex tieredIndex() {
        TieredIndex t = tiered;
        if (t == null) {
            synchronized (this) {
                t = tiered;
                if (t == null) {
                    t = buildTieredIndex();
                    tiered = t;
                }
            }
        }
        return t;
    }

    private TieredIndex buildTieredIndex() {
        if (numEntries <= TIER_SPACING) return EMPTY_TIER;
        // Over HTTP the sampled tier is a false economy: extracting every
        // 1024th entry decodes a whole front-coded block each, i.e. touches
        // essentially every range block of the section - a full download on
        // the first lookup - to save a handful of binary-search probes that
        // cost O(log n) blocks. The plain search over [1, numEntries] runs
        // instead; the search and block caches absorb repeats (BG-240).
        if (offsets.isRemote()) return EMPTY_TIER;
        int tierSize = (int) (numEntries / TIER_SPACING);
        long[] ids = new long[tierSize];
        Node[] nodes = new Node[tierSize];

        IdentityHashMap<Node, NodeValue> values = new IdentityHashMap<>();
        for (int i = 0; i < tierSize; i++) {
            long id = (long) i * TIER_SPACING + 1;
            ids[i] = id;
            nodes[i] = extract(id);
            if (nodes[i].isLiteral()) {
                try {
                    values.put(nodes[i], NodeValue.makeNode(nodes[i]));
                } catch (RuntimeException unparseable) {
                    // the comparator's own valueOf fallback handles this term
                }
            }
        }
        return new TieredIndex(ids, nodes, values);
    }

    @Override
    public Node extract(long id) {
        long idx = id - 1;
        if (idx < 0 || idx >= numEntries) throw new IllegalArgumentException("id [" + id + "] must be from 1 to " + getNumberOfNodes());
        long off = offsets.get(idx);
        int typeOrdinal = (int) datatype.get(idx);

        if (typeOrdinal < 0 || typeOrdinal >= DT_VALUES.length) {
             throw new RuntimeException("Corrupt HDF5: Unknown DataType ordinal " + typeOrdinal + " at ID " + id);
        }
        DataType dt = DT_VALUES[typeOrdinal];
        Node na = switch (dt) {
            case INTEGER -> NodeFactory.createLiteralByValue((int) integers.get(off));
            case LONG -> NodeFactory.createLiteralByValue(longs.get(off));
            case FLOAT -> NodeFactory.createLiteralByValue(floats.getFloat(off * Float.BYTES));
            case DOUBLE -> NodeFactory.createLiteralByValue(doubles.getDouble(off * Double.BYTES));
            case STRING -> {
                // A language tag takes precedence: rdf:langString is reconstructed
                // as a lang-tagged literal (term-exact per RDF semantics).
                long langId = (langTags != null) ? langTags.get(idx) : 0;
                if (langId > 0 && langTable != null) {
                    String lex = strings.get(off);
                    if (langId > langTable.length) {
                        throw new IllegalStateException("Corrupt HDF5: language tag id " + langId + " at ID " + id
                                + " in dictionary '" + getName() + "' exceeds the " + langTable.length + " stored tags");
                    }
                    String lang = langTable[(int) langId - 1];
                    // Base direction (format v4): one extra bit-packed read, only
                    // when the store has directional literals at all. A direction
                    // implies a language tag, so this stays inside the lang branch
                    // and the short-circuit past the datatype id is preserved.
                    long dirId = (langDirs != null) ? langDirs.get(idx) : 0;
                    if (dirId > 0) {
                        yield NodeFactory.createLiteralDirLang(lex, lang,
                                dirId == 1 ? TextDirection.LTR : TextDirection.RTL);
                    }
                    yield NodeFactory.createLiteralLang(lex, lang);
                }
                long dtId = typedLiterals.get(idx);
                if (dtId < 1) throw new RuntimeException("Corrupt HDF5: missing typed-literal datatype id at ID " + id);
                yield NodeFactory.createLiteralDT(strings.get(off), tm.getSafeTypeByName(typedLiteralsDictionary.get(dtId - 1)));
            }
            // IRI and RELATIVE_IRI share the `iri` buffer and reconstruct
            // identically - the stored string is returned verbatim as a
            // Node_URI (a relative reference for RELATIVE_IRI). The distinct
            // type is kept as an honest record of the source form; resolving a
            // relative IRI to an absolute one happens at the serving boundary
            // (core.RelativeIRIResolver), not here.
            case IRI, RELATIVE_IRI -> NodeFactory.createURI(iri.get(off));
            case BNODE -> NodeFactory.createBlankNode(Params.blankNodeLabel(id));
            case TRIPLE_TERM -> {
                TripleTermResolver r = tripleTermResolver;
                if (r == null || tripleTerms == null) {
                    throw new IllegalStateException("Triple-term row at ID " + id + " in dictionary '"
                            + getName() + "' but no component store/resolver is wired");
                }
                long k = off * 3;
                yield r.resolve(tripleTerms.get(k), tripleTerms.get(k + 1), tripleTerms.get(k + 2));
            }
            case SEQUENCE -> sequence(id, off, true);
            default -> throw new IllegalStateException("Unsupported DataType: " + dt);
        };
        return na;
    }

    /**
     * Whether any typed literal in this section carries a composite (cdt:List /
     * cdt:Map) datatype. Reads only the small per-store datatype-IRI table, not
     * the literals themselves, so it is cheap enough for an open-time check.
     */
    public boolean hasCompositeDatatype() {
        if (typedLiteralsDictionary == null) {
            return false;
        }
        for (long i = 0, n = typedLiteralsDictionary.getNumEntries(); i < n; i++) {
            if (tm.getSafeTypeByName(typedLiteralsDictionary.get(i)) instanceof CompositeDatatypeBase) {
                return true;
            }
        }
        return false;
    }

    /** The floats dataset exists only when the section stores an xsd:float literal. */
    @Override
    public boolean hasFloatLiterals() {
        return floats != null;
    }

    @Override
    public boolean hasDoubleLiterals() {
        return doubles != null;
    }

    public void setTripleTermResolver(TripleTermResolver resolver) {
        this.tripleTermResolver = resolver;
    }

    public void setSequenceSupport(SequenceSupport support) {
        this.sequenceSupport = support;
    }

    /** Number of sequence rows in this section (its contiguous suffix, format v7). */
    /**
     * The sequence row at section id {@code id} (sequence ordinal {@code k}) as
     * a lazy list (SPECIFICATIONS.md §7.7): its length from the starts, each
     * element read when asked for - from {@code /.BG/arrays} when
     * {@code packed} and a tensor or ragged class holds it (RDFARRY.md §3.6),
     * else from its canonical elements.
     */
    private Node sequence(long id, long k, boolean packed) {
        SequenceSupport support = sequenceSupport;
        if (support == null || sequenceStarts == null) {
            throw new IllegalStateException("Sequence row at ID " + id + " in dictionary '"
                    + getName() + "' but no sequence store/resolver is wired");
        }
        PackedSequences p = packedSequences;
        if (arraysOnly) {
            if (p == null) {
                throw new IllegalStateException("Dictionary '" + getName() + "' stores sequences only in /.BG/arrays"
                        + " (format v8), but they are not wired to be read");
            }
            List<Node> held = p.heldOnly(k);
            if (held != null) {
                return com.ebremer.beakgraph.core.lib.SeqTerms.view(held);
            }
        }
        long start = sequenceStarts.get(k);
        long length = sequenceStarts.get(k + 1) - start;
        if (length < 0 || length > Integer.MAX_VALUE) {
            throw new IllegalStateException("Corrupt HDF5: sequence at ID " + id + " in dictionary '"
                    + getName() + "' has length " + length);
        }
        List<Node> elements = (packed && p != null) ? p.row(k, (int) length) : null;
        return com.ebremer.beakgraph.core.lib.SeqTerms.view(
                (elements != null) ? elements : new SequenceRow(start, (int) length, support));
    }

    /**
     * The sequence row at section id {@code id} from its canonical elements,
     * never from {@code /.BG/arrays}: what {@code -verify -deep} checks the
     * arrays against.
     */
    public Node extractCanonicalSequence(long id) {
        long idx = id - 1;
        if (idx < 0 || idx >= numEntries) {
            throw new IllegalArgumentException("id [" + id + "] must be from 1 to " + getNumberOfNodes());
        }
        int typeOrdinal = (int) datatype.get(idx);
        if (typeOrdinal < 0 || typeOrdinal >= DT_VALUES.length || DT_VALUES[typeOrdinal] != DataType.SEQUENCE) {
            throw new IllegalArgumentException("id [" + id + "] is no sequence row");
        }
        if (heldOnlyInArrays(id)) {
            throw new IllegalArgumentException("sequence [" + id + "] is stored only in /.BG/arrays (format v8)");
        }
        return sequence(id, offsets.get(idx), false);
    }

    private volatile PackedSequences packedSequences;

    /**
     * Reads every sequence from its canonical elements from now on: those that have them. In an
     * {@code -arrays-only} store the arrays are the only copy of their sequences, so nothing changes.
     */
    void disablePackedReads() {
        if (!arraysOnly) {
            packedSequences = null;
        }
    }

    /** True when this section stores the sequences of its tensor and ragged classes only in {@code /.BG/arrays} (format v8). */
    public boolean arraysOnly() {
        return arraysOnly;
    }

    /**
     * The sequence row at section id {@code id} when it is held only in {@code /.BG/arrays} (format v8): its
     * elements, read from its class; else null.
     */
    public List<Node> heldSequence(long id) {
        if (!arraysOnly) {
            return null;
        }
        PackedSequences p = packedSequences;
        return (p == null) ? null : p.heldOnly(offsets.get(id - 1));
    }

    /** Whether the class holding the sequence row at section id {@code id} shares values with the dictionary (format v8). */
    public boolean heldSharesDictionary(long id) {
        PackedSequences p = packedSequences;
        return p == null || p.sharesDictionary(offsets.get(id - 1));
    }

    /**
     * The section ids {@code [first, last]} of the stored sequences that begin with the terms {@code prefix}
     * ({@link com.ebremer.beakgraph.core.lib.SeqTerms#UNDEF} for a slot), or null when none does: the suffix
     * searched by terms, in the order it was sorted by, as a store whose sequences may be held only in its
     * arrays must be (format v8). They are contiguous for the reason {@link #sequencePrefixRange} gives.
     */
    public long[] sequencePrefixRangeByTerms(List<Node> prefix) {
        long rows = sequenceRowCount();
        if (rows == 0) {
            return null;
        }
        long low = numEntries - rows + 1;
        long high = numEntries;
        while (low < high) {
            long mid = low + (high - low) / 2;
            if (comparePrefixByTerms(mid, prefix) < 0) low = mid + 1;
            else high = mid;
        }
        if (comparePrefixByTerms(low, prefix) != 0) {
            return null;
        }
        long first = low;
        high = numEntries;
        while (low < high) {
            long mid = low + (high - low + 1) / 2;
            if (comparePrefixByTerms(mid, prefix) > 0) high = mid - 1;
            else low = mid;
        }
        return new long[]{first, low};
    }

    /** The stored sequence at section id {@code id} against {@code prefix}: 0 when it begins with it, else their order. */
    private int comparePrefixByTerms(long id, List<Node> prefix) {
        List<Node> row = extract(id).getSeq();
        int m = Math.min(row.size(), prefix.size());
        int c = NodeComparator.INSTANCE.compare(com.ebremer.beakgraph.core.lib.SeqTerms.view(row.subList(0, m)),
                com.ebremer.beakgraph.core.lib.SeqTerms.view(prefix.subList(0, m)));
        if (c != 0) {
            return c;
        }
        return (row.size() < prefix.size()) ? -1 : 0;
    }

    /** True when the sequence row at section id {@code id} is held only in {@code /.BG/arrays} (format v8). */
    public boolean heldOnlyInArrays(long id) {
        if (!arraysOnly) {
            return false;
        }
        PackedSequences p = packedSequences;
        return p != null && p.heldOnly(offsets.get(id - 1)) != null;
    }

    /**
     * Reads the sequences that {@code /.BG/arrays} packs from their arrays
     * from now on, when this section has {@code sequenceArrays}; the group is
     * fetched on the first such read.
     */
    void enablePackedReads(java.util.function.Supplier<com.ebremer.beakgraph.store.StoreGroup> arrays,
                           long firstSequenceObjectId) {
        if (sequenceArrays != null && sequenceStarts != null) {
            packedSequences = new PackedSequences(arrays, sequenceArrays, firstSequenceObjectId);
        }
    }

    public long sequenceRowCount() {
        return (sequenceStarts == null) ? 0 : sequenceStarts.getNumEntries() - 1;
    }

    /** Each sequence row's {@code /.BG/arrays} class, 0 when unpacked; null without {@code -arrays}. */
    public long[] sequenceArrayClasses() {
        if (sequenceArrays == null) {
            return null;
        }
        long[] out = new long[Math.toIntExact(sequenceArrays.getNumEntries())];
        for (int i = 0; i < out.length; i++) {
            out[i] = sequenceArrays.get(i);
        }
        return out;
    }

    /** How many sequence elements are empty slots (the {@code numSlots} attribute; 0 without sequences). */
    public long numSlots() {
        return numSlots;
    }

    /** True when the section has the element index ({@code -seqindex}). */
    public boolean hasSequenceIndex() {
        return indexElements != null;
    }

    /**
     * Where the sequences holding element {@code elementId} (object space, 0
     * for a slot) lie in the element index's sequence column:
     * {@code {from, to}}, {@code to} exclusive, or null when none holds it.
     * The caller guarantees {@link #hasSequenceIndex}.
     */
    public long[] sequenceIndexRange(long elementId) {
        long n = indexElements.getNumEntries();
        long k = (n == 0) ? -1 : indexElements.binarySearch(0, n - 1, elementId);
        return (k < 0) ? null : new long[] {indexStarts.get(k), indexStarts.get(k + 1)};
    }

    /** The sequence ordinal at position {@code q} of the element index's sequence column. */
    public long sequenceIndexOrdinal(long q) {
        return indexSequences.get(q);
    }

    /**
     * The first position in {@code [from, to)} of the element index's
     * sequence column whose ordinal is at least {@code ordinal}, or
     * {@code to} when there is none: the positions of one element hold its
     * sequences in ascending order.
     */
    public long sequenceIndexSeek(long from, long to, long ordinal) {
        if (from >= to) {
            return to;
        }
        long q = indexSequences.lowerBound(from, to - 1, ordinal);
        return (q < 0) ? to : q;
    }

    /** {@code {distinct elements, (element, sequence) pairs}} of the element index; the caller guarantees one. */
    public long[] sequenceIndexSize() {
        return new long[] {indexElements.getNumEntries(), indexSequences.getNumEntries()};
    }

    /** Element {@code k} of the element index's element column, ascending. */
    public long sequenceIndexElement(long k) {
        return indexElements.get(k);
    }

    /** Number of sequence elements, slots included, over every sequence of the section. */
    public long sequenceElementCount() {
        return (sequenceElements == null) ? 0 : sequenceElements.getNumEntries();
    }

    /**
     * Where the elements of the sequence at section id {@code id} begin in
     * the element store, and where they end (exclusive). The caller
     * guarantees the id lies in the sequence suffix.
     */
    public long[] sequenceBounds(long id) {
        long k = offsets.get(id - 1);
        return new long[]{sequenceStarts.get(k), sequenceStarts.get(k + 1)};
    }

    /** Element {@code index} of the element store: an object-space id, 0 for a slot. */
    public long sequenceElementId(long index) {
        return sequenceElements.get(index);
    }

    /**
     * A stored sequence's elements, read when asked for: a nonzero id is the
     * object-space term it names (a nested sequence comes back lazy too), 0
     * the empty slot. Immutable, like the store.
     *
     * <p>Each element read is kept. That is what makes a row safe in Jena's
     * JVM-wide caches (the NodeValue cache above all), which outlive the
     * store: a cache hashes a key before it keeps it, which reads every
     * element, so a row a cache holds never reads its store again, and a
     * later, equal sequence from another store compares against the kept
     * elements. A row a caller keeps unread after its store is closed fails
     * when read (RDFARRY.md §6).
     */
    private final class SequenceRow extends AbstractList<Node> implements RandomAccess {
        private final long start;
        private final int size;
        private final SequenceSupport support;
        // Benign races: two threads that both find an element missing store
        // the same immutable term; a reference write is atomic.
        private Node[] read;
        private int hash;
        private boolean hashed;

        SequenceRow(long start, int size, SequenceSupport support) {
            this.start = start;
            this.size = size;
            this.support = support;
        }

        @Override
        public Node get(int index) {
            Objects.checkIndex(index, size);
            Node[] kept = read;
            if (kept == null) {
                kept = new Node[size];
                read = kept;
            }
            Node n = kept[index];
            if (n == null) {
                long id = sequenceElements.get(start + index);
                n = (id == 0) ? com.ebremer.beakgraph.core.lib.SeqTerms.UNDEF : support.extract(id);
                kept[index] = n;
            }
            return n;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public int hashCode() {
            if (!hashed) {
                hash = super.hashCode();    // reads, and so keeps, every element
                hashed = true;
            }
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            return o == this || super.equals(o);
        }
    }

    /** Number of triple-term rows in this section (they form a contiguous suffix of the id space). */
    public long tripleTermRowCount() {
        return (tripleTerms == null) ? 0 : tripleTerms.getNumEntries() / 3;
    }

    /**
     * Component ids (s, p, o) of the triple term at section id {@code id}. The
     * caller guarantees the id lies in the triple-term suffix (see
     * {@link #tripleTermRowCount()}); no datatype re-check is performed here.
     */
    public long[] tripleTermComponents(long id) {
        long k = offsets.get(id - 1) * 3;
        return new long[]{tripleTerms.get(k), tripleTerms.get(k + 1), tripleTerms.get(k + 2)};
    }

    @Override
    public long search(Node element) {
        // Build the tier (a one-off pass over the section) BEFORE entering the
        // cache's mapping function, which runs under a ConcurrentHashMap bin
        // lock: every concurrent search on the section would otherwise queue
        // behind the build.
        tieredIndex();
        return searchCache.get(element, this::searchFAST);
    }

    /**
     * Tiered binary search: narrows the id range to ~1024 via the tiered index, then
     * binary-searches with extract() + NodeComparator for a correct total ordering.
     */
    private long searchFAST(Node element) {
        long low = 1;
        long high = numEntries;

        // A literal target's NodeValue is needed at every literal-vs-literal probe;
        // memoize it (by identity - `element` is the same object throughout this
        // search) instead of re-deriving it ~log(n) times. The comparator's
        // ordering semantics are untouched: nodeValue() is the designated hook.
        if (element.isNodeSeq()) {
            long onIds = searchSequenceOnIds(element);
            if (onIds != SEARCH_BY_TERMS) {
                return onIds;
            }
        }
        TieredIndex tier = tieredIndex();
        ProbeComparator cmp = !element.isLiteral() ? null : new ProbeComparator(element, tier.values());
        NodeComparator order = (cmp != null) ? cmp : NodeComparator.INSTANCE;

        // 1. Tiered Index Lookup to narrow the range
        // This is safe because the tier nodes are actual Node objects compared using your specific Comparator
        if (tier.nodes().length > 0) {
            int tierIdx = Arrays.binarySearch(tier.nodes(), element, order);
            if (tierIdx >= 0) return tier.ids()[tierIdx];

            int insertionPoint = -(tierIdx + 1);
            if (insertionPoint > 0) {
                // The element at ids[insertionPoint-1] already compared < element,
                // so the real match (if any) starts strictly after it.
                low = tier.ids()[insertionPoint - 1] + 1;
            }
            if (insertionPoint < tier.ids().length) {
                high = tier.ids()[insertionPoint] - 1;
            }
        }

        // 2. Binary search within the narrowed range, comparing via extract() + NodeComparator.
        while (low <= high) {
            long midId = low + (high - low) / 2;
            Node midNode = extract(midId);
            if (midNode == null) throw new IllegalStateException("Dictionary corruption at ID: " + midId);
            if (cmp != null) {
                cmp.probe(midNode, packedValue(midId - 1));
            }

            int c = order.compare(midNode, element);

            if (c == 0) return midId;
            else if (c < 0) low = midId + 1;
            else high = midId - 1;
        }
        return -low - 1;
    }

    /** {@link #searchSequenceOnIds}' answer when the search must compare terms instead. */
    private static final long SEARCH_BY_TERMS = Long.MIN_VALUE;

    /**
     * Searches the sequence suffix on ids (SPECIFICATIONS.md §7.6b): the
     * probe's elements are located first, then the suffix is binary-searched
     * comparing element-id arrays, which order exactly as the sequences do
     * (the object space is sorted and a slot is 0), so no stored row is
     * materialized. The same answer as the search on terms: an id, or a
     * negative insertion point. {@link #SEARCH_BY_TERMS} when an element is
     * not in the store (the sequence is not either, and the term search
     * finds where it would be). A probe that is not a concrete sequence is
     * a miss.
     */
    private long searchSequenceOnIds(Node seq) {
        long rows = sequenceRowCount();
        if (rows == 0) {
            return -(numEntries + 1) - 1;   // every sequence ranks after every row here
        }
        if (!seq.isConcrete()) {
            // A pattern (a variable or gap inside) is no term and is in no
            // store; the comparator cannot rank it. A miss, not an exception,
            // until sequence patterns are delegated (RDFARRY.md step 4).
            return -(numEntries + 1) - 1;
        }
        SequenceSupport support = sequenceSupport;
        if (support == null || arraysOnly) {
            // -arrays-only: a sequence held only in /.BG/arrays has no element ids, and its leaves may have no
            // ids at all, so the suffix is searched on terms, in the order the writer sorted it by.
            return SEARCH_BY_TERMS;
        }
        List<Node> elements = seq.getSeq();
        long[] probe = new long[elements.size()];
        for (int i = 0; i < probe.length; i++) {
            Node e = elements.get(i);
            if (e.isNodeUndef()) {
                continue;   // 0
            }
            long id = support.locate(e);
            if (id < 1) {
                return SEARCH_BY_TERMS;
            }
            probe[i] = id;
        }
        return searchSequenceIds(probe);
    }

    /**
     * The section id of the stored sequence whose elements are exactly
     * {@code probe} (object-space ids, 0 for a slot), or a negative insertion
     * point: a binary search of the suffix on id arrays (SPECIFICATIONS.md
     * §7.6b), materializing nothing.
     */
    public long searchSequenceIds(long[] probe) {
        long rows = sequenceRowCount();
        long low = numEntries - rows + 1;
        long high = numEntries;
        while (low <= high) {
            long mid = low + (high - low) / 2;
            int c = compareStoredSequence(mid, probe);
            if (c == 0) return mid;
            if (c < 0) low = mid + 1;
            else high = mid - 1;
        }
        return -low - 1;
    }

    /**
     * The section ids {@code [first, last]} of the stored sequences that begin
     * with the elements {@code prefix} (object-space ids, 0 for a slot), or
     * null when none does. They are contiguous: the suffix is sorted on id
     * arrays, and a sequence shorter than the prefix sorts before every one
     * that holds it (RDFARRY.md §3.3, "a constant prefix is a range").
     */
    public long[] sequencePrefixRange(long[] prefix) {
        long rows = sequenceRowCount();
        if (rows == 0) {
            return null;
        }
        long base = numEntries - rows + 1;
        long low = base;
        long high = numEntries;
        while (low < high) {        // the first row not below the prefix
            long mid = low + (high - low) / 2;
            if (comparePrefix(mid, prefix) < 0) low = mid + 1;
            else high = mid;
        }
        if (comparePrefix(low, prefix) != 0) {
            return null;
        }
        long first = low;
        high = numEntries;
        while (low < high) {        // the last row holding the prefix
            long mid = low + (high - low + 1) / 2;
            if (comparePrefix(mid, prefix) > 0) high = mid - 1;
            else low = mid;
        }
        return new long[]{first, low};
    }

    /** The stored sequence at section id {@code id} against {@code prefix}: 0 when it begins with it, else their order. */
    private int comparePrefix(long id, long[] prefix) {
        long k = offsets.get(id - 1);
        long start = sequenceStarts.get(k);
        long length = sequenceStarts.get(k + 1) - start;
        long n = Math.min(length, prefix.length);
        for (long i = 0; i < n; i++) {
            long e = sequenceElements.get(start + i);
            if (e != prefix[(int) i]) {
                return Long.compare(e, prefix[(int) i]);
            }
        }
        return (length < prefix.length) ? -1 : 0;
    }

    /** The stored sequence at section id {@code id} against element ids {@code probe}: lexicographic, a prefix first. */
    private int compareStoredSequence(long id, long[] probe) {
        long k = offsets.get(id - 1);
        long start = sequenceStarts.get(k);
        long length = sequenceStarts.get(k + 1) - start;
        long n = Math.min(length, probe.length);
        for (long i = 0; i < n; i++) {
            long e = sequenceElements.get(start + i);
            if (e != probe[(int) i]) {
                return Long.compare(e, probe[(int) i]);
            }
        }
        return Long.compare(length, probe.length);
    }

    /**
     * The NodeValue of row {@code idx} straight from its packed value when the
     * row is a natively stored number (INTEGER/LONG/FLOAT/DOUBLE), else null.
     * Numerically identical to {@code NodeValue.makeNode(extract(id))} - the
     * comparator orders numbers by exact value and breaks ties on the term,
     * which it still takes from the Node - but without the lexical-form hash
     * and global-cache round trip a fresh literal pays per probe (BG-244).
     */
    private NodeValue packedValue(long idx) {
        int typeOrdinal = (int) datatype.get(idx);
        if (typeOrdinal < 0 || typeOrdinal >= DT_VALUES.length) {
            return null;
        }
        return switch (DT_VALUES[typeOrdinal]) {
            case INTEGER -> NodeValue.makeInteger((int) integers.get(offsets.get(idx)));
            case LONG -> NodeValue.makeInteger(longs.get(offsets.get(idx)));
            case FLOAT -> NodeValue.makeFloat(floats.getFloat(offsets.get(idx) * Float.BYTES));
            case DOUBLE -> NodeValue.makeDouble(doubles.getDouble(offsets.get(idx) * Double.BYTES));
            default -> null;
        };
    }

    /**
     * NodeComparator for one literal search: the target's value is derived
     * once (it takes part in every probe), the tier terms' values come from
     * the memo built with the tier, and the current probe row's value, when
     * the row is a native number, is the one built from its packed value.
     * Ordering semantics are untouched - nodeValue() is the designated hook.
     */
    private static final class ProbeComparator extends NodeComparator {
        private final Node target;
        private final IdentityHashMap<Node, NodeValue> tierValues;
        private NodeValue targetValue;
        private Node probeNode;
        private NodeValue probeValue;

        ProbeComparator(Node target, IdentityHashMap<Node, NodeValue> tierValues) {
            this.target = target;
            this.tierValues = tierValues;
        }

        void probe(Node node, NodeValue value) {
            this.probeNode = node;
            this.probeValue = value;
        }

        @Override
        protected NodeValue nodeValue(Node n) {
            if (n == target) {
                if (targetValue == null) {
                    targetValue = super.nodeValue(n);
                }
                return targetValue;
            }
            if (n == probeNode && probeValue != null) {
                return probeValue;
            }
            NodeValue tier = tierValues.get(n);
            return (tier != null) ? tier : super.nodeValue(n);
        }
    }

    @Override
    public long locate(Node element) {
        long result = search(element);
        return (result >= 0) ? result : -1;
    }

    @Override
    public Stream<Node> streamNodes() {
        return LongStream.rangeClosed(1, numEntries).mapToObj(this::extract);
    }

    @Override
    public long getNumberOfNodes() {
        return numEntries;
    }
    
}