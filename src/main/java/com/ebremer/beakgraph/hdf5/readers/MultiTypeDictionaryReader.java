package com.ebremer.beakgraph.hdf5.readers;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.AbstractDictionary;
import com.ebremer.beakgraph.core.lib.DataType;
import com.ebremer.beakgraph.core.lib.NodeComparator;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.io.DatasetBytes;
import com.ebremer.beakgraph.io.RandomAccessBytes;
import io.jhdf.api.Group;
import io.jhdf.api.dataset.ContiguousDataset;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.jena.cdt.CompositeDatatypeBase;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.TextDirection;
import org.apache.jena.graph.NodeFactory;
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
    private final long numEntries;
    private final String name;

    /** Cross-dictionary materialization of a triple term's component ids. */
    public interface TripleTermResolver {
        Node resolve(long subjectId, long predicateId, long objectId);
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
                    .weigher((Node n, Long pos) ->
                            n.isLiteral() ? 1 + (n.getLiteralLexicalForm().length() >>> 8) : 1)
                    .build();

    private record TieredIndex(long[] ids, Node[] nodes) {}
    private static final TieredIndex EMPTY_TIER = new TieredIndex(new long[0], new Node[0]);

    public MultiTypeDictionaryReader(Group d) {
        this.name = d.getName();
        ContiguousDataset offsetsDS = (ContiguousDataset) d.getDatasetByPath("offsets");
        this.numEntries = (Long) offsetsDS.getAttribute("numEntries").getData();
        this.offsets = BitPackedUnSignedLongBuffer.readView(DatasetBytes.of(offsetsDS), numEntries, (Integer) offsetsDS.getAttribute("width").getData());

        ContiguousDataset datatypeDS = (ContiguousDataset) d.getDatasetByPath("datatypes");
        this.datatype = BitPackedUnSignedLongBuffer.readView(DatasetBytes.of(datatypeDS), (Long) datatypeDS.getAttribute("numEntries").getData(), (Integer) datatypeDS.getAttribute("width").getData());

        ContiguousDataset typedLiteralsDS = (ContiguousDataset) d.getChild("typedLiterals");
        this.typedLiterals = (typedLiteralsDS != null) ? BitPackedUnSignedLongBuffer.readView(DatasetBytes.of(typedLiteralsDS), (Long) typedLiteralsDS.getAttribute("numEntries").getData(), (Integer) typedLiteralsDS.getAttribute("width").getData()) : null;

        this.doubles = getDataSet(d, "doubles").map(DatasetBytes::of).orElse(null);
        this.floats = getDataSet(d, "floats").map(DatasetBytes::of).orElse(null);

        this.integers = getDataSet(d, "integers").map(ds ->
            BitPackedUnSignedLongBuffer.readView(DatasetBytes.of(ds), (Long) ds.getAttribute("numEntries").getData(), (Integer) ds.getAttribute("width").getData())).orElse(null);

        this.longs = getDataSet(d, "longs").map(ds ->
            BitPackedUnSignedLongBuffer.readView(DatasetBytes.of(ds), (Long) ds.getAttribute("numEntries").getData(), (Integer) ds.getAttribute("width").getData())).orElse(null);

        this.tripleTerms = getDataSet(d, "tripleTerms").map(ds ->
            BitPackedUnSignedLongBuffer.readView(DatasetBytes.of(ds), (Long) ds.getAttribute("numEntries").getData(), (Integer) ds.getAttribute("width").getData())).orElse(null);

        Group stringsG = (Group) d.getChild("strings");
        this.strings = (stringsG != null) ? new FCDReader(stringsG) : null;

        Group typedLiteralsDictionaryG = (Group) d.getChild("typedLiteralsDictionary");
        this.typedLiteralsDictionary = (typedLiteralsDictionaryG != null) ? new FCDReader(typedLiteralsDictionaryG) : null;

        Group iriG = (Group) d.getChild("iri");
        this.iri = (iriG != null) ? new FCDReader(iriG) : null;

        Group langsG = (Group) d.getChild("langs");
        this.langs = (langsG != null) ? new FCDReader(langsG) : null;
        ContiguousDataset langTagsDS = (ContiguousDataset) d.getChild("langTags");
        this.langTags = (langTagsDS != null) ? BitPackedUnSignedLongBuffer.readView(DatasetBytes.of(langTagsDS), (Long) langTagsDS.getAttribute("numEntries").getData(), (Integer) langTagsDS.getAttribute("width").getData()) : null;
        ContiguousDataset langDirsDS = (ContiguousDataset) d.getChild("langDirs");
        this.langDirs = (langDirsDS != null) ? BitPackedUnSignedLongBuffer.readView(DatasetBytes.of(langDirsDS), (Long) langDirsDS.getAttribute("numEntries").getData(), (Integer) langDirsDS.getAttribute("width").getData()) : null;
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

        for (int i = 0; i < tierSize; i++) {
            long id = (long) i * TIER_SPACING + 1;
            ids[i] = id;
            nodes[i] = extract(id);
        }
        return new TieredIndex(ids, nodes);
    }

    private Optional<ContiguousDataset> getDataSet(Group g, String name) {
        return (g.getChild(name) != null) ? Optional.of((ContiguousDataset) g.getChild(name)) : Optional.empty();
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
                if (langId > 0 && langs != null) {
                    String lex = strings.get(off);
                    String lang = langs.get(langId - 1);
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
            // (RelativeIRIResolver), not here.
            case IRI, RELATIVE_IRI -> NodeFactory.createURI(iri.get(off));
            case BNODE -> NodeFactory.createBlankNode(Params.blankNodeLabel(id));
            case TRIPLE_TERM -> {
                TripleTermResolver r = tripleTermResolver;
                if (r == null || tripleTerms == null) {
                    throw new IllegalStateException("Triple-term row at ID " + id + " in dictionary '"
                            + name + "' but no component store/resolver is wired");
                }
                long k = off * 3;
                yield r.resolve(tripleTerms.get(k), tripleTerms.get(k + 1), tripleTerms.get(k + 2));
            }
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
        NodeComparator cmp = !element.isLiteral() ? NodeComparator.INSTANCE : new NodeComparator() {
            private NodeValue targetValue;
            @Override
            protected NodeValue nodeValue(Node n) {
                if (n != element) {
                    return super.nodeValue(n);
                }
                if (targetValue == null) {
                    targetValue = super.nodeValue(n);
                }
                return targetValue;
            }
        };

        // 1. Tiered Index Lookup to narrow the range
        // This is safe because the tier nodes are actual Node objects compared using your specific Comparator
        TieredIndex tier = tieredIndex();
        if (tier.nodes().length > 0) {
            int tierIdx = Arrays.binarySearch(tier.nodes(), element, cmp);
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

            int c = cmp.compare(midNode, element);

            if (c == 0) return midId;
            else if (c < 0) low = midId + 1;
            else high = midId - 1;
        }
        return -low - 1;
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