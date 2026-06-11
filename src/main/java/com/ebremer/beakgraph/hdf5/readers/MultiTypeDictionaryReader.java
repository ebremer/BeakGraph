package com.ebremer.beakgraph.hdf5.readers;

import com.ebremer.beakgraph.core.AbstractDictionary;
import com.ebremer.beakgraph.core.lib.DataType;
import com.ebremer.beakgraph.core.lib.NodeComparator;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import io.jhdf.api.Group;
import io.jhdf.api.dataset.ContiguousDataset;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;

public class MultiTypeDictionaryReader extends AbstractDictionary {
    private static final DataType[] DT_VALUES = DataType.values();
    private static final TypeMapper tm = TypeMapper.getInstance();
    private static final int TIER_SPACING = 1024;
    private final BitPackedUnSignedLongBuffer offsets;
    private final BitPackedUnSignedLongBuffer integers;
    private final BitPackedUnSignedLongBuffer longs;
    private final BitPackedUnSignedLongBuffer datatype;
    private final BitPackedUnSignedLongBuffer typedLiterals;
    private final ByteBuffer floats;
    private final ByteBuffer doubles;
    private final FCDReader iri;
    private final FCDReader strings;
    private final FCDReader typedLiteralsDictionary;
    // rdf:langString support: dictionary of distinct language tags + per-node
    // 1-based id buffer (0 = no tag). Both null for files written before
    // language-tag support, so those reconstruct exactly as before.
    private final FCDReader langs;
    private final BitPackedUnSignedLongBuffer langTags;
    private final long numEntries;
    private final String name;

    // Tiered index, built LAZILY on first search: building it in the
    // constructor performed numEntries/TIER_SPACING full extracts at every
    // file-open, paying for a search accelerator the caller might never use.
    // Volatile publication keeps concurrent first-searchers safe; the benign
    // race builds identical content over immutable data.
    private volatile TieredIndex tiered;

    private record TieredIndex(long[] ids, Node[] nodes) {}
    private static final TieredIndex EMPTY_TIER = new TieredIndex(new long[0], new Node[0]);

    public MultiTypeDictionaryReader(Group d) {
        this.name = d.getName();
        ContiguousDataset offsetsDS = (ContiguousDataset) d.getDatasetByPath("offsets");
        this.numEntries = (Long) offsetsDS.getAttribute("numEntries").getData();
        this.offsets = new BitPackedUnSignedLongBuffer(null, offsetsDS.getBuffer(), numEntries, (Integer) offsetsDS.getAttribute("width").getData());

        ContiguousDataset datatypeDS = (ContiguousDataset) d.getDatasetByPath("datatypes");
        this.datatype = new BitPackedUnSignedLongBuffer(null, datatypeDS.getBuffer(), (Long) datatypeDS.getAttribute("numEntries").getData(), (Integer) datatypeDS.getAttribute("width").getData());

        ContiguousDataset typedLiteralsDS = (ContiguousDataset) d.getChild("typedLiterals");
        this.typedLiterals = (typedLiteralsDS != null) ? new BitPackedUnSignedLongBuffer(null, typedLiteralsDS.getBuffer(), (Long) typedLiteralsDS.getAttribute("numEntries").getData(), (Integer) typedLiteralsDS.getAttribute("width").getData()) : null;

        this.doubles = getDataSet(d, "doubles").map(ds -> ds.getBuffer().order(ByteOrder.BIG_ENDIAN)).orElse(null);
        this.floats = getDataSet(d, "floats").map(ds -> ds.getBuffer().order(ByteOrder.BIG_ENDIAN)).orElse(null);

        this.integers = getDataSet(d, "integers").map(ds ->
            new BitPackedUnSignedLongBuffer(null, ds.getBuffer(), (Long) ds.getAttribute("numEntries").getData(), (Integer) ds.getAttribute("width").getData())).orElse(null);

        this.longs = getDataSet(d, "longs").map(ds ->
            new BitPackedUnSignedLongBuffer(null, ds.getBuffer(), (Long) ds.getAttribute("numEntries").getData(), (Integer) ds.getAttribute("width").getData())).orElse(null);

        Group stringsG = (Group) d.getChild("strings");
        this.strings = (stringsG != null) ? new FCDReader(stringsG) : null;

        Group typedLiteralsDictionaryG = (Group) d.getChild("typedLiteralsDictionary");
        this.typedLiteralsDictionary = (typedLiteralsDictionaryG != null) ? new FCDReader(typedLiteralsDictionaryG) : null;

        Group iriG = (Group) d.getChild("iri");
        this.iri = (iriG != null) ? new FCDReader(iriG) : null;

        Group langsG = (Group) d.getChild("langs");
        this.langs = (langsG != null) ? new FCDReader(langsG) : null;
        ContiguousDataset langTagsDS = (ContiguousDataset) d.getChild("langTags");
        this.langTags = (langTagsDS != null) ? new BitPackedUnSignedLongBuffer(null, langTagsDS.getBuffer(), (Long) langTagsDS.getAttribute("numEntries").getData(), (Integer) langTagsDS.getAttribute("width").getData()) : null;
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
            case FLOAT -> NodeFactory.createLiteralByValue(floats.getFloat(Math.toIntExact(off * Float.BYTES)));
            case DOUBLE -> NodeFactory.createLiteralByValue(doubles.getDouble(Math.toIntExact(off * Double.BYTES)));
            case STRING -> {
                // A language tag takes precedence: rdf:langString is reconstructed
                // as a lang-tagged literal (term-exact per RDF semantics).
                long langId = (langTags != null) ? langTags.get(idx) : 0;
                if (langId > 0 && langs != null) {
                    yield NodeFactory.createLiteralLang(strings.get(off), langs.get(langId - 1));
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
            case BNODE -> NodeFactory.createBlankNode(String.format("b%020d", id));
            default -> throw new IllegalStateException("Unsupported DataType: " + dt);
        };
        return na;
    }

    @Override
    public long search(Node element) {
        return this.searchFAST(element);
    }
    
    /**
     * Tiered binary search: narrows the id range to ~1024 via the tiered index, then
     * binary-searches with extract() + NodeComparator for a correct total ordering.
     */
    private long searchFAST(Node element) {
        long low = 1;
        long high = numEntries;

        // 1. Tiered Index Lookup to narrow the range
        // This is safe because the tier nodes are actual Node objects compared using your specific Comparator
        TieredIndex tier = tieredIndex();
        if (tier.nodes().length > 0) {
            int tierIdx = Arrays.binarySearch(tier.nodes(), element, NodeComparator.INSTANCE);
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
            
            int cmp = NodeComparator.INSTANCE.compare(midNode, element);

            if (cmp == 0) return midId;
            else if (cmp < 0) low = midId + 1;
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