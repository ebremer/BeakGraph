package com.ebremer.beakgraph.hdf5.readers;

import com.ebremer.beakgraph.core.AbstractDictionary;
import com.ebremer.beakgraph.core.lib.DataType;
import com.ebremer.beakgraph.core.lib.NodeComparator;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import io.jhdf.api.Group;
import io.jhdf.api.dataset.ContiguousDataset;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.Quad;

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
    private final long numEntries;
    private final String name;
    private long offset = 0;

    // Tiered Index Storage
    private long[] tieredIds;
    private Node[] tieredNodes;

    public MultiTypeDictionaryReader(Group d) {
        this.name = d.getName();
        ContiguousDataset offsetsDS = (ContiguousDataset) d.getDatasetByPath("offsets");
        this.numEntries = (Long) offsetsDS.getAttribute("numEntries").getData();
        this.offsets = new BitPackedUnSignedLongBuffer(null, offsetsDS.getBuffer(), numEntries, (Integer) offsetsDS.getAttribute("width").getData());

        ContiguousDataset datatypeDS = (ContiguousDataset) d.getDatasetByPath("datatypes");
        this.datatype = new BitPackedUnSignedLongBuffer(null, datatypeDS.getBuffer(), (Long) datatypeDS.getAttribute("numEntries").getData(), (Integer) datatypeDS.getAttribute("width").getData());

        Object xxx = d.getChild("typedLiterals");
        ContiguousDataset typedLiteralsDS = (xxx == null) ? null : (ContiguousDataset) d.getDatasetByPath("typedLiterals");
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

        buildTieredIndex();
    }

    private void buildTieredIndex() {
        if (numEntries <= TIER_SPACING) return;
        int tierSize = (int) (numEntries / TIER_SPACING);
        tieredIds = new long[tierSize];
        tieredNodes = new Node[tierSize];
        
        for (int i = 0; i < tierSize; i++) {
            long id = (long) i * TIER_SPACING + 1;
            tieredIds[i] = id;
            tieredNodes[i] = extract(id);
        }
    }

    private Optional<ContiguousDataset> getDataSet(Group g, String name) {
        return (g.getChild(name) != null) ? Optional.of((ContiguousDataset) g.getChild(name)) : Optional.empty();
    }

    @Override
    public Node extract(long id) {
        long idx = id - 1;
        if (idx < 0 || idx >= numEntries) return null;
        long off = offsets.get(idx);
        int typeOrdinal = (int) datatype.get(idx);
        
        if (typeOrdinal < 0 || typeOrdinal >= DT_VALUES.length) {
             throw new RuntimeException("Corrupt HDF5: Unknown DataType ordinal " + typeOrdinal + " at ID " + id);
        }
        DataType dt = DT_VALUES[typeOrdinal];        
        Node na = switch (dt) {
            case INTEGER -> NodeFactory.createLiteralByValue((int) integers.get(off));
            case LONG -> NodeFactory.createLiteralByValue(longs.get(off));
            case FLOAT -> NodeFactory.createLiteralByValue(floats.getFloat((int) (off * Float.BYTES)));
            case DOUBLE -> NodeFactory.createLiteralByValue(doubles.getDouble((int) (off * Double.BYTES)));
            case STRING -> NodeFactory.createLiteralDT(strings.get(off), tm.getSafeTypeByName(typedLiteralsDictionary.get(typedLiterals.get(idx)-1)));
            case IRI -> NodeFactory.createURI(iri.get(off));
            case BNODE -> NodeFactory.createBlankNode(String.format("b%020d", (id + offset)));
            default -> throw new IllegalStateException("Unsupported DataType: " + dt);
        };
        /*
        if (typedLiterals!=null) {
            long xxx = typedLiterals.get(idx);
            if (xxx>0) {
                IO.println("XXXXSTA : "+idx+" ] "+off+"  ===>  "+na+" LTT : "+typedLiterals.get(idx)+" ************* "+typedLiteralsDictionary.get(typedLiterals.get(idx)-1));
            }
        }*/
        return na;
    }

    @Override
    public long search(Node element) {
        //return this.searchGood(element);
        return this.searchFAST(element);
    }
    
    /**
     * Original binary search implementation.
     */
    private long searchGood(Node element) {
        long low = 1;
        long high = numEntries;
        while (low <= high) {
            long midId = low + (high - low) / 2;
            Node midNode = extract(midId);
            if (midNode == null) throw new Error("Dictionary Corruption at ID: " + midId);
            int cmp = NodeComparator.INSTANCE.compare(midNode, element);
            if (cmp == 0) {
                return midId;
            } else if (cmp < 0) {
                low = midId + 1;
            } else {
                high = midId - 1;
            }
        }
        return -low - 1;
    }

    /**
     * OPTIMIZED Search Method (Reliable Version).
     * 1. Uses Tiered Index to narrow the binary search range to ~1024 items.
     * 2. Uses extract() + NodeComparator to guarantee identical behavior to searchGood().
     */
    private long searchFAST(Node element) {
        long low = 1;
        long high = numEntries;

        // 1. Tiered Index Lookup to narrow the range
        // This is safe because tieredNodes are actual Node objects compared using your specific Comparator
        if (tieredNodes != null && tieredNodes.length > 0) {
            int tierIdx = Arrays.binarySearch(tieredNodes, element, NodeComparator.INSTANCE);
            if (tierIdx >= 0) return tieredIds[tierIdx]; 

            int insertionPoint = -(tierIdx + 1);
            if (insertionPoint > 0) {
                low = tieredIds[insertionPoint - 1];
            }
            if (insertionPoint < tieredIds.length) {
                high = tieredIds[insertionPoint] - 1;
            }
        }

        // 2. Binary Search within the narrowed range
        // Strictly uses extract() and NodeComparator.INSTANCE to match searchGood() behavior exactly.
        while (low <= high) {
            long midId = low + (high - low) / 2;
            Node midNode = extract(midId);
            if (midNode == null) throw new Error("Dictionary Corruption at ID: " + midId);
            
            int cmp = NodeComparator.INSTANCE.compare(midNode, element);

            if (cmp == 0) return midId;
            else if (cmp < 0) low = midId + 1;
            else high = midId - 1;
        }
        return -low - 1;
    }

    private boolean isDefaultGraph(Node n) {
        if (n == null) return true;
        return n.equals(Quad.defaultGraphIRI) || n.equals(Quad.defaultGraphNodeGenerated);
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
    
    public void setOffset(long off) {
        this.offset = off;
        // Rebuild index because BNode labels might have changed due to offset
        buildTieredIndex();
    }
}