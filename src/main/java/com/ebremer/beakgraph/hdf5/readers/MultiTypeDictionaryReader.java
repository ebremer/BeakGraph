package com.ebremer.beakgraph.hdf5.readers;

import com.ebremer.beakgraph.core.lib.DataType;
import com.ebremer.beakgraph.core.lib.NodeComparator;
import com.ebremer.beakgraph.core.AbstractDictionary;
import com.ebremer.beakgraph.hdf5.BitPackedSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import io.jhdf.api.Group;
import io.jhdf.api.dataset.ContiguousDataset;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;

/**
 * Reads a dictionary that can store multiple data types (IRI, BNode, String, numbers).
 * @author Erich Bremer
 */
public class MultiTypeDictionaryReader extends AbstractDictionary {

    private final BitPackedUnSignedLongBuffer offsets;
    private final BitPackedSignedLongBuffer integers;
    private final BitPackedSignedLongBuffer longs;
    private final BitPackedUnSignedLongBuffer datatype;
    private final ByteBuffer floats;
    private final ByteBuffer doubles;
    private final FCDReader iri;
    private final FCDReader strings;
    private final long numEntries;
    private long offset = 0;
    private String name;

    private Optional<ContiguousDataset> getDataSet(Group g, String name) {
        if (g.getChild(name) != null) {
            return Optional.of((ContiguousDataset) g.getChild(name));
        }
        return Optional.empty();
    }
    
    @Override
    public long getNumberOfNodes() {
        return numEntries;
    }
    
    public void setOffset(long off) {
        this.offset = off;
    }
    
    public MultiTypeDictionaryReader(Group d) {
        this.name = d.getName();
        ContiguousDataset offsetsDS = (ContiguousDataset) d.getDatasetByPath("offsets");
        this.numEntries = (Long) offsetsDS.getAttribute("numEntries").getData();
        this.offsets = new BitPackedUnSignedLongBuffer(null, offsetsDS.getBuffer(), numEntries, (Integer) offsetsDS.getAttribute("width").getData());

        ContiguousDataset datatypeDS = (ContiguousDataset) d.getDatasetByPath("datatype");
        this.datatype = new BitPackedUnSignedLongBuffer(null, datatypeDS.getBuffer(), (Long) datatypeDS.getAttribute("numEntries").getData(), (Integer) datatypeDS.getAttribute("width").getData());
        
        Optional<ContiguousDataset> doublesDS = getDataSet(d, "doubles");
        this.doubles = doublesDS.map(ds -> ds.getBuffer().order(ByteOrder.BIG_ENDIAN)).orElse(null);

        Optional<ContiguousDataset> floatsDS = getDataSet(d, "floats");
        this.floats = floatsDS.map(ds -> ds.getBuffer().order(ByteOrder.BIG_ENDIAN)).orElse(null);

        Optional<ContiguousDataset> integersDS = getDataSet(d, "integers");
        this.integers = integersDS.map(ds -> new BitPackedSignedLongBuffer(null, ds.getBuffer(), (Integer) ds.getAttribute("width").getData())).orElse(null);

        Optional<ContiguousDataset> longsDS = getDataSet(d, "longs");
        this.longs = longsDS.map(ds -> new BitPackedSignedLongBuffer(null, ds.getBuffer(), (Integer) ds.getAttribute("width").getData())).orElse(null);
        
        Group stringsG = (Group) d.getChild("strings");
        this.strings = (stringsG != null) ? new FCDReader(stringsG) : null;
        
        Group iriG = (Group) d.getChild("iri");
        this.iri = (iriG != null) ? new FCDReader(iriG) : null;
    }

    @Override
    public Stream<Node> streamNodes() {
        return LongStream.rangeClosed( 1, numEntries ).mapToObj(this::extract);
    }

    @Override
    public Node extract(long id) {
        long idx = id - 1;
        if (idx < 0 || idx >= numEntries) {
            return null;
        }
        long off = offsets.get(idx);
        DataType dt = DataType.values()[(int) datatype.get(idx)];
        return switch (dt) {
            case INTEGER -> NodeFactory.createLiteralByValue((int) integers.get(off));
            case LONG -> NodeFactory.createLiteralByValue(longs.get(off));
            case FLOAT -> NodeFactory.createLiteralByValue(floats.getFloat((int) (off * Float.BYTES)));
            case DOUBLE -> NodeFactory.createLiteralByValue(doubles.getDouble((int) (off * Double.BYTES)));
            case STRING -> NodeFactory.createLiteralByValue(strings.get(off));
            case IRI -> NodeFactory.createURI(iri.get(off));
            case BNODE -> NodeFactory.createBlankNode(String.format("b%020d", (idx + 1 + offset)));
            default -> throw new IllegalStateException("Unknown DataType: " + dt);
        };
    }

    @Override
    public long locate(Node element) {
        long result = search(element);
        return (result >= 0) ? result : -1;
    }

    @Override
    public long search(Node element) {
        long low = 1;
        long high = numEntries;        
        while (low <= high) {
            long midIndex = low + (high - low) / 2;
            long midId = midIndex;
            Node midNode = extract(midId);                        
            if (midNode == null) throw new Error("Dictionary Corruption: extract returned null for valid ID range");            
            int cmp = NodeComparator.INSTANCE.compare(midNode, element);            
            if (cmp == 0) {
                return midId; // Found
            } else if (cmp < 0) {
                low = midIndex + 1; // mid < element
            } else {
                high = midIndex - 1; // mid > element
            }
        }
        // Not found. Return encoded insertion point (negative).
        // low is the index of the first element greater than the key.
        return -low - 1; 
    }
}