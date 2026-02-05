package com.ebremer.beakgraph.hdf5.readers;

import com.ebremer.beakgraph.core.lib.DataType;
import com.ebremer.beakgraph.core.lib.NodeComparator;
import com.ebremer.beakgraph.core.AbstractDictionary;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import io.jhdf.api.Group;
import io.jhdf.api.dataset.ContiguousDataset;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;

public class MultiTypeDictionaryReader extends AbstractDictionary {
    private static final DataType[] DT_VALUES = DataType.values();
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
    private long offset = 0;
    private final String name;
    private static final TypeMapper tm = TypeMapper.getInstance();

    public MultiTypeDictionaryReader(Group d) {
        this.name = d.getName();
        ContiguousDataset offsetsDS = (ContiguousDataset) d.getDatasetByPath("offsets");
        this.numEntries = (Long) offsetsDS.getAttribute("numEntries").getData();
        this.offsets = new BitPackedUnSignedLongBuffer(null, offsetsDS.getBuffer(), numEntries, (Integer) offsetsDS.getAttribute("width").getData());

        ContiguousDataset datatypeDS = (ContiguousDataset) d.getDatasetByPath("datatypes");
        
        Object xxx= d.getChild("typedLiterals");
        //Dataset xxx = d.getDatasetByPath("typedLiterals");
        ContiguousDataset typedLiteralsDS = (xxx==null) ? null : (ContiguousDataset) d.getDatasetByPath("typedLiterals");        
        this.typedLiterals = (typedLiteralsDS != null) ? new BitPackedUnSignedLongBuffer(null, typedLiteralsDS.getBuffer(), (Long) typedLiteralsDS.getAttribute("numEntries").getData(), (Integer) typedLiteralsDS.getAttribute("width").getData()):null;

        this.datatype = new BitPackedUnSignedLongBuffer(null, datatypeDS.getBuffer(), (Long) datatypeDS.getAttribute("numEntries").getData(), (Integer) datatypeDS.getAttribute("width").getData());
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
        /*
        if (typedLiteralsDictionary!=null) {
            LongStream.range(0, typedLiteralsDictionary.getNumEntries())
                    .forEach(i->IO.println("typedLiteralsDictionary : "+i+" ] "+typedLiteralsDictionary.get(i)));
            IO.println("NUMBER OF typedLiterals : "+typedLiterals.getNumEntries());
            try {
            LongStream.range(0, typedLiterals.getNumEntries())
                    .mapToObj(i-> {
                        return extract(i+1)+" >>>> typedLiterals : "+i+" ] "+(typedLiterals.get(i));
                    })
                    .filter(s->!s.contains("POLYGON"))
                    .filter(s->!s.contains("_:b"))
                    .forEach(s->IO.println("*** "+s));
                    
            } catch (BufferUnderflowException ex) {
                IO.println("ACK!!!!!!!!!!!!!!!! "+ex.getMessage());
            }
        }*/
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
        // Safety check for corrupt HDF5 files
        if (typeOrdinal < 0 || typeOrdinal >= DT_VALUES.length) {
             throw new RuntimeException("Corrupt HDF5: Unknown DataType ordinal " + typeOrdinal + " at ID " + id);
        }
        DataType dt = DT_VALUES[typeOrdinal];        
        Node na = switch (dt) {
            case INTEGER -> NodeFactory.createLiteralByValue((int) integers.get(off));
            case LONG    -> NodeFactory.createLiteralByValue(longs.get(off));
            case FLOAT   -> NodeFactory.createLiteralByValue(floats.getFloat((int) (off * Float.BYTES)));
            case DOUBLE  -> NodeFactory.createLiteralByValue(doubles.getDouble((int) (off * Double.BYTES)));
            //case STRING  -> NodeFactory.createLiteralByValue(strings.get(off));
            case STRING  -> NodeFactory.createLiteralDT(strings.get(off), tm.getSafeTypeByName(typedLiteralsDictionary.get(typedLiterals.get(idx)-1)));
            case IRI     -> NodeFactory.createURI(iri.get(off));
            case BNODE   -> NodeFactory.createBlankNode(String.format("b%020d", (id + offset)));
            default      -> throw new IllegalStateException("Unsupported DataType: " + dt);
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
    }
}
