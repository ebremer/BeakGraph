package com.ebremer.beakgraph.hdf5.writers;

import com.ebremer.beakgraph.core.DictionaryWriter;
import com.ebremer.beakgraph.core.Dictionary;
import com.ebremer.beakgraph.core.GSPODictionary;
import com.ebremer.beakgraph.core.lib.NodeComparator;
import com.ebremer.beakgraph.core.lib.Stats;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.Types;
import static com.ebremer.beakgraph.utils.UTIL.MinBits;
import io.jhdf.api.WritableGroup;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;

/**
 * Monolithic Entity Dictionary with Columnar ID lists for Graphs, Subjects, and Objects.
 * @author Erich Bremer
 */
public class PositionalDictionaryWriter implements GSPODictionary, AutoCloseable, DictionaryWriter {
    private final DictionaryWriter entitiesdict;
    private final DictionaryWriter predicatesdict;
    private final DictionaryWriter literalsdict;    
    private final long numQuads;
    private final String name;
    private final Quad[] quads;
    private final long maxEntityId;
    
    // Columnar ID Storage
    private final BitPackedUnSignedLongBuffer graphs;
    private final BitPackedUnSignedLongBuffer subjects;
    private final BitPackedUnSignedLongBuffer objects;

    public PositionalDictionaryWriter(PositionalDictionaryWriterBuilder builder) throws FileNotFoundException, IOException {
        this.name = builder.getName();
        this.numQuads = builder.getNumberOfQuads();
        this.quads = builder.getQuads();
        
        Stats stats = builder.getStats();
        IO.println(stats);
        
        // 1. Build the Monolithic Entity Dictionary (G, S, O URIs + BNodes)
        entitiesdict = new MultiTypeDictionaryWriter.Builder()
            .setName("entities")
            .setNodes(builder.getEntities())
            .setStats(builder.getStats())
            .enable(Types.IRI, Types.BNODE)
            .build();
            
        // 2. Build the Isolated Predicate Dictionary (P URIs)
        predicatesdict = new MultiTypeDictionaryWriter.Builder()
            .setName("predicates")
            .setNodes(builder.getPredicates())
            .setStats(builder.getStats())
            .enable(Types.IRI)
            .build();
            
        // 3. Build the Isolated Literal Dictionary (O Native Literals)
        literalsdict = new MultiTypeDictionaryWriter.Builder()
            .setName("literals")
            .setNodes(builder.getLiterals())
            .setDataTypes(builder.getDataTypes())
            .setStats(builder.getStats())
            .enable(Types.DOUBLE, Types.FLOAT, Types.LONG, Types.INTEGER, Types.STRING)
            .build();
            
        // Cache this for fast offset math in locateObject
        this.maxEntityId = entitiesdict.getNumberOfNodes();

        // 4. Initialize Bit-Packed Buffers for columnar ID lists
        // Determine required bit-widths based on the dictionary sizes
        int gBits = (int) (Math.ceil(MinBits(getNumberOfGraphs() + 1) / 8.0) * 8);
        int sBits = (int) (Math.ceil(MinBits(getNumberOfSubjects() + 1) / 8.0) * 8);
        int oBits = (int) (Math.ceil(MinBits(getNumberOfObjects() + 1) / 8.0) * 8);

        this.graphs = new BitPackedUnSignedLongBuffer(Path.of("graphs"), null, 0, gBits);
        this.subjects = new BitPackedUnSignedLongBuffer(Path.of("subjects"), null, 0, sBits);
        this.objects = new BitPackedUnSignedLongBuffer(Path.of("objects"), null, 0, oBits);

        // 5. Populate ID lists from the unique sets collected by the Builder
        System.out.println("Populating columnar ID lists with unique entities...");
        ArrayList<Node> src = parallelSort(builder.getUniqueGraphs());
        for (Node n : src) {
            graphs.writeLong(locateGraph(n));
        }
        src = parallelSort(builder.getUniqueSubjects());
        for (Node n : src) {
            subjects.writeLong(locateSubject(n));
        }
        src = parallelSort(builder.getUniqueObjects());
        for (Node n : src) {
            objects.writeLong(locateObject(n));
        }
        src = null;
        // Finalize buffers for writing to HDF5
        graphs.prepareForReading();
        subjects.prepareForReading();
        objects.prepareForReading();
    }
    
    private static ArrayList<Node> parallelSort(Set<Node> nodes) {
        return nodes.parallelStream()
            .sorted(NodeComparator.INSTANCE)
            .collect(Collectors.toCollection(ArrayList::new));
    }
   
    public Quad[] getQuads() {
        return quads;
    }
   
    public long getNumberOfQuads() {
        return numQuads;
    }
    
    public long getNumberOfGraphs() {
        return maxEntityId; 
    }
   
    public long getNumberOfSubjects() {
        return maxEntityId; 
    }
   
    public long getNumberOfPredicates() {
        return predicatesdict.getNumberOfNodes();
    }
   
    public long getNumberOfObjects() {
        return maxEntityId + literalsdict.getNumberOfNodes();
    }
   
    @Override
    public long locateGraph(Node element) {
        long c = ((Dictionary) entitiesdict).locate(element);
        if (c > 0) return c;
        throw new Error("Cannot resolve Graph : " + element);
    }
   
    @Override
    public long locateSubject(Node element) {
        long c = ((Dictionary) entitiesdict).locate(element);
        if (c > 0) return c;
        throw new Error("Cannot resolve Subject : " + element);
    }
   
    @Override
    public long locatePredicate(Node element) {
        long c = ((Dictionary) predicatesdict).locate(element);
        if (c > 0) return c;
        throw new Error("Cannot resolve Predicate : " + element);
    }
   
    @Override
    public long locateObject(Node element) {
        if (element.isLiteral()) {
            long c = ((Dictionary) literalsdict).locate(element);
            if (c > 0) return c + maxEntityId; // Offset by Entity block size
            return -1;
        } else {
            long c = ((Dictionary) entitiesdict).locate(element);
            if (c > 0) return c;
            return -1;
        }
    }
   
    @Override
    public void Add(WritableGroup group) {
        WritableGroup dictionary = group.putGroup(name);
        
        // Add Sub-dictionaries
        if (entitiesdict.getNumberOfNodes() > 0) entitiesdict.Add(dictionary);
        if (predicatesdict.getNumberOfNodes() > 0) predicatesdict.Add(dictionary);
        if (literalsdict.getNumberOfNodes() > 0) literalsdict.Add(dictionary);
        
        // Add columnar ID lists
        if (numQuads > 0) {
            graphs.Add(dictionary);
            subjects.Add(dictionary);
            objects.Add(dictionary);
        }
    }

    @Override
    public void close() {
        // Implementation for AutoCloseable if needed
    }

    // --- Interface Boilerplate / Unsupported Methods ---

    @Override public Object extractGraph(long id) { throw new UnsupportedOperationException(); }
    @Override public Object extractSubject(long id) { throw new UnsupportedOperationException(); }
    @Override public Object extractPredicate(long id) { throw new UnsupportedOperationException(); }
    @Override public Object extractObject(long id) { throw new UnsupportedOperationException(); }
    @Override public long getNumberOfNodes() { throw new UnsupportedOperationException(); }
    @Override public List<Node> getNodes() { throw new UnsupportedOperationException(); }
    @Override public Stream<Node> streamSubjects() { throw new UnsupportedOperationException(); }
    @Override public Stream<Node> streamPredicates() { throw new UnsupportedOperationException(); }
    @Override public Stream<Node> streamObjects() { throw new UnsupportedOperationException(); }
    @Override public Stream<Node> streamGraphs() { throw new UnsupportedOperationException(); }
    @Override public Dictionary getGraphs() { throw new UnsupportedOperationException(); }
    @Override public Dictionary getSubjects() { throw new UnsupportedOperationException(); }
    @Override public Dictionary getPredicates() { throw new UnsupportedOperationException(); }
    @Override public Dictionary getObjects() { throw new UnsupportedOperationException(); }
}
