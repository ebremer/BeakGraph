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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Monolithic Entity Dictionary with Columnar ID lists for Graphs, Subjects, and Objects.
 * @author Erich Bremer
 */
public class PositionalDictionaryWriter implements GSPODictionary, AutoCloseable, DictionaryWriter {
    private static final Logger logger = LoggerFactory.getLogger(PositionalDictionaryWriter.class);
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
        logger.debug("{}", stats);
        
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
            
        // Cache this for fast offset math in locateObject. Assigned BEFORE the
        // literals build: the triple-term encoder below resolves object-space
        // ids, which are literal-section ranks offset by this value.
        this.maxEntityId = entitiesdict.getNumberOfNodes();

        // 3. Build the Isolated Literal Dictionary (O native literals + RDF 1.2
        // triple terms, which macro-rank after every literal and so form a
        // contiguous suffix of this section - PLAN Part IV §IV.2).
        literalsdict = new MultiTypeDictionaryWriter.Builder()
            .setName("literals")
            .setNodes(builder.getLiterals())
            .setDataTypes(builder.getDataTypes())
            .setStats(builder.getStats())
            .enable(Types.DOUBLE, Types.FLOAT, Types.LONG, Types.INTEGER, Types.STRING, Types.TRIPLE_TERM)
            .setTripleTermEncoder(this::encodeTripleTerm)
            .setTripleTermComponentIdBound(maxEntityId + builder.getLiterals().size())
            .build();

        // 4. Initialize Bit-Packed Buffers for columnar ID lists
        // Determine required bit-widths based on the dictionary sizes
        int gBits = (int) (Math.ceil(MinBits(getNumberOfGraphs() + 1) / 8.0) * 8);
        int sBits = (int) (Math.ceil(MinBits(getNumberOfSubjects() + 1) / 8.0) * 8);
        int oBits = (int) (Math.ceil(MinBits(getNumberOfObjects() + 1) / 8.0) * 8);

        this.graphs = new BitPackedUnSignedLongBuffer(Path.of("graphs"), null, 0, gBits);
        this.subjects = new BitPackedUnSignedLongBuffer(Path.of("subjects"), null, 0, sBits);
        this.objects = new BitPackedUnSignedLongBuffer(Path.of("objects"), null, 0, oBits);

        // 5. Populate ID lists from the unique sets collected by the Builder
        logger.info("Populating columnar ID lists...");
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
        logger.info("Columnar ID lists populated");
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
        throw new IllegalStateException("Cannot resolve Graph (not in dictionary): " + element);
    }
   
    @Override
    public long locateSubject(Node element) {
        long c = ((Dictionary) entitiesdict).locate(element);
        if (c > 0) return c;
        throw new IllegalStateException("Cannot resolve Subject (not in dictionary): " + element);
    }
   
    @Override
    public long locatePredicate(Node element) {
        long c = ((Dictionary) predicatesdict).locate(element);
        if (c > 0) return c;
        throw new IllegalStateException("Cannot resolve Predicate (not in dictionary): " + element);
    }
   
    /**
     * Component-id resolution for the literals section's triple terms (PLAN
     * Part IV §IV.3). Entities and predicates are fully built by the time the
     * literals section encodes; literal and nested-triple-term objects resolve
     * through the section's OWN already-sorted ranks (passed in as
     * {@code ownSection}, since this runs while literalsdict is still under
     * construction), offset into the object space.
     */
    private long[] encodeTripleTerm(Node tt, Dictionary ownSection) {
        org.apache.jena.graph.Triple t = tt.getTriple();
        long s = ((Dictionary) entitiesdict).locate(t.getSubject());
        long p = ((Dictionary) predicatesdict).locate(t.getPredicate());
        Node o = t.getObject();
        long oid;
        if (o.isLiteral() || o.isTripleTerm()) {
            long lid = ownSection.locate(o);
            oid = (lid > 0) ? lid + maxEntityId : -1;
        } else {
            oid = ((Dictionary) entitiesdict).locate(o);
        }
        if (s < 1 || p < 1 || oid < 1) {
            // Same stance as the locate* methods: during a write every component
            // is already in its dictionary, so a miss is a build-invariant violation.
            throw new IllegalStateException("Cannot resolve triple-term components (not in dictionaries): "
                    + tt + " (s=" + s + ", p=" + p + ", o=" + oid + ")");
        }
        return new long[]{s, p, oid};
    }

    @Override
    public long locateObject(Node element) {
        if (element.isLiteral() || element.isTripleTerm()) {
            long c = ((Dictionary) literalsdict).locate(element);
            if (c > 0) return c + maxEntityId; // Offset by Entity block size
        } else {
            long c = ((Dictionary) entitiesdict).locate(element);
            if (c > 0) return c;
        }
        // Consistent with the other locate* methods: during a write every quad's nodes
        // are already in the dictionary, so a miss is a build-invariant violation.
        throw new IllegalStateException("Cannot resolve Object (not in dictionary): " + element);
    }
   
    @Override
    public void add(WritableGroup group) {
        WritableGroup dictionary = group.putGroup(name);
        
        // Add Sub-dictionaries
        if (entitiesdict.getNumberOfNodes() > 0) entitiesdict.add(dictionary);
        if (predicatesdict.getNumberOfNodes() > 0) predicatesdict.add(dictionary);
        if (literalsdict.getNumberOfNodes() > 0) literalsdict.add(dictionary);
        
        // Add columnar ID lists whenever any quads are stored. Gating on the
        // SOURCE quad count (numQuads) left an empty-source file internally
        // inconsistent: the always-written VoID metadata graph was present in the
        // indexes, but with no graphs list, containsGraph answered false and ARQ
        // refused to execute GRAPH queries against rows that are demonstrably there.
        if (graphs.getNumEntries() > 0) {
            graphs.add(dictionary);
            subjects.add(dictionary);
            objects.add(dictionary);
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
