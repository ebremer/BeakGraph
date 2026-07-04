package com.ebremer.beakgraph.benchmarks;

import com.ebremer.beakgraph.core.Dictionary;
import com.ebremer.beakgraph.core.GSPODictionary;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Dictionary term resolution: {@code locate} (Node -> id, tiered binary search
 * whose every probe decodes a front-coded block and compares via NodeComparator)
 * and {@code extract} (id -> Node, one block decode + Node allocation).
 *
 * <p>These dominate iterator construction (each concrete pattern term is
 * located) and result materialization (each projected id is extracted).
 * {@code nodeTableLookup} measures the cached path the query engine uses for
 * bound-variable terms, for contrast with the raw dictionary search.
 *
 * <p>Probe nodes are extracted from the store then re-created fresh, so hits are
 * guaranteed regardless of writer canonicalization while equality (not identity)
 * does the work.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsAppend = {
        "--enable-native-access=ALL-UNNAMED",
        "--sun-misc-unsafe-memory-access=allow",
        "-Xmx4g"})
@State(Scope.Benchmark)
public class DictionaryBench {

    private static final int PROBES = 1 << 12;
    private static final int MASK = PROBES - 1;

    @Param({"50000"})
    public int subjects;

    private HDF5Reader reader;
    private GSPODictionary dict;
    private Node[] entityProbes;
    private Node[] objectProbes;
    private Node[] predicateProbes;
    private Node[] missProbes;
    private long[] entityIds;
    private long[] objectIds;
    private int cursor;

    @Setup
    public void setup() {
        reader = new HDF5Reader(SyntheticStore.get(subjects).toFile());
        dict = reader.getDictionary();
        SplittableRandom rnd = new SplittableRandom(42);

        Dictionary entities = dict.getSubjects();
        Dictionary objects = dict.getObjects();
        long numEntities = entities.getNumberOfNodes();
        long numObjects = objects.getNumberOfNodes();

        entityProbes = new Node[PROBES];
        objectProbes = new Node[PROBES];
        missProbes = new Node[PROBES];
        entityIds = new long[PROBES];
        objectIds = new long[PROBES];
        for (int i = 0; i < PROBES; i++) {
            entityIds[i] = 1 + rnd.nextLong(numEntities);
            objectIds[i] = 1 + rnd.nextLong(numObjects);
            entityProbes[i] = freshCopy(entities.extract(entityIds[i]));
            objectProbes[i] = freshCopy(objects.extract(objectIds[i]));
            missProbes[i] = NodeFactory.createURI(SyntheticStore.NS + "missing/" + i);
        }
        predicateProbes = dict.streamPredicates()
                .map(DictionaryBench::freshCopy)
                .toArray(Node[]::new);
    }

    @TearDown
    public void tearDown() {
        reader.close();
    }

    /** Same term, new object: hits must come from equality, never identity. */
    private static Node freshCopy(Node n) {
        if (n.isURI()) {
            return NodeFactory.createURI(n.getURI());
        }
        if (n.isLiteral()) {
            String lang = n.getLiteralLanguage();
            if (lang != null && !lang.isEmpty()) {
                return NodeFactory.createLiteralLang(n.getLiteralLexicalForm(), lang);
            }
            return NodeFactory.createLiteralDT(n.getLiteralLexicalForm(), n.getLiteralDatatype());
        }
        if (n.isBlank()) {
            return NodeFactory.createBlankNode(n.getBlankNodeLabel());
        }
        return n;
    }

    @Benchmark
    public long entityLocateHit() {
        return dict.getSubjects().locate(entityProbes[cursor++ & MASK]);
    }

    /** Mixed URI/literal probes through the entity+literal object view. */
    @Benchmark
    public long objectLocateHit() {
        return dict.getObjects().locate(objectProbes[cursor++ & MASK]);
    }

    @Benchmark
    public long predicateLocateHit() {
        int i = cursor++;
        return dict.getPredicates().locate(predicateProbes[i % predicateProbes.length]);
    }

    @Benchmark
    public long locateMiss() {
        return dict.getSubjects().locate(missProbes[cursor++ & MASK]);
    }

    @Benchmark
    public Node extractEntity() {
        return dict.getSubjects().extract(entityIds[cursor++ & MASK]);
    }

    @Benchmark
    public Node extractObject() {
        return dict.getObjects().extract(objectIds[cursor++ & MASK]);
    }

    /** The Caffeine-cached Node -> NodeId path (SimpleNodeTable) used for bound variables. */
    @Benchmark
    public Object nodeTableLookup() {
        return reader.getNodeTable().getNodeIdForNode(entityProbes[cursor++ & MASK]);
    }
}
