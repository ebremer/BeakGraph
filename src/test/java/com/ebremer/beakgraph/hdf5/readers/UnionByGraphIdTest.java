package com.ebremer.beakgraph.hdf5.readers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.jena.BindingNodeId;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The union and any-graph fan-outs drive their per-graph reads from the
 * columnar graph ids (BG-259). Parity against an in-memory dataset for the
 * union graph, explicit FROM lists, the id-taking read, and the dataset
 * graph's wildcard find / findNG.
 */
class UnionByGraphIdTest {

    private static final String TRIG = """
        @prefix ex: <http://ex.org/> .
        ex:d ex:p ex:o0 .
        ex:g1 { ex:s ex:p ex:o1 . ex:s ex:p ex:shared . ex:t ex:q 1 . }
        ex:g2 { ex:s ex:p ex:o2 . ex:s ex:p ex:shared . ex:t ex:q 2 . }
        ex:g3 { ex:s ex:p ex:o3 . ex:u ex:q "three" . }
        ex:g4 { ex:s ex:p ex:shared . ex:g1 ex:p ex:g2 . }
        """;

    @TempDir
    static Path dir;
    static File h5;
    static HDF5Reader reader;
    static DatasetGraph truth;

    @BeforeAll
    static void build() throws Exception {
        Path src = dir.resolve("union.trig");
        Files.writeString(src, TRIG);
        h5 = dir.resolve("union.h5").toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        reader = new HDF5Reader(h5);
        truth = DatasetGraphFactory.create();
        RDFParser.create().source(src.toUri().toString()).lang(Lang.TRIG).parse(truth);
    }

    @AfterAll
    static void close() {
        if (reader != null) reader.close();
    }

    private static final Var S = Var.alloc("s"), P = Var.alloc("p"), O = Var.alloc("o");
    private static final Triple ALL = Triple.create(S, P, O);

    private static Set<String> triplesOf(Iterator<BindingNodeId> it) {
        return triplesOf(it, ALL);
    }

    private static Set<String> triplesOf(Iterator<BindingNodeId> it, Triple pattern) {
        Set<String> out = new TreeSet<>();
        while (it.hasNext()) {
            BindingNodeId b = it.next();
            out.add(term(pattern.getSubject(), b, S) + " " + term(pattern.getPredicate(), b, P) + " " + term(pattern.getObject(), b, O));
        }
        return out;
    }

    private static Node term(Node patternNode, BindingNodeId b, Var v) {
        return patternNode.isConcrete() ? patternNode : reader.getNodeTable().getNodeForNodeId(b.get(v));
    }

    private static Set<String> truthTriples(Node... graphs) {
        Set<String> out = new TreeSet<>();
        for (Node g : graphs) {
            Iterator<Quad> it = truth.find(g, Node.ANY, Node.ANY, Node.ANY);
            while (it.hasNext()) {
                Quad q = it.next();
                out.add(q.getSubject() + " " + q.getPredicate() + " " + q.getObject());
            }
        }
        return out;
    }

    private static Node g(int i) {
        return NodeFactory.createURI("http://ex.org/g" + i);
    }

    @Test
    void theUnionGraphIsTheDistinctUnionOfEveryNamedGraph() {
        Set<String> expected = truthTriples(g(1), g(2), g(3), g(4));
        assertEquals(8, expected.size(), "fixture: the shared triple collapses");
        assertEquals(expected, triplesOf(reader.read(Quad.unionGraph, new BindingNodeId(), ALL, null, reader.getNodeTable())));
        Triple bound = Triple.create(NodeFactory.createURI("http://ex.org/s"), NodeFactory.createURI("http://ex.org/p"), O);
        Set<String> sp = new TreeSet<>();
        for (String t : expected) if (t.startsWith("http://ex.org/s http://ex.org/p ")) sp.add(t);
        assertEquals(sp, triplesOf(reader.read(Quad.unionGraph, new BindingNodeId(), bound, null, reader.getNodeTable()), bound));
    }

    @Test
    void aFromListReadsExactlyItsMembers() {
        assertEquals(truthTriples(g(1), g(3)),
                triplesOf(reader.readGraphs(List.of(g(1), g(3)), new BindingNodeId(), ALL, null, reader.getNodeTable())));
        assertEquals(truthTriples(g(1), g(2), g(3), g(4)),
                triplesOf(reader.readGraphs(List.of(g(2), Quad.unionGraph), new BindingNodeId(), ALL, null, reader.getNodeTable())));
        assertEquals(truthTriples(Quad.defaultGraphIRI, g(4)),
                triplesOf(reader.readGraphs(List.of(Quad.defaultGraphNodeGenerated, g(4), NodeFactory.createURI("http://ex.org/absent")),
                        new BindingNodeId(), ALL, null, reader.getNodeTable())));
    }

    @Test
    void theIdTakingReadAnswersWhatTheTermTakingReadDoes() {
        PositionalDictionaryReader dict = (PositionalDictionaryReader) reader.getDictionary();
        long[] ids = reader.graphIds().toArray();
        assertEquals(5, ids.length, "default graph plus four named graphs");
        for (long gid : ids) {
            Node gn = dict.getGraphs().extract(gid);
            assertEquals(triplesOf(reader.read(gn, new BindingNodeId(), ALL, null, reader.getNodeTable())),
                    triplesOf(reader.read(gid, new BindingNodeId(), ALL, null, reader.getNodeTable())), gn.toString());
        }
        assertTrue(triplesOf(reader.read(0L, new BindingNodeId(), ALL, null, reader.getNodeTable())).isEmpty());
        assertTrue(triplesOf(reader.read(ids[ids.length - 1] + 100_000, new BindingNodeId(), ALL, null, reader.getNodeTable())).isEmpty());
    }

    @Test
    void theDatasetGraphWildcardFindsWalkEveryGraphOnce() {
        try (BeakGraph bg = new BeakGraph(new HDF5Reader(h5))) {
            DatasetGraph dsg = bg.getDataset().asDatasetGraph();
            List<String> any = new ArrayList<>();
            dsg.find(Node.ANY, Node.ANY, Node.ANY, Node.ANY).forEachRemaining(q -> {
                if (!Params.isInternalGraph(q.getGraph())) any.add(q.toString());
            });
            List<String> expected = new ArrayList<>();
            truth.find(Node.ANY, Node.ANY, Node.ANY, Node.ANY).forEachRemaining(q -> expected.add(q.toString()));
            any.sort(null);
            expected.sort(null);
            assertEquals(expected, any, "find(ANY): the default graph and every named graph, each once");

            List<String> named = new ArrayList<>();
            dsg.findNG(Node.ANY, Node.ANY, Node.ANY, Node.ANY).forEachRemaining(q -> {
                if (!Params.isInternalGraph(q.getGraph())) named.add(q.toString());
            });
            List<String> expectedNamed = new ArrayList<>();
            truth.findNG(Node.ANY, Node.ANY, Node.ANY, Node.ANY).forEachRemaining(q -> expectedNamed.add(q.toString()));
            named.sort(null);
            expectedNamed.sort(null);
            assertEquals(expectedNamed, named, "findNG(ANY): named graphs only");
        }
    }
}
