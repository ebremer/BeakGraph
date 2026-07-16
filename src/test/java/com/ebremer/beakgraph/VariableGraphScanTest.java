package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.hdf5.jena.BGIteratorMaster;
import com.ebremer.beakgraph.hdf5.jena.BindingNodeId;
import com.ebremer.beakgraph.hdf5.jena.NodeId;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * When the graph position is an unbound variable, BGIteratorMaster must scan only
 * the actual graphs (default graph + named graphs such as the generated VOID graph)
 * and bind the graph variable to each - not spin up a sub-iterator per entity and
 * drop the binding.
 */
class VariableGraphScanTest {

    @TempDir
    static Path dir;
    static File h5;

    @BeforeAll
    static void build() throws Exception {
        // Default-graph triples; the writer also emits a named VOID graph, so there is
        // more than one real graph to visit.
        StringBuilder ttl = new StringBuilder("@prefix ex: <http://ex.org/> .\n");
        for (int i = 0; i < 20; i++) {
            ttl.append("ex:s").append(i).append(" ex:p ex:o").append(i).append(" .\n");
        }
        File t = dir.resolve("vg.ttl").toFile();
        h5 = dir.resolve("vg.ttl.h5").toFile();
        Files.write(t.toPath(), ttl.toString().getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setVoidMode(com.ebremer.beakgraph.core.VoidMode.EXACT).setSource(t).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
    }

    @Test
    void variableGraphScanBindsGraphAndCoversExactlyTheRealGraphs() throws Exception {
        try (HDF5Reader reader = new HDF5Reader(h5)) {
            PositionalDictionaryReader dict = (PositionalDictionaryReader) reader.getDictionary();
            NodeTable nt = reader.getNodeTable();
            Var g = Var.alloc("g"), s = Var.alloc("s"), p = Var.alloc("p"), o = Var.alloc("o");

            Set<Node> graphsSeen = new HashSet<>();
            long count = 0;
            Iterator<BindingNodeId> it = new BGIteratorMaster(
                    reader, dict, new BindingNodeId(), new Quad(g, s, p, o), null, nt);
            while (it.hasNext()) {
                BindingNodeId b = it.next();
                long gid = b.get(g);
                assertNotEquals(NodeId.NONE, gid, "the graph variable must be bound in every solution");
                graphsSeen.add(nt.getNodeForNodeId(gid));
                count++;
            }

            Set<Node> realGraphs = dict.streamGraphs().collect(Collectors.toSet());
            // More than one graph (default + VOID), and we visit exactly the real ones.
            assertTrue(realGraphs.size() >= 2, "expected default + at least one named graph");
            assertEquals(realGraphs, graphsSeen, "scan must visit exactly the real graphs");
            assertTrue(count >= 20, "should return at least the 20 default-graph triples; got " + count);
        }
    }

    @Test
    void preBoundGraphVariableScansTheBoundGraph() throws Exception {
        // The graph var is a VARIABLE in the quad but already bound in the
        // BindingNodeId (BGIteratorMaster routes this shape to BGIteratorSPO_All
        // when the predicate is unbound). SPO_All used to locate() the raw
        // variable node, get -1, and silently yield nothing for a graph that
        // exists - unlike the other three iterators behind the same dispatcher.
        try (HDF5Reader reader = new HDF5Reader(h5)) {
            PositionalDictionaryReader dict = (PositionalDictionaryReader) reader.getDictionary();
            NodeTable nt = reader.getNodeTable();
            Var g = Var.alloc("g"), s = Var.alloc("s"), p = Var.alloc("p"), o = Var.alloc("o");

            long gid = dict.getGraphs().locate(Quad.defaultGraphIRI);
            assertTrue(gid >= 1, "the default graph must exist in the dictionary");
            BindingNodeId bnid = new BindingNodeId();
            bnid.put(g, NodeId.pack(com.ebremer.beakgraph.hdf5.jena.NodeType.GRAPH, gid));

            long count = 0;
            Iterator<BindingNodeId> it = new BGIteratorMaster(
                    reader, dict, bnid, new Quad(g, s, p, o), null, nt);
            while (it.hasNext()) {
                it.next();
                count++;
            }
            assertEquals(20, count, "the pre-bound default graph holds exactly the 20 source triples");
        }
    }
}
