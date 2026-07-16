package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.jena.BindingBG;
import com.ebremer.beakgraph.hdf5.jena.BindingNodeId;
import com.ebremer.beakgraph.hdf5.jena.NodeId;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression tests for three dataset/binding contract gaps:
 * <ul>
 *   <li>{@code GRAPH <urn:x-arq:unionGraph>} must query the union of all named
 *       graphs (it silently returned empty).</li>
 *   <li>{@code findNG} must return named-graph quads only (it delegated to
 *       {@code find}, which prepends the default graph).</li>
 *   <li>{@code BindingBG.detachWithNewParent} must materialize the
 *       NodeId-backed bindings (it threw UnsupportedOperationException, which
 *       breaks Jena's binding detach/copy paths).</li>
 * </ul>
 */
class UnionGraphAndDetachTest {

    private static final String TTL = """
        @prefix ex: <http://ex.org/> .
        ex:s1 ex:p ex:o1 .
        ex:s2 ex:p "v" .
        """;

    private static final int DEFAULT_GRAPH_TRIPLES = 2;
    // Use Jena's constant - the URI is urn:x-arq:UnionGraph (capital U), and a
    // hand-typed variant silently names a non-existent graph.
    private static final String UNION = Quad.unionGraph.getURI();

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;

    @BeforeAll
    static void buildAndOpen() throws Exception {
        File ttl = dir.resolve("union.ttl").toFile();
        File h5 = dir.resolve("union.ttl.h5").toFile();
        Files.write(ttl.toPath(), TTL.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setVoidMode(com.ebremer.beakgraph.core.VoidMode.EXACT)
                .setSource(ttl).setDestination(h5)
                .setSpatial(false).setFeatures(false)
                .build().write();
        bg = new BeakGraph(new HDF5Reader(h5));
        ds = bg.getDataset();
    }

    @AfterAll
    static void closeReader() {
        if (bg != null) bg.close();
    }

    private static int count(String query) {
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create("PREFIX ex: <http://ex.org/> " + query)).build()) {
            ResultSet rs = qe.execSelect();
            int n = 0;
            while (rs.hasNext()) { rs.next(); n++; }
            return n;
        }
    }

    private static int voidGraphSize() {
        return count("SELECT * WHERE { GRAPH <" + Params.VOIDSTRING + "> { ?s ?p ?o } }");
    }

    // --- union default graph -------------------------------------------------

    @Test
    void unionGraphQueriesAllNamedGraphs() {
        int voidCount = voidGraphSize();
        assertTrue(voidCount > 0, "control: the VoID named graph is non-empty");
        assertEquals(voidCount,
            count("SELECT * WHERE { GRAPH <" + UNION + "> { ?s ?p ?o } }"),
            "the union graph must contain exactly the named-graph triples");
    }

    @Test
    void unionGraphExcludesDefaultGraphTriples() {
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(
                "PREFIX ex: <http://ex.org/> ASK { GRAPH <" + UNION + "> { ex:s1 ex:p ex:o1 } }")).build()) {
            assertFalse(qe.execAsk(), "default-graph triples are not part of the union of named graphs");
        }
    }

    @Test
    void getUnionModelWorks() {
        assertEquals(voidGraphSize(), ds.getUnionModel().size());
    }

    // --- findNG ---------------------------------------------------------------

    @Test
    void findNGReturnsNamedGraphQuadsOnly() {
        Iterator<Quad> it = ds.asDatasetGraph().findNG(Node.ANY, Node.ANY, Node.ANY, Node.ANY);
        int n = 0;
        while (it.hasNext()) {
            Quad q = it.next();
            assertFalse(Quad.isDefaultGraph(q.getGraph()),
                "findNG must not return default-graph quads, got: " + q);
            n++;
        }
        assertEquals(voidGraphSize(), n);
    }

    @Test
    void findAnyStillIncludesDefaultGraph() {
        Iterator<Quad> it = ds.asDatasetGraph().find(Node.ANY, Node.ANY, Node.ANY, Node.ANY);
        int n = 0;
        while (it.hasNext()) { it.next(); n++; }
        assertEquals(DEFAULT_GRAPH_TRIPLES + voidGraphSize(), n);
    }

    // --- detach ----------------------------------------------------------------

    @Test
    void detachMaterializesNodeIdBindings() {
        Node s1 = NodeFactory.createURI("http://ex.org/s1");
        long id = bg.getReader().getNodeTable().getNodeIdForNode(s1);
        assertFalse(NodeId.isDoesNotExist(id), "control: ex:s1 must be in the dictionary");

        Node parentTerm = NodeFactory.createURI("http://ex.org/parent");
        Binding parent = BindingFactory.binding(Var.alloc("x"), parentTerm);
        BindingNodeId bnid = new BindingNodeId(parent);
        bnid.put(Var.alloc("s"), id);

        Binding detached = new BindingBG(bnid, bg).detach();
        assertFalse(detached instanceof BindingBG,
            "detach must materialize away from the reader-backed binding");
        assertEquals(s1, detached.get(Var.alloc("s")),
            "detached binding must carry the materialized node");
        assertEquals(parentTerm, detached.get(Var.alloc("x")),
            "detached binding must keep parent bindings");
    }
}
