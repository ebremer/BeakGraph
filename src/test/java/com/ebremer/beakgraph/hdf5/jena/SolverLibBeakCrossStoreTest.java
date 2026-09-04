package com.ebremer.beakgraph.hdf5.jena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Var;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression test for BG-332: {@link SolverLibBeak#convert} short-circuited on
 * any {@link BindingBG} and reused its id layer verbatim. Term ids are
 * dictionary ranks, so when two independent stores are named graphs of one
 * Jena dataset (the stage generator is global), a row produced by store A
 * carried A's ranks into store B's iterators, which consume bound ids
 * directly: the join silently matched unrelated terms or nothing at all.
 * The fast path is now taken only when the binding's store shares the reader
 * being solved.
 */
class SolverLibBeakCrossStoreTest {

    private static final String EX = "http://ex.org/";
    private static final int N = 40;
    private static final Node A = NodeFactory.createURI("urn:store:a");
    private static final Node B = NodeFactory.createURI("urn:store:b");

    @TempDir
    static Path dir;
    private static BeakGraph graphA;
    private static BeakGraph graphB;
    private static Dataset linked;
    private static Dataset reference;

    private static BeakGraph build(String name, String ttl) throws Exception {
        File src = dir.resolve(name + ".ttl").toFile();
        Files.writeString(src.toPath(), ttl, StandardCharsets.UTF_8);
        File h5 = dir.resolve(name + ".h5").toFile();
        HDF5Writer.Builder().setSource(src).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        return new BeakGraph(new HDF5Reader(h5));
    }

    private static Model parse(String ttl) {
        Model m = ModelFactory.createDefaultModel();
        RDFDataMgr.read(m, new StringReader(ttl), null, Lang.TURTLE);
        return m;
    }

    @BeforeAll
    static void build() throws Exception {
        // A: s_i -p-> o_i. B: o_i -q-> "z_i", plus padding terms that sort
        // before "o" so B's ranks for the shared o_i terms differ from A's.
        StringBuilder a = new StringBuilder("@prefix ex: <" + EX + "> .\n");
        StringBuilder b = new StringBuilder("@prefix ex: <" + EX + "> .\n");
        for (int i = 0; i < N; i++) {
            a.append("ex:s").append(i).append(" ex:p ex:o").append(i).append(" .\n");
            b.append("ex:o").append(i).append(" ex:q \"z").append(i).append("\" .\n");
        }
        for (int i = 0; i < 3 * N; i++) {
            b.append("ex:a").append(i).append(" ex:q \"pad").append(i).append("\" .\n");
        }
        graphA = build("a", a.toString());
        graphB = build("b", b.toString());

        DatasetGraph dsg = DatasetGraphFactory.createGeneral();
        dsg.addGraph(A, graphA);
        dsg.addGraph(B, graphB);
        linked = DatasetFactory.wrap(dsg);

        reference = DatasetFactory.create();
        reference.addNamedModel(A.getURI(), parse(a.toString()));
        reference.addNamedModel(B.getURI(), parse(b.toString()));
    }

    @AfterAll
    static void close() {
        if (graphA != null) graphA.close();
        if (graphB != null) graphB.close();
    }

    private static List<String> rows(Dataset d, String query) {
        List<String> out = new ArrayList<>();
        try (QueryExecution qe = QueryExecution.dataset(d).query(QueryFactory.create(query)).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                QuerySolution qs = rs.next();
                StringBuilder sb = new StringBuilder();
                rs.getResultVars().forEach(v -> sb.append(v).append('=').append(qs.get(v)).append(' '));
                out.add(sb.toString());
            }
        }
        Collections.sort(out);
        return out;
    }

    @Test
    void theSharedTermsHaveDifferentRanksInTheTwoStores() {
        // Precondition for the test to mean anything: reusing A's id in B must
        // actually name a different term.
        Node o = NodeFactory.createURI(EX + "o7");
        long inA = graphA.getReader().getNodeTable().getNodeIdForNode(o);
        long inB = graphB.getReader().getNodeTable().getNodeIdForNode(o);
        assertFalse(NodeId.isDoesNotExist(inA));
        assertFalse(NodeId.isDoesNotExist(inB));
        assertNotEquals(inA, inB, "stores were built so that ranks differ");
    }

    @Test
    void aRowFromAnotherStoreIsReconvertedThroughItsTerms() {
        Var o = Var.alloc("o");
        Node term = NodeFactory.createURI(EX + "o7");
        BindingNodeId fromA = new BindingNodeId();
        fromA.put(o, graphA.getReader().getNodeTable().getNodeIdForNode(term));
        BindingBG rowA = new BindingBG(fromA, graphA);

        BindingNodeId forB = SolverLibBeak.convert(rowA, graphB);
        assertNotSame(fromA, forB, "another store's id layer must not be reused");
        assertEquals(graphB.getReader().getNodeTable().getNodeIdForNode(term), forB.get(o),
                "the id must be B's rank of the same term");
    }

    @Test
    void aRowFromAViewOverTheSameReaderKeepsTheFastPath() {
        Var o = Var.alloc("o");
        BindingNodeId fromA = new BindingNodeId();
        fromA.put(o, graphA.getReader().getNodeTable().getNodeIdForNode(NodeFactory.createURI(EX + "o7")));
        BindingBG rowA = new BindingBG(fromA, graphA);

        // BGDatasetGraph.getGraph mints a fresh BeakGraph per call over the
        // same reader; same-store GRAPH joins must stay on the id fast path.
        BeakGraph view = new BeakGraph(NodeFactory.createURI("urn:view"), graphA.getReader());
        assertSame(fromA, SolverLibBeak.convert(rowA, view));
        assertSame(fromA, SolverLibBeak.convert(rowA, graphA));
    }

    @Test
    void crossStoreJoinsMatchJena() {
        String prefix = "PREFIX ex: <" + EX + "> ";
        String[] queries = {
            prefix + "SELECT ?s ?o ?z WHERE { GRAPH <urn:store:a> { ?s ex:p ?o } GRAPH <urn:store:b> { ?o ex:q ?z } }",
            prefix + "SELECT ?s ?o ?z WHERE { GRAPH <urn:store:b> { ?o ex:q ?z } GRAPH <urn:store:a> { ?s ex:p ?o } }",
            prefix + "SELECT ?o ?z WHERE { GRAPH <urn:store:a> { ex:s5 ex:p ?o } GRAPH <urn:store:b> { ?o ex:q ?z } }",
            prefix + "SELECT ?s ?o ?z WHERE { GRAPH <urn:store:a> { ?s ex:p ?o } OPTIONAL { GRAPH <urn:store:b> { ?o ex:q ?z } } }",
            prefix + "SELECT ?o ?z WHERE { VALUES ?o { ex:o3 ex:o33 } GRAPH <urn:store:a> { ?s ex:p ?o } GRAPH <urn:store:b> { ?o ex:q ?z } }",
        };
        for (String q : queries) {
            List<String> expected = rows(reference, q);
            assertFalse(expected.isEmpty(), "reference must have answers for " + q);
            assertEquals(expected, rows(linked, q), q);
        }
    }
}
