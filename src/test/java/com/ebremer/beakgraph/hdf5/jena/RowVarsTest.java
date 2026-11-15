package com.ebremer.beakgraph.hdf5.jena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Var;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A row binds a variable without looking for it first when the iterator has
 * established, once, that the variable is new to every row it makes
 * ({@link RowVars}, {@link BindingNodeId#bind}). What that must not cost: a
 * variable that is not new - held by the parent binding, or repeated in the
 * pattern - still has its two values compared, and a row with two terms for
 * one variable is still no row.
 */
class RowVarsTest {

    private static final Var S = Var.alloc("s");
    private static final Var P = Var.alloc("p");
    private static final Var O = Var.alloc("o");
    private static final Var X = Var.alloc("x");

    private static BindingNodeId chainHolding(Var v, long id) {
        BindingNodeId holder = new BindingNodeId(new BindingNodeId());
        holder.bind(v, id);
        return new BindingNodeId(holder); // the variable is held two layers down
    }

    @Test
    void aVariableIsFreshWhenNoLayerHoldsItAndNoEarlierPositionIsIt() {
        BindingNodeId parent = chainHolding(X, NodeId.pack(NodeType.SUBJECT, 7));
        assertTrue(RowVars.fresh(null, S), "no parent at all");
        assertTrue(RowVars.fresh(new BindingNodeId(), S));
        assertTrue(RowVars.fresh(parent, S, P, O));
        assertTrue(RowVars.fresh(parent, S, null, P), "a position that binds nothing is no earlier variable");
        assertFalse(RowVars.fresh(parent, X), "held by a layer of the parent");
        assertFalse(RowVars.fresh(parent, S, P, Var.alloc("s")), "an earlier position's variable again, whatever object names it");
        assertFalse(RowVars.fresh(parent, null), "a position that is no variable");
    }

    @Test
    void distinctIsPairwise() {
        assertTrue(RowVars.distinct(S, P, O, X));
        assertTrue(RowVars.distinct(null, S, null, O));
        assertTrue(RowVars.distinct());
        assertFalse(RowVars.distinct(S, P, Var.alloc("s")));
        assertFalse(RowVars.distinct(null, O, null, Var.alloc("o")));
    }

    @Test
    void theGraphIsHeldOnlyByAnEntityIdOfThatVeryGraph() {
        assertTrue(RowVars.holds(chainHolding(X, NodeId.pack(NodeType.GRAPH, 7)), X, 7));
        assertTrue(RowVars.holds(chainHolding(X, NodeId.pack(NodeType.SUBJECT, 7)), X, 7), "graph ids are entity ids, whatever position bound it");
        assertFalse(RowVars.holds(chainHolding(X, NodeId.pack(NodeType.GRAPH, 8)), X, 7), "another graph");
        assertFalse(RowVars.holds(chainHolding(X, NodeId.pack(NodeType.PREDICATE, 7)), X, 7), "a predicate's id says nothing about a graph");
        assertFalse(RowVars.holds(chainHolding(X, NodeId.DOES_NOT_EXIST), X, 7));
        assertFalse(RowVars.holds(chainHolding(S, NodeId.pack(NodeType.GRAPH, 7)), X, 7), "not bound");
        assertFalse(RowVars.holds(null, X, 7));
        assertFalse(RowVars.holds(chainHolding(X, NodeId.pack(NodeType.GRAPH, 7)), null, 7));
    }

    @Test
    void bindAddsWhatGetFinds() {
        BindingNodeId row = new BindingNodeId(chainHolding(X, NodeId.pack(NodeType.SUBJECT, 7)));
        row.bind(S, NodeId.pack(NodeType.SUBJECT, 1));
        row.bind(P, NodeId.pack(NodeType.PREDICATE, 2));
        row.bind(O, NodeId.pack(NodeType.OBJECT, 3));
        assertEquals(NodeId.pack(NodeType.SUBJECT, 1), row.get(S));
        assertEquals(NodeId.pack(NodeType.PREDICATE, 2), row.get(P));
        assertEquals(NodeId.pack(NodeType.OBJECT, 3), row.get(O));
        assertEquals(NodeId.pack(NodeType.SUBJECT, 7), row.get(X), "the chain below is still there");
        row.bind(Var.alloc("g"), NodeId.pack(NodeType.GRAPH, 4));
        assertTrue(row.isFull());
        assertThrows(IllegalStateException.class, () -> row.bind(Var.alloc("fifth"), 5), "a layer holds four");
    }

    // --- the rows themselves ------------------------------------------------

    /**
     * Terms in two roles, and triples that repeat a term: the shapes in which
     * a variable of a pattern is not new to its row.
     */
    private static final String TRIG = """
        @prefix ex: <http://ex.org/> .
        ex:e ex:refl ex:e . ex:e ex:knows ex:f . ex:f ex:knows ex:e . ex:f ex:refl ex:g .
        ex:same ex:same ex:o1 . ex:s1 ex:both ex:both . ex:all ex:all ex:all .
        ex:a ex:knows ex:e . ex:a ex:knows ex:f . ex:a ex:refl ex:a . ex:b ex:knows ex:a .
        ex:g1 { ex:e ex:refl ex:e . ex:e ex:knows ex:f . ex:g1 ex:refl ex:g1 . ex:same ex:same ex:o1 . ex:x ex:knows ex:x . }
        ex:g2 { ex:f ex:refl ex:f . ex:f ex:knows ex:e . ex:all ex:all ex:all . ex:s1 ex:both ex:both . }
        """;

    private static final String[] QUERIES = {
        // a variable twice in one pattern
        "SELECT * WHERE { ?x ?p ?x }",
        "SELECT * WHERE { ?x ex:refl ?x }",
        "SELECT * WHERE { ?x ex:knows ?x }",
        "SELECT * WHERE { ?x ?x ?o }",
        "SELECT * WHERE { ?s ?x ?x }",
        "SELECT * WHERE { ?x ?x ?x }",
        "SELECT * WHERE { ex:e ?p ?p }",
        "SELECT * WHERE { ?x ?p ex:e . ?x ?q ?x }",
        // a variable the rows before already hold: in every position, and twice
        "SELECT * WHERE { ?a ex:knows ?x . ?x ?p ?x }",
        "SELECT * WHERE { ?a ex:knows ?x . ?x ex:refl ?x }",
        "SELECT * WHERE { ?a ex:knows ?x . ?x ex:knows ?a }",
        "SELECT * WHERE { ?a ?p ?b . ?b ?p ?c }",
        "SELECT * WHERE { ?a ?p ?b . ?b ?q ?a }",
        "SELECT * WHERE { ?s ?p ?o . ?s ?p ?o }",
        "SELECT * WHERE { ?s ?p ?o . ?o ?p ?s }",
        "SELECT * WHERE { ?s ?p ?o . ?p ?p ?o }",
        "SELECT * WHERE { ?s ?p ?o . ?x ?s ?y }",
        "SELECT * WHERE { VALUES ?x { ex:e ex:all ex:nowhere } ?x ?p ?x }",
        "SELECT * WHERE { VALUES ?p { ex:refl ex:all } ?s ?p ?s }",
        "SELECT * WHERE { VALUES (?s ?o) { (ex:e ex:e) (ex:e ex:f) (ex:f ex:e) } ?s ?p ?o }",
        // in a named graph, under a variable graph, and with the graph's variable in the pattern
        "SELECT * WHERE { GRAPH ex:g1 { ?x ?p ?x } }",
        "SELECT * WHERE { GRAPH ex:g2 { ?x ?x ?x } }",
        "SELECT * WHERE { GRAPH ?g { ?x ?p ?x } }",
        "SELECT * WHERE { GRAPH ?g { ?x ?x ?o } }",
        "SELECT * WHERE { GRAPH ?g { ?s ?x ?x } }",
        "SELECT * WHERE { GRAPH ?g { ?g ?p ?g } }",
        "SELECT * WHERE { GRAPH ?g { ?a ex:knows ?x . ?x ?p ?x } }",
        "SELECT * WHERE { GRAPH ?g { ?a ?p ?b . ?b ?p ?a } }",
        "SELECT * WHERE { ?x ex:refl ?x . GRAPH ?g { ?x ?p ?x } }",
        "SELECT * WHERE { GRAPH ?g { ?x ?p ?x } GRAPH ?h { ?x ?q ?x } }",
    };

    private static List<String> rows(Dataset dataset, String query) {
        List<String> out = new ArrayList<>();
        try (QueryExecution qe = QueryExecution.dataset(dataset).query(QueryFactory.create("PREFIX ex: <http://ex.org/>\n" + query)).build()) {
            ResultSet rs = qe.execSelect();
            List<String> vars = new ArrayList<>(rs.getResultVars());
            Collections.sort(vars);
            while (rs.hasNext()) {
                QuerySolution row = rs.next();
                StringBuilder sb = new StringBuilder();
                for (String v : vars) {
                    if (row.contains(v)) {
                        sb.append(v).append('=').append(row.get(v).asNode()).append(' ');
                    }
                }
                out.add(sb.toString());
            }
        }
        Collections.sort(out);
        return out;
    }

    @Test
    void aRowWithTwoTermsForOneVariableIsStillNoRow(@TempDir Path dir) throws Exception {
        Path src = dir.resolve("repeat.trig");
        Files.writeString(src, TRIG);
        File h5 = dir.resolve("repeat.trig.h5").toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        DatasetGraph dsg = DatasetGraphFactory.create();
        RDFParser.create().source(src.toUri().toString()).lang(Lang.TRIG).parse(dsg);
        Dataset reference = DatasetFactory.wrap(dsg);
        try (BeakGraph bg = new BeakGraph(new HDF5Reader(h5))) {
            Dataset store = bg.getDataset();
            int withRows = 0;
            for (String query : QUERIES) {
                List<String> expected = rows(reference, query);
                assertEquals(expected, rows(store, query), query);
                withRows += expected.isEmpty() ? 0 : 1;
            }
            assertTrue(withRows >= QUERIES.length - 3, "the shapes are held to data that has them: " + withRows + " of " + QUERIES.length);
        }
    }
}
