package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.jena.DistinctTermFastPath;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.query.ResultSetRewindable;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.resultset.ResultsCompare;
import org.apache.jena.query.ResultSetFormatter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.io.TempDir;

/**
 * BG-333: every algebra shape that leaves OpExecutorBG's overrides (OPTIONAL,
 * MINUS, EXISTS, property paths, FROM / FROM NAMED, sub-SELECT with
 * ORDER/LIMIT/OFFSET, DESCRIBE) is executed by Jena's defaults over BeakGraph
 * views - re-entering execute(OpBGP) with non-root inputs, BindingBG parent
 * chains, Graph.find for every path step, dynamic datasets, DESCRIBE's
 * internal GRAPH ?g query against the DISTINCT fast-path guard. None of it
 * had a test. Each query runs against a store and against Jena's in-memory
 * dataset loaded from the same TriG, in four contexts: the default graph,
 * {@code GRAPH <g1>}, {@code GRAPH ?g} and {@code FROM <g1>}; results are
 * compared by term with blank-node isomorphism. Parameterized over the
 * in-memory writer engines.
 */
class AlgebraShapeParityTest {

    private static final String PREFIX = "PREFIX ex: <http://ex.org/> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ";
    private static final String G1 = "http://ex.org/g1";
    private static final String TRIG = """
        @prefix ex: <http://ex.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        ex:a ex:p ex:b . ex:b ex:p ex:c . ex:c ex:p ex:d .
        ex:a ex:q ex:x . ex:x ex:q ex:y . ex:y ex:p ex:a .
        ex:a ex:name "Alice" . ex:b ex:name "Bob" . ex:c ex:name "Carol" . ex:d ex:name "Dan" .
        ex:a ex:age 30 . ex:b ex:age 7 . ex:c ex:age 45 . ex:x ex:age 12 .
        ex:a ex:knows _:k1 . _:k1 ex:name "Anon" . _:k1 ex:knows _:k2 . _:k2 ex:name "Deep" . _:k2 ex:knows ex:b .
        ex:a ex:says <<( ex:b ex:p ex:c )>> .
        ex:c ex:says <<( ex:b ex:p ex:c )>> .
        ex:l1 ex:v "x" . ex:l2 ex:v "x" . ex:l3 ex:v "y" . ex:l1 ex:w "x" .
        ex:g1 {
            ex:a ex:p ex:b .
            ex:a ex:only "g1" .
            ex:d ex:p ex:e . ex:e ex:p ex:f . ex:e ex:name "Eve" . ex:e ex:age 3 .
            ex:a ex:name "Alice" .
            ex:e ex:knows _:g . _:g ex:name "GhostOfG1" .
        }
        ex:g2 {
            ex:a ex:p ex:b .
            ex:b ex:only "g2" .
            ex:b ex:age 8 .
            ex:b ex:name "Bob" .
        }
        """;

    @TempDir
    static Path dir;
    static final Map<String, BeakGraph> graphs = new LinkedHashMap<>();
    static final Map<String, Dataset> stores = new LinkedHashMap<>();
    static Dataset reference;

    static Stream<String> engines() {
        return WriterEngines.all().filter(e -> !e.needsNative()).map(WriterEngines.Engine::name);
    }

    @BeforeAll
    static void build() throws Exception {
        Path src = dir.resolve("shapes.trig");
        Files.writeString(src, TRIG);
        DatasetGraph dsg = DatasetGraphFactory.create();
        RDFParser.create().source(src.toUri().toString()).lang(Lang.TRIG).parse(dsg);
        reference = DatasetFactory.wrap(dsg);
        for (WriterEngines.Engine e : WriterEngines.all().filter(x -> !x.needsNative()).toList()) {
            File h5 = dir.resolve(e.name() + ".h5").toFile();
            e.buildStore(src.toFile(), h5);
            BeakGraph bg = BG.getBeakGraph(h5);
            graphs.put(e.name(), bg);
            stores.put(e.name(), bg.getDataset());
        }
    }

    @AfterAll
    static void close() {
        graphs.values().forEach(BeakGraph::close);
    }

    // --- comparison -------------------------------------------------------

    private static ResultSetRewindable select(Dataset d, Query q) {
        try (QueryExecution qe = QueryExecution.dataset(d).query(q).build()) {
            return ResultSetFactory.makeRewindable(qe.execSelect());
        }
    }

    private static void assertSelectParity(Dataset store, String query) {
        Query q = QueryFactory.create(PREFIX + query);
        ResultSetRewindable expected = select(reference, q);
        ResultSetRewindable actual = select(store, q);
        boolean same = ResultsCompare.equalsByTerm(expected, actual);
        if (!same) {
            expected.reset();
            actual.reset();
            assertEquals(ResultSetFormatter.asText(expected), ResultSetFormatter.asText(actual), query);
            assertTrue(false, "result sets differ (by term, blank-node isomorphic): " + query);
        }
    }

    private static void assertModelParity(Dataset store, String query) {
        Query q = QueryFactory.create(PREFIX + query);
        try (QueryExecution a = QueryExecution.dataset(reference).query(q).build();
             QueryExecution b = QueryExecution.dataset(store).query(q).build()) {
            Model expected = q.isDescribeType() ? a.execDescribe() : a.execConstruct();
            Model actual = q.isDescribeType() ? b.execDescribe() : b.execConstruct();
            assertTrue(expected.isIsomorphicWith(actual), () -> query + "\nexpected:\n" + dump(expected) + "\nactual:\n" + dump(actual));
        }
    }

    private static String dump(Model m) {
        java.io.StringWriter w = new java.io.StringWriter();
        m.write(w, "N-TRIPLES");
        return w.toString();
    }

    private static boolean ask(Dataset d, String query) {
        try (QueryExecution qe = QueryExecution.dataset(d).query(QueryFactory.create(PREFIX + query)).build()) {
            return qe.execAsk();
        }
    }

    private static int count(Dataset d, String query) {
        ResultSetRewindable rs = select(d, QueryFactory.create(PREFIX + query));
        return rs.size();
    }

    /** The four execution contexts a group pattern runs in. */
    private static List<String> contexts(String body) {
        return List.of(
                "SELECT * WHERE { " + body + " }",
                "SELECT * WHERE { GRAPH <" + G1 + "> { " + body + " } }",
                "SELECT * WHERE { GRAPH ?g { " + body + " } }",
                "SELECT * FROM <" + G1 + "> WHERE { " + body + " }");
    }

    private static void assertBodiesInEveryContext(Dataset store, String... bodies) {
        for (String body : bodies) {
            for (String q : contexts(body)) {
                assertSelectParity(store, q);
            }
        }
    }

    // --- (1) OPTIONAL ------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("engines")
    void optional(String engine) {
        Dataset store = stores.get(engine);
        assertBodiesInEveryContext(store,
                "?s ex:p ?o OPTIONAL { ?o ex:name ?z }",
                "?s ex:p ?o OPTIONAL { ?o ex:name ?z FILTER(?z != \"Bob\") }",
                "?s ex:p ?o OPTIONAL { ?o ex:p ?z FILTER(?z != ?s) }",
                "?s ex:name ?n OPTIONAL { ?x ex:only ?y }",
                "?s ex:p ?o OPTIONAL { ?o ex:age ?a } OPTIONAL { ?o ex:name ?n }",
                "?s ex:p ?o OPTIONAL { ?o ex:p ?t OPTIONAL { ?t ex:name ?tn } }",
                "?s ex:knows ?k OPTIONAL { ?k ex:knows ?k2 . ?k2 ex:name ?n }",
                "?s ex:says ?tt OPTIONAL { ?tt ex:name ?n }",
                "?s ex:v ?x OPTIONAL { ?t ex:w ?x }");
        assertFalse(select(store, QueryFactory.create(PREFIX + contexts("?s ex:p ?o OPTIONAL { ?o ex:name ?z }").get(0))).size() == 0);
    }

    // --- (2) MINUS ---------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("engines")
    void minus(String engine) {
        Dataset store = stores.get(engine);
        assertBodiesInEveryContext(store,
                "?s ex:p ?o MINUS { ?s ex:age ?a }",
                "?s ex:p ?o MINUS { ?o ex:age ?a }",
                "?s ex:p ?o MINUS { ?x ex:age ?y }",
                "?s ex:p ?o MINUS { ?x ex:nowhere ?y }",
                "?s ex:p ?o OPTIONAL { ?o ex:age ?a } MINUS { ?x ex:age ?a }",
                "?s ex:name ?n MINUS { ?s ex:p ?o . ?o ex:age ?a }",
                "?s ex:v ?x MINUS { ?s ex:w ?x }");
    }

    // --- (3) EXISTS / NOT EXISTS ------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("engines")
    void exists(String engine) {
        Dataset store = stores.get(engine);
        assertBodiesInEveryContext(store,
                "?s ex:p ?o FILTER EXISTS { ?s ex:name ?n }",
                "?s ex:p ?o FILTER NOT EXISTS { ?o ex:p ?z }",
                "?s ex:p ?o FILTER EXISTS { ?x ex:only ?y }",
                "?s ex:p ?o FILTER NOT EXISTS { ?x ex:nowhere ?y }",
                "VALUES ?o { ex:nowhere } ?s ex:p ?o",
                "VALUES ?o { ex:b ex:nowhere } ?s ex:p ?o",
                "?s ex:p ?o FILTER NOT EXISTS { VALUES ?q { ex:nowhere } ?o ex:p ?q }",
                "?s ex:age ?o FILTER(?o > 5 && NOT EXISTS { ?s ex:only ?w })",
                "?s ex:age ?o FILTER(?o > 5 && EXISTS { ?s ex:p ?w })",
                "?s ex:name ?n FILTER EXISTS { ?s ex:knows/ex:name ?k }");
    }

    // --- (4) property paths -----------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("engines")
    void propertyPaths(String engine) {
        Dataset store = stores.get(engine);
        assertBodiesInEveryContext(store,
                "?s ex:p+ ?o",
                "?s ex:p* ?o",
                "?s ex:p/ex:name ?o",
                "?s ^ex:p ?o",
                "?s !(ex:p) ?o",
                "?s !(ex:p|ex:name) ?o",
                "?s ex:p? ?o",
                "ex:a ex:p+ ?o",
                "ex:a ex:p* ?o",
                "?s ex:p+ ex:d",
                "?s ex:p* ex:d",
                "ex:a ex:p* ex:a",
                "ex:a ex:p+ ex:a",
                "?s (ex:p|ex:q)+ ?o",
                "?s (ex:p|ex:q)* ex:y",
                "?s ex:knows+/ex:name ?o",
                "?s ex:knows/ex:knows/ex:knows ?o",
                "?s ex:says/ex:p ?o",
                "?s ex:name/ex:p ?o",
                "?s ^ex:knows/ex:name ?o",
                "?s ex:p/^ex:p ?o");
        assertEquals(count(reference, "SELECT ?s WHERE { ?s ex:p+ ex:d }"), count(store, "SELECT ?s WHERE { ?s ex:p+ ex:d }"));
        assertTrue(count(store, "SELECT ?s WHERE { ?s ex:p+ ex:d }") >= 4, "fixture sanity: the chain a-b-c-d and the cycle y-a reach d");
    }

    // --- (5) FROM / FROM NAMED --------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("engines")
    void datasetClauses(String engine) {
        Dataset store = stores.get(engine);
        String[] queries = {
            "SELECT * FROM <" + G1 + "> WHERE { ?s ?p ?o }",
            "SELECT * FROM <" + G1 + "> FROM <http://ex.org/g2> WHERE { ?s ?p ?o }",
            "SELECT (COUNT(*) AS ?n) FROM <" + G1 + "> FROM <http://ex.org/g2> WHERE { ?s ?p ?o }",
            "SELECT DISTINCT ?g FROM NAMED <" + G1 + "> WHERE { GRAPH ?g { ?s ?p ?o } }",
            "SELECT * FROM NAMED <" + G1 + "> FROM NAMED <http://ex.org/g2> WHERE { GRAPH ?g { ?s ex:only ?o } }",
            "SELECT * FROM <urn:x-arq:UnionGraph> WHERE { ?s ex:only ?o }",
            "SELECT * FROM <urn:x-arq:UnionGraph> WHERE { ?s ex:p ?o }",
            "SELECT * FROM NAMED <http://ex.org/missing> WHERE { GRAPH ?g { ?s ?p ?o } }",
            "SELECT * FROM <http://ex.org/missing> WHERE { ?s ?p ?o }",
            "SELECT * FROM <" + G1 + "> FROM NAMED <http://ex.org/g2> WHERE { ?s ex:p ?o . GRAPH ?g { ?s ex:only ?x } }",
            "SELECT * FROM <" + G1 + "> WHERE { ?s ex:p+ ?o }",
            "SELECT * FROM <" + G1 + "> FROM <http://ex.org/g2> WHERE { ?s ex:p ?o OPTIONAL { ?o ex:only ?x } MINUS { ?s ex:only \"g1\" } }",
            "SELECT * FROM <" + G1 + "> WHERE { ?s ex:knows/ex:name ?n }",
            "SELECT * FROM <urn:x-arq:DefaultGraph> FROM <" + G1 + "> WHERE { ?s ex:name ?n }",
        };
        for (String q : queries) {
            assertSelectParity(store, q);
        }
        ResultSetRewindable named = select(store, QueryFactory.create(PREFIX + "SELECT DISTINCT ?g FROM NAMED <" + G1 + "> WHERE { GRAPH ?g { ?s ?p ?o } }"));
        assertEquals(1, named.size());
        assertEquals(G1, named.next().getResource("g").getURI(), "only the FROM NAMED graph is listed - never an internal graph");
        assertFalse(ask(store, "ASK FROM <" + G1 + "> { ex:a ex:q ex:x }"), "the default graph is not part of a FROM dataset");
    }

    // --- (6) sub-SELECT ------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("engines")
    void subSelects(String engine) {
        Dataset store = stores.get(engine);
        assertBodiesInEveryContext(store,
                "{ SELECT ?s WHERE { ?s ex:age ?a } ORDER BY ?a LIMIT 2 OFFSET 1 } ?s ex:name ?n",
                "{ SELECT ?s ?a WHERE { ?s ex:age ?a } ORDER BY DESC(?a) LIMIT 3 } ?s ex:p ?o",
                "?s ex:p ?o MINUS { SELECT DISTINCT ?s WHERE { ?s ex:age ?a } }",
                "{ SELECT ?s (COUNT(?o) AS ?c) WHERE { ?s ex:p ?o } GROUP BY ?s } ?s ex:name ?n",
                "{ SELECT DISTINCT ?o WHERE { ?s ex:p ?o } } ?o ex:name ?n",
                "{ SELECT ?s WHERE { ?s ex:p ?o } ORDER BY ?s LIMIT 1 } ?s ex:p+ ?t");
        // A sub-SELECT is evaluated on its own and then joined (SPARQL sub-query
        // semantics), so Jena hands the left side of the join the root input and
        // the whole-graph DISTINCT / COUNT shortcuts may legitimately answer it;
        // what matters is that the joined result is Jena's, in every position.
        assertSelectParity(store, "SELECT * WHERE { { SELECT DISTINCT ?s WHERE { ?s ?p ?o } } ?s ex:name ?n }");
        assertSelectParity(store, "SELECT * WHERE { { SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o } } ?x ex:only ?y }");
        assertSelectParity(store, "SELECT * WHERE { ?s ex:name ?n { SELECT DISTINCT ?p WHERE { ?s ?p ?o } } }");
        assertSelectParity(store, "SELECT * WHERE { ?s ex:name ?n { SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o } } }");
        assertSelectParity(store, "SELECT * WHERE { VALUES ?s { ex:a } { SELECT DISTINCT ?s WHERE { ?s ?p ?o } } }");
    }

    // --- (7) DESCRIBE / CONSTRUCT -------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("engines")
    void describe(String engine) {
        Dataset store = stores.get(engine);
        long distinct = DistinctTermFastPath.HITS.get();
        String[] queries = {
            "DESCRIBE ex:a",
            "DESCRIBE ex:e",
            "DESCRIBE ex:b ex:c",
            "DESCRIBE ?x WHERE { GRAPH ?g { ?x ex:only ?v } }",
            "DESCRIBE ?x WHERE { ?x ex:age ?a FILTER(?a > 10) }",
            "DESCRIBE ?x FROM <" + G1 + "> WHERE { ?x ex:p ?o }",
            "DESCRIBE ex:nowhere",
            "CONSTRUCT { ?s ex:reach ?o } WHERE { ?s ex:p+ ?o }",
            "CONSTRUCT { ?s ex:n ?n } WHERE { ?s ex:p ?o OPTIONAL { ?o ex:name ?n } }",
            "CONSTRUCT { ?s ?p ?o } FROM <" + G1 + "> FROM <http://ex.org/g2> WHERE { ?s ?p ?o }",
        };
        for (String q : queries) {
            assertModelParity(store, q);
        }
        assertEquals(distinct, DistinctTermFastPath.HITS.get(),
                "DESCRIBE's internal SELECT DISTINCT ?g { GRAPH ?g { ... } } must stay off the DISTINCT fast path");
        try (QueryExecution qe = QueryExecution.dataset(store).query(QueryFactory.create(PREFIX + "DESCRIBE ex:a")).build()) {
            Model m = qe.execDescribe();
            assertTrue(m.contains(m.createResource("http://ex.org/a"), m.createProperty("http://ex.org/knows")), "DESCRIBE reaches the bnode closure");
            assertTrue(m.listStatements(null, m.createProperty("http://ex.org/name"), "Deep").hasNext(),
                    "the bnode chain is followed two levels deep");
        }
    }

    // --- (8) mixtures --------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("engines")
    void mixedShapes(String engine) {
        Dataset store = stores.get(engine);
        assertBodiesInEveryContext(store,
                "{ ?s ex:p ?o } UNION { ?s ex:q ?o } OPTIONAL { ?o ex:name ?n } FILTER NOT EXISTS { ?o ex:only ?x }",
                "?s ex:p+ ?o OPTIONAL { ?o ex:age ?a } FILTER(!BOUND(?a) || ?a > 10)",
                "?s ex:name ?n MINUS { ?s ex:p+ ex:d } FILTER EXISTS { ?s ex:age ?a }",
                "?s ex:p ?o BIND(STR(?o) AS ?str) OPTIONAL { ?o ex:name ?n } FILTER(STRLEN(?str) > 0)",
                "?s (ex:p|ex:knows)+ ?o MINUS { ?o ex:name ?n }",
                "?s ex:says <<( ?x ex:p ?y )>> OPTIONAL { ?y ex:name ?n } FILTER EXISTS { ?x ex:p ?y }");
        ResultSetRewindable a = select(store, QueryFactory.create(PREFIX + "SELECT ?s ?n WHERE { ?s ex:p+ ?o OPTIONAL { ?o ex:name ?n } } ORDER BY ?s ?n"));
        ResultSetRewindable b = select(reference, QueryFactory.create(PREFIX + "SELECT ?s ?n WHERE { ?s ex:p+ ?o OPTIONAL { ?o ex:name ?n } } ORDER BY ?s ?n"));
        assertTrue(ResultsCompare.equalsByTermAndOrder(b, a), "ORDER BY over a path + OPTIONAL");
        List<String> vars = new ArrayList<>(a.getResultVars());
        assertEquals(List.of("s", "n"), vars);
    }
}
