package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.VoidMode;
import com.ebremer.beakgraph.hdf5.jena.GraphVarBG;
import com.ebremer.beakgraph.hdf5.jena.ParallelScan;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

/**
 * {@code GRAPH ?g { BGP }} answered by one id-level walk over the named
 * graphs ({@link GraphVarBG}) must give the rows Jena's own operator gives,
 * which executes the pattern anew in every graph: the same graphs, the same
 * rows, the same multiplicities. Each query here runs three ways - the walk,
 * the walk switched off, and plain ARQ over an in-memory dataset parsed from
 * the same source - and the walk's counter says which shapes it took.
 */
// Switches the walk off and lowers the parallel-scan threshold for the JVM: never beside another query test.
@Isolated
class GraphVariableWalkTest {

    private static final String PREFIX = "PREFIX ex: <http://ex.org/>\n";

    /**
     * Three named graphs. Their names are terms too: {@code ex:g1} is a
     * subject and an object in itself, {@code ex:g2} an object in itself,
     * {@code ex:g3} a predicate in itself and in the default graph; and
     * {@code ex:g4} is named in the default graph but is no graph.
     */
    private static final String TRIG = """
        @prefix ex: <http://ex.org/> .
        ex:a ex:name "Alice" . ex:b ex:name "Bob" .
        ex:g1 a ex:Layer . ex:g2 a ex:Layer . ex:g4 a ex:Layer .
        ex:x ex:in ex:g1 . ex:y ex:in ex:g2 . ex:z ex:in ex:g4 .
        ex:a ex:knows ex:b . ex:b ex:knows ex:c .
        ex:q ex:g3 ex:r .
        ex:g1 {
            ex:a ex:p ex:b . ex:a ex:only "g1" . ex:b ex:next ex:c . ex:c ex:next ex:d .
            ex:g1 ex:self ex:g1 . ex:a ex:name "Alice" . ex:e ex:refl ex:e .
        }
        ex:g2 {
            ex:a ex:p ex:b . ex:b ex:only "g2" . ex:b ex:next ex:c . ex:b ex:name "Bob" . ex:m ex:says ex:g2 .
        }
        ex:g3 {
            ex:c ex:p ex:d . ex:s ex:g3 ex:o . ex:c ex:next ex:d . ex:d ex:next ex:a .
        }
        """;

    /** Shapes the walk answers. */
    private static final String[] WALKED = {
        "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }",
        "SELECT * WHERE { GRAPH ?g { ex:a ?p ?o } }",
        "SELECT * WHERE { GRAPH ?g { ?s ex:p ?o } }",
        "SELECT * WHERE { GRAPH ?g { ?s ?p ex:b } }",
        "SELECT * WHERE { GRAPH ?g { ?s ?p \"Alice\" } }",
        "SELECT * WHERE { GRAPH ?g { ex:a ex:p ?o } }",
        "SELECT * WHERE { GRAPH ?g { ?s ex:p ex:b } }",
        "SELECT * WHERE { GRAPH ?g { ex:a ?p ex:b } }",
        "SELECT ?g WHERE { GRAPH ?g { ex:a ex:p ex:b } }",
        // joins inside the graph: every triple in the same graph
        "SELECT * WHERE { GRAPH ?g { ?a ex:next ?b . ?b ex:next ?c } }",
        "SELECT * WHERE { GRAPH ?g { ?a ex:next ?b . ?a ex:name ?n } }",
        "SELECT * WHERE { GRAPH ?g { ?a ex:p ?b . ?b ex:next ?c . ?a ex:only ?v } }",
        // the graph's name as a term of the pattern, in each position
        "SELECT * WHERE { GRAPH ?g { ?g ?p ?o } }",
        "SELECT * WHERE { GRAPH ?g { ?s ?g ?o } }",
        "SELECT * WHERE { GRAPH ?g { ?s ?p ?g } }",
        "SELECT * WHERE { GRAPH ?g { ?g ?p ?g } }",
        "SELECT * WHERE { GRAPH ?g { ?s ?p ?s } }",
        // rows of an outer pattern: joined on a term, and on the graph itself
        "SELECT * WHERE { ?x ex:knows ?s . GRAPH ?g { ?s ?p ?o } }",
        "SELECT * WHERE { ?x ex:in ?g . GRAPH ?g { ?s ex:p ?o } }",
        "SELECT * WHERE { ?g a ex:Layer . GRAPH ?g { ?s ?p ?o } }",
        "SELECT * WHERE { VALUES ?g { ex:g1 ex:nope ex:a } GRAPH ?g { ?s ex:p ?o } }",
        "SELECT * WHERE { VALUES ?g { ex:g2 \"g2\" } GRAPH ?g { ?s ?p ?o } }",
        "SELECT * WHERE { GRAPH ?g { ?s ex:only ?v } GRAPH ?g { ?s ex:p ?o } }",
        "SELECT * WHERE { GRAPH ?g { ?s ex:p ?o } GRAPH ?h { ?s ex:only ?v } }",
        "SELECT * WHERE { GRAPH ex:g1 { ?a ex:p ?b } GRAPH ?g { ?a ex:only ?o } }",
        // under other operators
        "SELECT * WHERE { ex:a ex:name ?n OPTIONAL { GRAPH ?g { ?s ex:name ?n } } }",
        "SELECT * WHERE { ex:a ex:name ?n OPTIONAL { GRAPH ?g { ?s ex:nope ?n } } }",
        "SELECT * WHERE { { GRAPH ?g { ?s ex:only ?o } } UNION { GRAPH ?g { ?s ex:self ?o } } }",
        "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } MINUS { GRAPH ?g { ?s ex:p ?o } } }",
        "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } FILTER(isLiteral(?o)) }",
        "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } FILTER(?g != ex:g2) }",
        "SELECT * WHERE { GRAPH ?g { ?s ex:next ?o } FILTER NOT EXISTS { GRAPH ?g { ?o ex:next ?z } } }",
        "SELECT * WHERE { { SELECT ?g ?s WHERE { GRAPH ?g { ?s ex:next ?o } } } GRAPH ?g { ?s ?p ?v } }",
        // terms the store does not hold
        "SELECT * WHERE { GRAPH ?g { ex:absent ?p ?o } }",
        "SELECT * WHERE { GRAPH ?g { ?s ex:absent ?o } }",
        "SELECT * WHERE { GRAPH ?g { ?s ?p \"nothing\" } }",
        "SELECT * WHERE { GRAPH ?g { ?s ex:p ?o . ?o ex:absent ?z } }",
        // aggregates and DISTINCT whose own fast paths decline: they consume the walk
        "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ?g { ex:a ?p ?o } }",
        "SELECT ?g (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ex:next ?o } } GROUP BY ?g",
        "SELECT ?s (COUNT(DISTINCT ?g) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } } GROUP BY ?s",
        "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } }",
        "SELECT DISTINCT ?p WHERE { GRAPH ?g { ?s ?p ?o } }",
        // a FILTER, an OPTIONAL, a UNION inside the block: evaluated in each graph, with ?g bound but out of their sight
        "SELECT * WHERE { GRAPH ?g { ?s ?p ?o FILTER(isLiteral(?o)) } }",
        "SELECT * WHERE { GRAPH ?g { ex:a ?p ?o FILTER(isLiteral(?o)) } }",
        "SELECT * WHERE { GRAPH ?g { ?s ex:only ?v FILTER(?v > \"g1\") } }",
        "SELECT * WHERE { GRAPH ?g { ?a ex:next ?b . ?b ex:next ?c FILTER(?a != ex:b) FILTER(isIRI(?c)) } }",
        "SELECT * WHERE { GRAPH ?g { ?a ex:next ?b FILTER(?a = ex:b) } }",
        "SELECT * WHERE { GRAPH ?g { ?a ex:next ?b FILTER(?a = ex:b || ?a = ex:c) } }",
        "SELECT * WHERE { GRAPH ?g { ?a ex:next ?b . ?b ex:next ?c FILTER(?a = ex:b) FILTER(?c = ex:d) } }",
        "SELECT * WHERE { GRAPH ?g { ?s ex:only ?v FILTER(?v = \"g1\") } }",
        "SELECT * WHERE { GRAPH ?g { ?s ex:p ?o OPTIONAL { ?o ex:next ?n } } }",
        "SELECT * WHERE { GRAPH ?g { ?s ex:p ?o OPTIONAL { ?s ex:only ?v } OPTIONAL { ?o ex:next ?n } } }",
        "SELECT * WHERE { GRAPH ?g { ?s ex:p ?o OPTIONAL { ?s ex:name ?n FILTER(?n = \"Alice\") } } }",
        "SELECT * WHERE { GRAPH ?g { ?s ex:p ?o OPTIONAL { ?o ex:next ?n . ?n ex:next ?m } } }",
        "SELECT * WHERE { GRAPH ?g { ?s ex:next ?o OPTIONAL { ?s ex:nope ?n } } }",
        "SELECT * WHERE { GRAPH ?g { { ?s ex:only ?o } UNION { ?s ex:self ?o } } }",
        "SELECT * WHERE { GRAPH ?g { { ?s ex:only ?o } UNION { ?s ex:self ?o } UNION { ?s ex:says ?o } } }",
        "SELECT * WHERE { GRAPH ?g { ?s ex:only ?v FILTER(?v >= \"g2\") } }", // a comparison stays a filter, and is evaluated as one
        "SELECT * WHERE { GRAPH ?g { { ?s ex:only ?o FILTER(?o = \"g2\") } UNION { ?s ex:self ?o } } }",
        "SELECT * WHERE { GRAPH ?g { { ?s ex:p ?o } UNION { ?s ex:p ?o } } }",
        "SELECT * WHERE { ?x ex:knows ?s . GRAPH ?g { ?s ex:p ?o FILTER(?x != ex:b) OPTIONAL { ?o ex:next ?n } } }",
        "SELECT * WHERE { ex:a ex:name ?n OPTIONAL { GRAPH ?g { ?s ex:name ?m FILTER(?m = ?n) } } }",
        "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o FILTER(isIRI(?o)) } }",
        "SELECT ?g (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ex:next ?o OPTIONAL { ?o ex:next ?z } } } GROUP BY ?g",
    };

    /** Shapes left to Jena's operator: not a plain BGP under the graph, not a variable graph, or another dataset. */
    private static final String[] LEFT_TO_JENA = {
        // the block mentions ?g and is more than a pattern: binding ?g first would be seen from inside
        "SELECT * WHERE { GRAPH ?g { ?s ?p ?o FILTER(?g = ex:g2) } }",
        "SELECT * WHERE { GRAPH ?g { ?s ?p ?o FILTER(BOUND(?g)) } }",
        "SELECT * WHERE { GRAPH ?g { ?s ?p ?o FILTER(!BOUND(?g)) } }",
        "SELECT * WHERE { GRAPH ?g { ?g ?p ?o FILTER(isIRI(?o)) } }",
        "SELECT * WHERE { GRAPH ?g { ?s ex:p ?o OPTIONAL { ?g ex:self ?x } } }",
        "SELECT * WHERE { GRAPH ?g { { ?s ex:says ?g } UNION { ?s ex:self ?o } } }",
        // operators the walk does not run
        "SELECT * WHERE { GRAPH ?g { ?s ex:next ?o FILTER NOT EXISTS { ?o ex:next ?z } } }",
        "SELECT * WHERE { GRAPH ?g { ?s ex:next ?o FILTER EXISTS { ?o ex:next ?z } } }",
        "SELECT * WHERE { GRAPH ?g { ?s ?p ?o MINUS { ?s ex:p ?o } } }",
        "SELECT * WHERE { GRAPH ?g { ?s ex:only ?v BIND(STR(?v) AS ?t) } }",
        "SELECT * WHERE { GRAPH ?g { { SELECT ?s (COUNT(*) AS ?n) WHERE { ?s ?p ?o } GROUP BY ?s } } }",
        "SELECT * WHERE { GRAPH ?g { ?a ex:next+ ?b } }",
        "SELECT * WHERE { GRAPH ?g { ?s ex:p ?o VALUES ?o { ex:b } } }",
        "SELECT * WHERE { GRAPH ?g { ?s ?p ?o FILTER(false) } }",    // ARQ folds it to an empty table
        "SELECT * WHERE { GRAPH ?g { ?a ex:next ?b FILTER(?a = ex:nowhere) } }", // an assignment of a term the store has no id for
        "SELECT * WHERE { GRAPH ?g { ?s ex:p ?b { ?b ex:next ?o } UNION { ?s ex:only ?o } } }", // a join ARQ does not make a sequence of
        "SELECT * WHERE { GRAPH ?g { ?s ex:p ?o OPTIONAL { { ?s ex:only ?v } UNION { ?o ex:only ?v } } } }", // an optional ARQ keeps a left join
        "SELECT ?g WHERE { GRAPH ?g { } }",
        "SELECT * WHERE { GRAPH ex:g1 { ?s ?p ?o } }",
        // ARQ rewrites an equality on the graph variable into the graph it names: no variable graph is left
        "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } FILTER(?g = ex:g2) }",
        "SELECT * FROM NAMED ex:g1 FROM NAMED ex:g3 WHERE { GRAPH ?g { ?s ?p ?o } }",
        "SELECT * FROM ex:g2 FROM NAMED ex:g1 WHERE { ?s ex:p ?o . GRAPH ?g { ?s ex:only ?v } }",
    };

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset store;
    static Dataset reference;

    @BeforeAll
    static void build() throws Exception {
        Path src = dir.resolve("walk.trig");
        Files.writeString(src, TRIG);
        File h5 = dir.resolve("walk.trig.h5").toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        bg = new BeakGraph(new HDF5Reader(h5));
        store = bg.getDataset();
        DatasetGraph dsg = DatasetGraphFactory.create();
        RDFParser.create().source(src.toUri().toString()).lang(Lang.TRIG).parse(dsg);
        reference = DatasetFactory.wrap(dsg);
    }

    @AfterAll
    static void close() {
        if (bg != null) bg.close();
    }

    /** Every row as its sorted var=term pairs; the rows sorted, so that two bags of rows compare. */
    private static List<String> rows(Dataset dataset, String query) {
        List<String> out = new ArrayList<>();
        try (QueryExecution qe = QueryExecution.dataset(dataset).query(QueryFactory.create(PREFIX + query)).build()) {
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

    /** The store's rows with the walk off: Jena's operator, graph by graph. */
    private static List<String> rowsWithoutTheWalk(Dataset dataset, String query) {
        boolean previous = GraphVarBG.setEnabled(false);
        try {
            long before = GraphVarBG.HITS.get();
            List<String> rows = rows(dataset, query);
            assertEquals(before, GraphVarBG.HITS.get(), "switched off, the walk answers nothing");
            return rows;
        } finally {
            GraphVarBG.setEnabled(previous);
        }
    }

    @Test
    void theWalkGivesTheRowsJenaGives() {
        for (String query : WALKED) {
            long before = GraphVarBG.HITS.get();
            List<String> walked = rows(store, query);
            assertTrue(GraphVarBG.HITS.get() > before, "answered by the walk: " + query);
            assertEquals(rowsWithoutTheWalk(store, query), walked, "the walk against Jena's operator on the same store: " + query);
            assertEquals(rows(reference, query), walked, "the walk against an in-memory dataset: " + query);
        }
    }

    @Test
    void theQueriesReallyHaveRows() {
        // The parity above would hold for two empty answers: these hold the data to the shapes that matter.
        assertEquals(16, rows(store, "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }").size(), "7 + 5 + 4 quads of three graphs, the default graph's 12 left out");
        assertEquals(List.of("g=http://ex.org/g1 ", "g=http://ex.org/g2 "), rows(store, "SELECT ?g WHERE { GRAPH ?g { ex:a ex:p ex:b } }"));
        assertEquals(List.of("g=http://ex.org/g1 o=http://ex.org/g1 p=http://ex.org/self "), rows(store, "SELECT * WHERE { GRAPH ?g { ?g ?p ?o } }"));
        assertEquals(List.of("g=http://ex.org/g3 o=http://ex.org/o s=http://ex.org/s "), rows(store, "SELECT * WHERE { GRAPH ?g { ?s ?g ?o } }"));
        assertEquals(2, rows(store, "SELECT * WHERE { GRAPH ?g { ?s ?p ?g } }").size(), "ex:g1 ex:self ex:g1 and ex:m ex:says ex:g2");
        assertEquals(2, rows(store, "SELECT * WHERE { GRAPH ?g { ?a ex:next ?b . ?b ex:next ?c } }").size(),
                "b-c-d in g1 and c-d-a in g3: g2 holds b-c, but c-d is in other graphs");
        assertEquals(2, rows(store, "SELECT * WHERE { ?x ex:in ?g . GRAPH ?g { ?s ex:p ?o } }").size(), "g1 and g2; ex:g4 is no graph");
        assertEquals(List.of(), rows(store, "SELECT * WHERE { GRAPH ?g { ex:absent ?p ?o } }"));
    }

    @Test
    void otherShapesAreLeftToJenaAndGiveTheSameRows() {
        for (String query : LEFT_TO_JENA) {
            long before = GraphVarBG.HITS.get();
            List<String> rows = rows(store, query);
            assertEquals(before, GraphVarBG.HITS.get(), "not a shape of the walk: " + query);
            assertEquals(rows(reference, query), rows, query);
        }
    }

    /**
     * A filter inside the graph block does not see the graph's variable -
     * SPARQL joins the name to the pattern's solutions afterwards - which is
     * why that shape is not walked with {@code ?g} bound first.
     */
    @Test
    void aFilterInsideTheGraphBlockDoesNotSeeTheGraphVariable() {
        assertEquals(List.of(), rows(store, "SELECT * WHERE { GRAPH ?g { ?s ?p ?o FILTER(?g = ex:g2) } }"));
        assertEquals(5, rows(store, "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } FILTER(?g = ex:g2) }").size());
    }

    @Test
    void aLimitStopsTheWalkEarly() {
        for (int limit : new int[] {1, 3, 8}) {
            assertEquals(limit, rows(store, "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } } LIMIT " + limit).size());
        }
    }

    @Test
    void askAndConstructGoThroughTheWalkToo() {
        long before = GraphVarBG.HITS.get();
        try (QueryExecution qe = QueryExecution.dataset(store).query(QueryFactory.create(PREFIX + "ASK { GRAPH ?g { ex:m ex:says ?g } }")).build()) {
            assertTrue(qe.execAsk());
        }
        try (QueryExecution qe = QueryExecution.dataset(store).query(QueryFactory.create(PREFIX + "ASK { GRAPH ?g { ex:m ex:says ex:g1 } }")).build()) {
            assertFalse(qe.execAsk());
        }
        try (QueryExecution qe = QueryExecution.dataset(store).query(QueryFactory.create(
                PREFIX + "CONSTRUCT { ?s ex:inGraph ?g } WHERE { GRAPH ?g { ?s ex:only ?v } }")).build()) {
            assertEquals(2, qe.execConstruct().size());
        }
        assertEquals(3, GraphVarBG.HITS.get() - before);
    }

    /**
     * The named graphs of the walk are the dataset's: the graphs BeakGraph
     * writes for itself are among them, as they are for Jena's operator, and
     * the default graph is not.
     */
    @Test
    void theWalkVisitsTheGraphsTheDatasetLists() throws Exception {
        Path src = dir.resolve("void.trig");
        Files.writeString(src, TRIG);
        File h5 = dir.resolve("void.trig.h5").toFile();
        HDF5Writer.Builder().setVoidMode(VoidMode.EXACT).setSource(src.toFile()).setDestination(h5)
                .setSpatial(false).setFeatures(false).build().write();
        try (BeakGraph withVoid = new BeakGraph(new HDF5Reader(h5))) {
            Dataset ds = withVoid.getDataset();
            List<String> listed = new ArrayList<>();
            ds.asDatasetGraph().listGraphNodes().forEachRemaining(n -> listed.add("g=" + n + " "));
            Collections.sort(listed);
            assertTrue(listed.size() > 3, "the VoID graph is a named graph of the dataset: " + listed);
            long before = GraphVarBG.HITS.get();
            assertEquals(listed, rows(ds, "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } }"));
            assertTrue(GraphVarBG.HITS.get() > before);
            for (String query : new String[] {
                    "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }",
                    "SELECT * WHERE { GRAPH ?g { ?s a ?t } }",
                    "SELECT * WHERE { GRAPH ?g { ex:a ?p ?o } }"}) {
                assertEquals(rowsWithoutTheWalk(ds, query), rows(ds, query), query);
            }
            // A view of one named graph is a dataset whose default graph is that graph: it is not among its named graphs.
            Dataset view = new BeakGraph(org.apache.jena.graph.NodeFactory.createURI("http://ex.org/g1"), withVoid.getReader(), withVoid).getDataset();
            List<String> ofView = rows(view, "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } }");
            assertFalse(ofView.contains("g=http://ex.org/g1 "), "the view's own graph: " + ofView);
            assertEquals(rowsWithoutTheWalk(view, "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }"), rows(view, "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }"));
        }
    }

    /**
     * A large graph met on the walk is scanned in parallel, as it was when
     * Jena ran the pattern in each graph: for the root binding only, and the
     * scan's workers end with the query, also when a LIMIT ends it early.
     */
    @Test
    void aLargeGraphOnTheWalkIsScannedInParallel() throws Exception {
        String oldThreshold = System.setProperty("beakgraph.scan.parallel.threshold", "64");
        String oldMinChunk = System.setProperty("beakgraph.scan.parallel.minchunk", "64");
        try {
            StringBuilder trig = new StringBuilder("@prefix ex: <http://ex.org/> .\nex:k ex:knows ex:s7 .\nex:small { ex:a ex:value 1 . }\nex:big {\n");
            for (int i = 0; i < 600; i++) {
                trig.append("ex:s").append(i).append(" ex:value ").append(i % 300).append(" ; ex:link ex:s").append((i * 7 + 13) % 600).append(" .\n");
            }
            trig.append("}\nex:other { ex:b ex:value 2 . ex:b ex:link ex:a . }\n");
            Path src = dir.resolve("big.trig");
            Files.writeString(src, trig.toString());
            File h5 = dir.resolve("big.trig.h5").toFile();
            HDF5Writer.Builder().setSource(src.toFile()).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
            DatasetGraph dsg = DatasetGraphFactory.create();
            RDFParser.create().source(src.toUri().toString()).lang(Lang.TRIG).parse(dsg);
            Dataset truth = DatasetFactory.wrap(dsg);
            try (BeakGraph big = new BeakGraph(new HDF5Reader(h5))) {
                Dataset ds = big.getDataset();
                for (String query : new String[] {
                        "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }",
                        "SELECT * WHERE { GRAPH ?g { ?s ex:value ?o } }",
                        "SELECT * WHERE { GRAPH ?g { ?s ex:link ?t . ?t ex:value ?v } }"}) {
                    long scans = ParallelScan.HITS.get();
                    long shared = GraphVarBG.PARALLEL.get();
                    long walks = GraphVarBG.HITS.get();
                    List<String> rows = rows(ds, query);
                    assertTrue(GraphVarBG.HITS.get() > walks, query);
                    // A walk that hands the graphs it has left to workers is a parallel scan too: not that one.
                    long ofOneGraph = (ParallelScan.HITS.get() - scans) - (GraphVarBG.PARALLEL.get() - shared);
                    assertEquals(1, ofOneGraph, "ex:big, and only it, is scanned in parallel as a graph: " + query);
                    assertEquals(rows(truth, query), rows, query);
                }
                // An outer row carries variables: its pattern runs once a row, and plans no scan.
                long scans = ParallelScan.HITS.get();
                String joined = "SELECT * WHERE { ?k ex:knows ?x . GRAPH ?g { ?s ex:link ?x } }";
                assertEquals(rows(truth, joined), rows(ds, joined));
                assertEquals(scans, ParallelScan.HITS.get(), "no parallel scan under an outer row");
                // Ended early, the scan's workers stop.
                assertEquals(5, rows(ds, "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } } LIMIT 5").size());
                long deadline = System.nanoTime() + 10_000_000_000L;
                while (ParallelScan.ACTIVE_WORKERS.get() > 0 && System.nanoTime() < deadline) {
                    Thread.sleep(10);
                }
                assertEquals(0, ParallelScan.ACTIVE_WORKERS.get(), "workers of a scan a LIMIT ended");
            }
        } finally {
            restore("beakgraph.scan.parallel.threshold", oldThreshold);
            restore("beakgraph.scan.parallel.minchunk", oldMinChunk);
        }
    }

    /**
     * Many small graphs: none is large enough to be scanned in parallel by
     * itself, so a walk whose rows do work - a join, an optional - reads on
     * one thread until it has produced as many rows as a parallel scan is
     * worth, and hands the graphs it has left to workers. The rows are the
     * same. A single pattern's rows are only read, and stay on one thread;
     * so do a walk that finds little, a LIMIT met early and an outer row;
     * and a filter is evaluated by the consuming thread, so a block with one
     * that is not outermost stays on one thread.
     */
    @Test
    void manySmallGraphsAreSharedAmongWorkers() throws Exception {
        String oldThreshold = System.setProperty("beakgraph.scan.parallel.threshold", "64");
        String oldMinChunk = System.setProperty("beakgraph.scan.parallel.minchunk", "64");
        try {
            StringBuilder trig = new StringBuilder("@prefix ex: <http://ex.org/> .\nex:k ex:knows ex:n3_4 .\n");
            for (int g = 0; g < 70; g++) {
                trig.append("ex:t").append(g).append(" {\n");
                for (int i = 0; i < 12; i++) {
                    trig.append("ex:n").append(g).append('_').append(i).append(" ex:value ").append((g * 7 + i) % 40)
                        .append(" ; ex:next ex:n").append(g).append('_').append((i + 1) % 12).append(" .\n");
                }
                if (g % 9 == 0) {
                    trig.append("ex:n").append(g).append("_0 ex:rare \"r").append(g).append("\" .\n");
                }
                trig.append("}\n");
            }
            Path src = dir.resolve("many.trig");
            Files.writeString(src, trig.toString());
            File h5 = dir.resolve("many.trig.h5").toFile();
            HDF5Writer.Builder().setSource(src.toFile()).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
            DatasetGraph dsg = DatasetGraphFactory.create();
            RDFParser.create().source(src.toUri().toString()).lang(Lang.TRIG).parse(dsg);
            Dataset truth = DatasetFactory.wrap(dsg);
            try (BeakGraph many = new BeakGraph(new HDF5Reader(h5))) {
                Dataset ds = many.getDataset();
                // 70 graphs of 24 or 25 quads: the walk passes 64 rows in its third graph, and 67 are left.
                String[] shared = {
                    "SELECT * WHERE { GRAPH ?g { ?a ex:next ?b . ?b ex:next ?c } }",
                    "SELECT * WHERE { GRAPH ?g { ?a ex:next ?b . ?b ex:next ?c . ?c ex:value ?v } }",
                    "SELECT * WHERE { GRAPH ?g { ?s ex:value ?v OPTIONAL { ?s ex:rare ?r } } }",
                    "SELECT * WHERE { GRAPH ?g { ?s ex:value ?v OPTIONAL { ?s ex:next ?t . ?t ex:rare ?r } } }",
                    "SELECT * WHERE { GRAPH ?g { { ?a ex:next ?b . ?b ex:value ?v } UNION { ?a ex:rare ?v } } }",
                    "SELECT * WHERE { GRAPH ?g { ?a ex:next ?b . ?b ex:next ?c } FILTER(?g != ex:t5) }",
                    "SELECT ?g (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?a ex:next ?b . ?b ex:next ?c } } GROUP BY ?g",
                    "SELECT (SUM(?v) AS ?n) WHERE { GRAPH ?g { ?a ex:next ?b . ?b ex:value ?v } }",
                };
                for (String query : shared) {
                    long before = GraphVarBG.PARALLEL.get();
                    List<String> rows = rows(ds, query);
                    assertEquals(1, GraphVarBG.PARALLEL.get() - before, "the graphs left are shared among workers: " + query);
                    assertEquals(rows(truth, query), rows, query);
                    assertEquals(rowsWithoutTheWalk(ds, query), rows, query);
                }
                String[] oneThread = {
                    "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }",                                      // a single pattern: rows that are only read
                    "SELECT * WHERE { GRAPH ?g { ?s ex:value ?v } }",
                    "SELECT * WHERE { GRAPH ?g { ?s ex:value ?v FILTER(?v > 20) } }",
                    "SELECT * WHERE { GRAPH ?g { { ?s ex:value ?v } UNION { ?s ex:rare ?v } } }",
                    "SELECT * WHERE { GRAPH ?g { ?a ex:next ?b . ?b ex:rare ?o } }",                 // 8 rows: never worth it
                    "SELECT * WHERE { GRAPH ?g { ex:n3_4 ?p ?o } }",                                 // a lookup
                    "SELECT * WHERE { ?k ex:knows ?x . GRAPH ?g { ?s ex:next ?x } }",                // an outer row
                    "SELECT * WHERE { GRAPH ?g { ?s ex:value ?v OPTIONAL { ?s ex:rare ?r FILTER(?r != \"r9\") } } }", // a filter inside
                };
                for (String query : oneThread) {
                    long before = GraphVarBG.PARALLEL.get();
                    long walks = GraphVarBG.HITS.get();
                    List<String> rows = rows(ds, query);
                    assertTrue(GraphVarBG.HITS.get() > walks, query);
                    assertEquals(0, GraphVarBG.PARALLEL.get() - before, "walked on one thread: " + query);
                    assertEquals(rows(truth, query), rows, query);
                }
                // A LIMIT met before the walk has produced 64 rows starts no worker; one met after stops them.
                long before = GraphVarBG.PARALLEL.get();
                assertEquals(10, rows(ds, "SELECT * WHERE { GRAPH ?g { ?a ex:next ?b . ?b ex:next ?c } } LIMIT 10").size());
                assertEquals(0, GraphVarBG.PARALLEL.get() - before);
                assertEquals(300, rows(ds, "SELECT * WHERE { GRAPH ?g { ?a ex:next ?b . ?b ex:next ?c } } LIMIT 300").size());
                assertEquals(1, GraphVarBG.PARALLEL.get() - before);
                long deadline = System.nanoTime() + 10_000_000_000L;
                while (ParallelScan.ACTIVE_WORKERS.get() > 0 && System.nanoTime() < deadline) {
                    Thread.sleep(10);
                }
                assertEquals(0, ParallelScan.ACTIVE_WORKERS.get(), "workers of a walk a LIMIT ended");
                // Parallel scans switched off: the walk stays on one thread, and answers the same.
                System.setProperty("beakgraph.scan.parallel.threshold", "0");
                before = GraphVarBG.PARALLEL.get();
                assertEquals(rows(truth, shared[0]), rows(ds, shared[0]));
                assertEquals(0, GraphVarBG.PARALLEL.get() - before);
            }
        } finally {
            restore("beakgraph.scan.parallel.threshold", oldThreshold);
            restore("beakgraph.scan.parallel.minchunk", oldMinChunk);
        }
    }

    private static void restore(String property, String value) {
        if (value == null) {
            System.clearProperty(property);
        } else {
            System.setProperty(property, value);
        }
    }
}
