package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.jena.AggregateCountFastPath;
import com.ebremer.beakgraph.hdf5.jena.GraphVarBG;
import com.ebremer.beakgraph.hdf5.jena.GroupOnIds;
import com.ebremer.beakgraph.hdf5.jena.ParallelScan;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryCancelledException;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.function.FunctionBase1;
import org.apache.jena.sparql.function.FunctionRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

/**
 * {@code GROUP BY} and the aggregates computed on ids, by the threads that
 * read the rows ({@link GroupOnIds}), must give the rows Jena's own group
 * gives over the same pattern. Each query here runs three ways - on ids, with
 * that path switched off (Jena's group over the id-level rows), and plain ARQ
 * over an in-memory dataset parsed from the same source - and the path's
 * counter says which shapes it took. Then the same with the rows divided
 * among workers: many graphs, one large graph, and more groups than a worker
 * keeps to itself.
 */
// Switches the path off and lowers the parallel-scan threshold for the JVM: never beside another query test.
@Isolated
class GroupOnIdsTest {

    private static final String PREFIX = "PREFIX ex: <http://ex.org/>\nPREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n";

    /**
     * Values of every numeric kind and one that is no number; subjects with
     * and without a value; a term - {@code ex:tag} - that is a predicate and
     * a subject; a graph without any of it. (The doubles are written as the
     * store gives them back: it keeps a double's value, and {@code 0.5e0}
     * comes back as {@code "0.5"^^xsd:double}, which a minimum would show.)
     */
    private static final String TRIG = """
        @prefix ex: <http://ex.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        ex:d1 ex:value 5 ; ex:score 1.5 ; ex:tag "a" .
        ex:d2 ex:value 7 ; ex:score 2.25 ; ex:tag "b" .
        ex:d3 ex:value 7 ; ex:tag "a" ; ex:label "seven" .
        ex:d4 ex:label "no value" ; ex:tag "c" .
        ex:x ex:in ex:g1 .
        ex:g1 {
            ex:a ex:value 1 ; ex:score "0.5"^^xsd:double ; ex:tag "x" ; ex:next ex:b .
            ex:b ex:value 2 ; ex:tag "x" ; ex:next ex:c .
            ex:c ex:value 3 ; ex:tag "y" ; ex:word "three" .
            ex:tag a ex:Property .
        }
        ex:g2 {
            ex:a ex:value 10 ; ex:tag "x" ; ex:next ex:c .
            ex:c ex:value "12"^^xsd:int ; ex:tag "z" .
            ex:e ex:value 2.5 ; ex:score "4.0"^^xsd:float ; ex:score "0.25"^^xsd:double .
        }
        ex:g3 {
            ex:m ex:value "not a number" ; ex:tag "x" .
            ex:n ex:value 4 ; ex:tag "x" ; ex:next ex:m .
        }
        ex:g4 {
            ex:p ex:q ex:r .
        }
        """;

    /** Shapes grouped on ids. */
    private static final String[] TAKEN = {
        // counts, of every named graph
        "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ex:value ?v } }",
        "SELECT (COUNT(?v) AS ?n) (COUNT(?t) AS ?m) (COUNT(*) AS ?r) WHERE { GRAPH ?g { ?s ex:tag ?t OPTIONAL { ?s ex:value ?v } } }",
        "SELECT (COUNT(DISTINCT ?t) AS ?n) (COUNT(DISTINCT ?s) AS ?m) (COUNT(DISTINCT ?g) AS ?k) WHERE { GRAPH ?g { ?s ex:tag ?t } }",
        "SELECT (COUNT(?nothing) AS ?n) (COUNT(DISTINCT ?nothing) AS ?m) WHERE { GRAPH ?g { ?s ex:tag ?t } }",
        "SELECT (COUNT(?v + 1) AS ?n) WHERE { GRAPH ?g { ?s ex:value ?v } }",    // the one that is no number is an error, and not counted
        // keys: the graph, a value, both, a predicate, a variable that is not always bound
        "SELECT ?g (COUNT(*) AS ?n) (COUNT(DISTINCT ?t) AS ?d) WHERE { GRAPH ?g { ?s ex:tag ?t } } GROUP BY ?g",
        "SELECT ?t (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ex:tag ?t } } GROUP BY ?t",
        "SELECT ?g ?t (COUNT(?s) AS ?n) WHERE { GRAPH ?g { ?s ex:tag ?t } } GROUP BY ?g ?t",
        "SELECT ?p (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o FILTER(isLiteral(?o)) } } GROUP BY ?p",
        "SELECT ?v (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ex:tag ?t OPTIONAL { ?s ex:value ?v } } } GROUP BY ?v",
        "SELECT ?s (COUNT(DISTINCT ?g) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o FILTER(isLiteral(?o)) } } GROUP BY ?s",
        "SELECT ?t WHERE { GRAPH ?g { ?s ex:tag ?t } } GROUP BY ?t",
        // a term that is a predicate in one branch and a subject in the other is one group, and one distinct term
        "SELECT ?x (COUNT(*) AS ?n) WHERE { GRAPH ?g { { ?s ?x ?o } UNION { ?x a ?c } } } GROUP BY ?x",
        "SELECT (COUNT(DISTINCT ?x) AS ?n) WHERE { GRAPH ?g { { ?s ?x ?o } UNION { ?x a ?c } } }",
        // sums, averages, the least and the greatest: of numbers of every kind, and of what is no number
        "SELECT (SUM(?v) AS ?sum) (AVG(?v) AS ?avg) (MIN(?v) AS ?min) (MAX(?v) AS ?max) WHERE { GRAPH ?g { ?s ex:value ?v } }",
        "SELECT ?g (SUM(?v) AS ?sum) (AVG(?v) AS ?avg) (MIN(?v) AS ?min) (MAX(?v) AS ?max) WHERE { GRAPH ?g { ?s ex:value ?v } } GROUP BY ?g",
        "SELECT ?g (SUM(?v) AS ?sum) (AVG(?v) AS ?avg) WHERE { GRAPH ?g { ?s ex:value ?v FILTER(isNumeric(?v)) } } GROUP BY ?g",
        "SELECT (SUM(?v) AS ?sum) (AVG(?v) AS ?avg) (MIN(?v) AS ?min) (MAX(?v) AS ?max) WHERE { GRAPH ?g { ?s ex:value ?v FILTER(isNumeric(?v)) } }",
        "SELECT (SUM(?v * 2) AS ?n) (MAX(?v + 0.5) AS ?m) WHERE { GRAPH ?g { ?s ex:value ?v FILTER(isNumeric(?v)) } }",
        "SELECT (SUM(?c) AS ?sum) (AVG(?c) AS ?avg) (MIN(?c) AS ?min) (MAX(?c) AS ?max) WHERE { GRAPH ?g { ?s ex:score ?c } }",
        "SELECT ?t (SUM(?v) AS ?sum) (MAX(?v) AS ?max) (COUNT(?v) AS ?n) WHERE { GRAPH ?g { ?s ex:tag ?t OPTIONAL { ?s ex:value ?v } } } GROUP BY ?t",
        "SELECT (MIN(?t) AS ?min) (MAX(?t) AS ?max) (MIN(?s) AS ?first) WHERE { GRAPH ?g { ?s ex:tag ?t } }",
        // joins, optionals and unions under the group
        "SELECT ?g (COUNT(*) AS ?n) (SUM(?v) AS ?sum) WHERE { GRAPH ?g { ?a ex:next ?b . ?b ex:value ?v } } GROUP BY ?g",
        "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ?g { { ?s ex:tag ?o } UNION { ?s ex:word ?o } } }",
        "SELECT (COUNT(*) AS ?n) (COUNT(DISTINCT ?s) AS ?d) WHERE { { GRAPH ?g { ?s ex:tag ?o } } UNION { GRAPH ?g { ?s ex:value ?o } } }",
        "SELECT ?t (COUNT(*) AS ?n) WHERE { { ?s ex:tag ?t } UNION { GRAPH ?g { ?s ex:tag ?t } } } GROUP BY ?t",
        // what stands on the group
        "SELECT ?t (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ex:tag ?t } } GROUP BY ?t HAVING (COUNT(*) > 1)",
        "SELECT ?t (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ex:tag ?t } } GROUP BY ?t ORDER BY DESC(?n) ?t LIMIT 2",
        "SELECT ?t ((SUM(?v) + COUNT(*)) AS ?n) WHERE { GRAPH ?g { ?s ex:tag ?t . ?s ex:value ?v FILTER(isNumeric(?v)) } } GROUP BY ?t",
        // no rows: one row of what each aggregate says of nothing, or no group
        "SELECT (COUNT(*) AS ?n) (SUM(?v) AS ?sum) (MIN(?v) AS ?min) (AVG(?v) AS ?avg) (COUNT(DISTINCT ?v) AS ?d) WHERE { GRAPH ?g { ?s ex:nope ?v } }",
        "SELECT ?s (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ex:nope ?v } } GROUP BY ?s",
        "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ex:value ?v FILTER(?v > 1000) } }",
        // one graph: the default graph, and a graph by its name
        "SELECT (COUNT(*) AS ?n) (SUM(?v) AS ?sum) (AVG(?v) AS ?avg) WHERE { ?s ex:value ?v }",
        "SELECT ?t (COUNT(*) AS ?n) WHERE { ?s ex:tag ?t } GROUP BY ?t",
        "SELECT (COUNT(*) AS ?n) WHERE { ?s ex:value ?v FILTER(?v > 5) }",
        "SELECT ?t (MAX(?v) AS ?m) WHERE { ?s ex:tag ?t OPTIONAL { ?s ex:value ?v } } GROUP BY ?t",
        "SELECT (AVG(?v) AS ?a) (COUNT(DISTINCT ?s) AS ?n) WHERE { GRAPH ex:g1 { ?s ex:value ?v } }",
        "SELECT ?t (MAX(?v) AS ?m) WHERE { GRAPH ex:g2 { ?s ex:tag ?t OPTIONAL { ?s ex:value ?v } } } GROUP BY ?t",
        "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ex:nowhere { ?s ?p ?o FILTER(isIRI(?o)) } }",
        "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ex:g4 { ?s ?p ?o FILTER(isIRI(?o)) } }",
        "SELECT (COUNT(*) AS ?n) WHERE { { ?s ex:tag ?t } UNION { ?s ex:label ?t } }",
        // a group in a sub-select is a group over the root as well
        "SELECT * WHERE { { SELECT ?t (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ex:tag ?t } } GROUP BY ?t } FILTER(?n > 1) }",
    };

    /** Shapes left to Jena's group, over whatever executes the pattern. */
    private static final String[] LEFT_TO_JENA = {
        // a key that is an expression; aggregates that are not combined from parts here
        "SELECT ?l (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ex:tag ?t } } GROUP BY (STRLEN(?t) AS ?l)",
        "SELECT (SUM(DISTINCT ?v) AS ?n) WHERE { GRAPH ?g { ?s ex:value ?v FILTER(isNumeric(?v)) } }",
        "SELECT (COUNT(DISTINCT STR(?t)) AS ?n) WHERE { GRAPH ?g { ?s ex:tag ?t } }",
        "SELECT (COUNT(DISTINCT *) AS ?n) WHERE { GRAPH ?g { ?s ex:tag ?t } }",
        "SELECT (MIN(DISTINCT ?v) AS ?n) WHERE { GRAPH ?g { ?s ex:value ?v } }",
        "SELECT (COUNT(*) AS ?n) (SUM(IF(EXISTS { ?s ex:next ?x }, 1, 0)) AS ?m) WHERE { GRAPH ?g { ?s ex:tag ?t } }",
        // patterns the walk does not run
        "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?a ex:next+ ?b } }",
        "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ex:tag ?t MINUS { ?s ex:word ?w } } }",
        "SELECT (SUM(?w) AS ?n) WHERE { GRAPH ?g { ?s ex:value ?v FILTER(isNumeric(?v)) BIND(?v * 2 AS ?w) } }",
        "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ex:tag ?t } FILTER(?g != ex:g2) }",
        "SELECT (COUNT(*) AS ?n) WHERE { ?x ex:in ?g . GRAPH ?g { ?s ex:tag ?t } }",
        "SELECT (COUNT(*) AS ?n) WHERE { ?a ex:next* ?b }",
        // graphs that are unions, and datasets that are not the store's
        "SELECT (COUNT(*) AS ?n) WHERE { GRAPH <urn:x-arq:UnionGraph> { ?s ex:tag ?t } }",
        "SELECT (COUNT(*) AS ?n) FROM ex:g1 FROM ex:g2 WHERE { ?s ex:tag ?t }",
        "SELECT (COUNT(*) AS ?n) FROM NAMED ex:g1 FROM NAMED ex:g3 WHERE { GRAPH ?g { ?s ex:tag ?t } }",
        // a group under a row of something else
        "SELECT * WHERE { GRAPH ?g { { SELECT ?t (COUNT(*) AS ?n) WHERE { ?s ex:tag ?t } GROUP BY ?t } } }",
    };

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset store;
    static Dataset reference;

    @BeforeAll
    static void build() throws Exception {
        Path src = dir.resolve("group.trig");
        Files.writeString(src, TRIG);
        File h5 = dir.resolve("group.trig.h5").toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        bg = new BeakGraph(new HDF5Reader(h5));
        store = bg.getDataset();
        reference = inMemory(src);
    }

    @AfterAll
    static void close() {
        if (bg != null) bg.close();
    }

    private static Dataset inMemory(Path src) {
        DatasetGraph dsg = DatasetGraphFactory.create();
        RDFParser.create().source(src.toUri().toString()).lang(Lang.TRIG).parse(dsg);
        return DatasetFactory.wrap(dsg);
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

    /** The store's rows with the path off: Jena's group over the id-level rows. */
    private static List<String> rowsGroupedByJena(Dataset dataset, String query) {
        boolean previous = GroupOnIds.setEnabled(false);
        try {
            long before = GroupOnIds.HITS.get();
            List<String> rows = rows(dataset, query);
            assertEquals(before, GroupOnIds.HITS.get(), "switched off, the path answers nothing");
            return rows;
        } finally {
            GroupOnIds.setEnabled(previous);
        }
    }

    @Test
    void groupsOnIdsAreTheGroupsJenaMakes() {
        for (String query : TAKEN) {
            long before = GroupOnIds.HITS.get();
            List<String> rows = rows(store, query);
            assertTrue(GroupOnIds.HITS.get() > before, "grouped on ids: " + query);
            assertEquals(rows(reference, query), rows, query);
            assertEquals(rowsGroupedByJena(store, query), rows, query);
        }
    }

    @Test
    void theQueriesReallyHaveRowsAndValues() {
        // A table of queries that all answered nothing, or an unbound value, would compare equal three ways too.
        assertEquals(List.of("n=\"8\"^^xsd:integer "), rows(store, TAKEN[0]));
        int withRows = 0;
        for (String query : TAKEN) {
            if (rows(store, query).stream().anyMatch(row -> row.contains("="))) {
                withRows++;
            }
        }
        assertTrue(withRows >= TAKEN.length - 1, withRows + " of " + TAKEN.length + " queries bind something");
        // The sum over every value has none (one of them is no number); the sum over the numbers is exact.
        List<String> all = rows(store, "SELECT (SUM(?v) AS ?sum) (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ex:value ?v } }");
        assertEquals(1, all.size());
        assertFalse(all.get(0).contains("sum="), all.get(0));
        List<String> numbers = rows(store, "SELECT (SUM(?v) AS ?sum) WHERE { GRAPH ?g { ?s ex:value ?v FILTER(isNumeric(?v)) } }");
        assertEquals(List.of("sum=\"34.5\"^^xsd:decimal "), numbers);
    }

    @Test
    void otherShapesAreLeftToJenaAndGiveTheSameRows() {
        for (String query : LEFT_TO_JENA) {
            long before = GroupOnIds.HITS.get();
            List<String> rows = rows(store, query);
            assertEquals(before, GroupOnIds.HITS.get(), "not a shape grouped on ids: " + query);
            assertEquals(rows(reference, query), rows, query);
        }
    }

    @Test
    void theCountsTheIndexHoldsStillComeFromTheIndex() {
        long index = AggregateCountFastPath.HITS.get();
        long ids = GroupOnIds.HITS.get();
        assertEquals(rows(reference, "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } }"),
                rows(store, "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } }"));
        assertEquals(rows(reference, "SELECT ?p (COUNT(*) AS ?n) WHERE { ?s ?p ?o } GROUP BY ?p"),
                rows(store, "SELECT ?p (COUNT(*) AS ?n) WHERE { ?s ?p ?o } GROUP BY ?p"));
        assertEquals(2, AggregateCountFastPath.HITS.get() - index);
        assertEquals(0, GroupOnIds.HITS.get() - ids);
    }

    @Test
    void aGroupOverAWalkIsAWalk() {
        long walks = GraphVarBG.HITS.get();
        rows(store, "SELECT ?t (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ex:tag ?t } } GROUP BY ?t");
        assertEquals(1, GraphVarBG.HITS.get() - walks, "the block is planned and read as the walk plans and reads it");
        // With the walk switched off there is no block to take rows from: Jena's group, over Jena's graphs.
        boolean previous = GraphVarBG.setEnabled(false);
        try {
            long ids = GroupOnIds.HITS.get();
            String query = "SELECT ?t (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ex:tag ?t } } GROUP BY ?t";
            assertEquals(rows(reference, query), rows(store, query));
            assertEquals(0, GroupOnIds.HITS.get() - ids);
        } finally {
            GraphVarBG.setEnabled(previous);
        }
    }

    /** A store of {@code graphs} graphs of twelve subjects each: a number, a tag of five, a ring of {@code ex:next}. */
    private static String many(int graphs, int big) {
        StringBuilder trig = new StringBuilder("@prefix ex: <http://ex.org/> .\n@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n");
        for (int i = 0; i < 40; i++) {
            trig.append("ex:d").append(i).append(" ex:value ").append(i).append(" ; ex:tag \"t").append(i % 3).append("\" .\n");
        }
        for (int g = 0; g < graphs; g++) {
            trig.append("ex:t").append(g).append(" {\n");
            int subjects = (g == 7) ? big : 12;
            for (int i = 0; i < subjects; i++) {
                trig.append("ex:n").append(g).append('_').append(i).append(" ex:value ").append((g * 7 + i) % 40)
                    .append(" ; ex:tag \"t").append((g + i) % 5).append("\" ; ex:half \"").append(((g + i) % 8) * 0.5).append("\"^^xsd:double")
                    .append(" ; ex:next ex:n").append(g).append('_').append((i + 1) % subjects).append(" .\n");
            }
            if (g % 9 == 0) {
                trig.append("ex:n").append(g).append("_0 ex:rare \"r").append(g).append("\" .\n");
            }
            trig.append("}\n");
        }
        return trig.toString();
    }

    private static final String[] MANY = {
        "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ex:value ?v } }",
        "SELECT (COUNT(*) AS ?n) (COUNT(DISTINCT ?v) AS ?d) (COUNT(DISTINCT ?g) AS ?k) WHERE { GRAPH ?g { ?s ex:value ?v FILTER(?v > 20) } }",
        "SELECT (SUM(?v) AS ?sum) (AVG(?v) AS ?avg) (MIN(?v) AS ?min) (MAX(?v) AS ?max) WHERE { GRAPH ?g { ?s ex:value ?v } }",
        "SELECT (SUM(?h) AS ?sum) (AVG(?h) AS ?avg) (MAX(?h) AS ?max) WHERE { GRAPH ?g { ?s ex:half ?h } }",
        "SELECT ?t (COUNT(*) AS ?n) (SUM(?v) AS ?sum) (MAX(?v) AS ?max) WHERE { GRAPH ?g { ?s ex:tag ?t ; ex:value ?v } } GROUP BY ?t",
        "SELECT ?g (COUNT(*) AS ?n) (AVG(?v) AS ?avg) WHERE { GRAPH ?g { ?a ex:next ?b . ?b ex:value ?v } } GROUP BY ?g",
        "SELECT ?g ?t (COUNT(?s) AS ?n) WHERE { GRAPH ?g { ?s ex:tag ?t } } GROUP BY ?g ?t",
        "SELECT ?s (COUNT(*) AS ?n) (MIN(?o) AS ?least) WHERE { GRAPH ?g { ?s ?p ?o FILTER(isLiteral(?o)) } } GROUP BY ?s",
        "SELECT ?r (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ex:value ?v OPTIONAL { ?s ex:rare ?r } } } GROUP BY ?r",
        "SELECT (COUNT(*) AS ?n) (COUNT(?r) AS ?m) WHERE { GRAPH ?g { { ?s ex:value ?v } UNION { ?s ex:rare ?r } } }",
        "SELECT ?p (COUNT(*) AS ?n) WHERE { { GRAPH ?g { ?s ?p ?o FILTER(isIRI(?o)) } } UNION { GRAPH ?g { ?s ?p ?o FILTER(isLiteral(?o)) } } } GROUP BY ?p",
        "SELECT (COUNT(*) AS ?n) (COUNT(DISTINCT ?x) AS ?d) (MIN(?x) AS ?least) WHERE { GRAPH ex:t7 { ?s ex:next ?x } }",
        "SELECT (COUNT(*) AS ?n) (MAX(?y) AS ?last) WHERE { GRAPH ex:t7 { ?s ex:next ?x . ?x ex:next ?y } }",
        // 600 rows of 40 values, and of 5: too few objects to divide the scan by, so it is divided by its rows
        "SELECT (COUNT(*) AS ?n) (SUM(?v) AS ?sum) WHERE { GRAPH ex:t7 { ?s ex:value ?v } }",
        "SELECT ?t (COUNT(*) AS ?n) (COUNT(DISTINCT ?s) AS ?d) WHERE { GRAPH ex:t7 { ?s ex:tag ?t } } GROUP BY ?t",
        "SELECT ?t (COUNT(*) AS ?n) WHERE { GRAPH ex:t7 { ?s ex:tag ?t FILTER(?t >= \"t2\") } } GROUP BY ?t",
        "SELECT ?s (COUNT(*) AS ?n) WHERE { GRAPH ex:t7 { ?s ?p ?o FILTER(isLiteral(?o)) } } GROUP BY ?s",
    };

    private static void restore(String property, String value) {
        if (value == null) {
            System.clearProperty(property);
        } else {
            System.setProperty(property, value);
        }
    }

    private static void awaitNoWorkers() throws InterruptedException {
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (ParallelScan.ACTIVE_WORKERS.get() > 0 && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertEquals(0, ParallelScan.ACTIVE_WORKERS.get(), "workers still running");
    }

    @Test
    void workersGroupTheRowsTheyRead() throws Exception {
        String oldThreshold = System.setProperty("beakgraph.scan.parallel.threshold", "64");
        String oldMinChunk = System.setProperty("beakgraph.scan.parallel.minchunk", "64");
        String oldHandOff = System.setProperty("beakgraph.group.handoff", "64");
        String oldFlush = System.getProperty("beakgraph.group.flush");
        try {
            Path src = dir.resolve("many.trig");
            Files.writeString(src, many(70, 600)); // 69 graphs of 12 subjects and one, ex:t7, of 600
            File h5 = dir.resolve("many.trig.h5").toFile();
            HDF5Writer.Builder().setSource(src.toFile()).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
            Dataset truth = inMemory(src);
            try (BeakGraph many = new BeakGraph(new HDF5Reader(h5))) {
                Dataset ds = many.getDataset();
                // Few groups a worker, then more groups than a worker keeps to itself.
                for (String flush : new String[] {null, "4"}) {
                    restore("beakgraph.group.flush", flush);
                    for (String query : MANY) {
                        long before = GroupOnIds.PARALLEL.get();
                        long scans = ParallelScan.HITS.get();
                        List<String> rows = rows(ds, query);
                        assertTrue(GroupOnIds.PARALLEL.get() > before, "rows grouped by workers: " + query);
                        assertEquals(scans, ParallelScan.HITS.get(), "and no row handed from one thread to another: " + query);
                        assertEquals(rows(truth, query), rows, query);
                        assertEquals(rowsGroupedByJena(ds, query), rows, query);
                        assertEquals(rows, rows(ds, query), "the same answer the next time: " + query);
                    }
                }
                restore("beakgraph.group.flush", oldFlush);
                awaitNoWorkers();
                // A query that reads little starts no worker: what it costs is counted, and the rest is handed to
                // workers from 4,096 on - a graph visited being four, these seventy graphs are 280 by themselves.
                restore("beakgraph.group.handoff", oldHandOff);
                for (String query : new String[] {
                        "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ?g { ex:n3_4 ?p ?o } }",
                        "SELECT ?r (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ex:rare ?r } } GROUP BY ?r",
                        "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ex:t3 { ?s ex:value ?v } }",
                        "SELECT (SUM(?v) AS ?n) WHERE { ?s ex:value ?v }"}) {
                    long before = GroupOnIds.PARALLEL.get();
                    long ids = GroupOnIds.HITS.get();
                    assertEquals(rows(truth, query), rows(ds, query), query);
                    assertEquals(1, GroupOnIds.HITS.get() - ids, query);
                    assertEquals(0, GroupOnIds.PARALLEL.get() - before, "grouped on the thread that asked: " + query);
                }
                // What a filter turns away and what a join does not find is work all the same: a count of nothing
                // out of 1,400 rows read (the rings of ex:next hold no pair pointing at each other) is with
                // workers long before it is done.
                System.setProperty("beakgraph.group.handoff", "128");
                for (String query : new String[] {
                        "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ex:value ?v FILTER(isIRI(?v)) } }",
                        "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?a ex:next ?b . ?b ex:next ?a } }"}) {
                    long before = GroupOnIds.PARALLEL.get();
                    assertEquals(rows(truth, query), rows(ds, query), query);
                    assertEquals(1, GroupOnIds.PARALLEL.get() - before, "handed to workers for the work, not the rows: " + query);
                }
                // But not where the graphs left have too little to do for two workers: a filter the index answers
                // (no value is over 1000, and none is read to find that out), a lookup, and a join on a predicate
                // 8 of the 70 graphs hold - the walk visits only those.
                for (String query : new String[] {
                        "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ex:value ?v FILTER(?v > 1000) } }",
                        "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ?g { ex:n3_4 ?p ?o } }",
                        "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?a ex:next ?b . ?b ex:rare ?r } }"}) {
                    long before = GroupOnIds.PARALLEL.get();
                    assertEquals(rows(truth, query), rows(ds, query), query);
                    assertEquals(0, GroupOnIds.PARALLEL.get() - before, "too little left for workers: " + query);
                }
                // Parallel work switched off: every group on the thread that asked, and the same answers.
                System.setProperty("beakgraph.scan.parallel.threshold", "0");
                for (String query : MANY) {
                    long before = GroupOnIds.PARALLEL.get();
                    long ids = GroupOnIds.HITS.get();
                    assertEquals(rows(truth, query), rows(ds, query), query);
                    assertEquals(1, GroupOnIds.HITS.get() - ids, query);
                    assertEquals(0, GroupOnIds.PARALLEL.get() - before, query);
                }
            }
        } finally {
            restore("beakgraph.scan.parallel.threshold", oldThreshold);
            restore("beakgraph.scan.parallel.minchunk", oldMinChunk);
            restore("beakgraph.group.handoff", oldHandOff);
            restore("beakgraph.group.flush", oldFlush);
        }
    }

    @Test
    void aTimeoutStopsTheWorkers() throws Exception {
        String oldThreshold = System.setProperty("beakgraph.scan.parallel.threshold", "64");
        String oldMinChunk = System.setProperty("beakgraph.scan.parallel.minchunk", "64");
        String oldHandOff = System.setProperty("beakgraph.group.handoff", "64");
        try {
            Path src = dir.resolve("slow.trig");
            Files.writeString(src, many(40, 3000));
            File h5 = dir.resolve("slow.trig.h5").toFile();
            HDF5Writer.Builder().setSource(src.toFile()).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
            try (BeakGraph slow = new BeakGraph(new HDF5Reader(h5))) {
                Dataset ds = slow.getDataset();
                // Every subject of a graph with every other and every third: 1,728 rows a graph of twelve, which is
                // enough for workers after the first; and 27,000,000,000 in the graph of 3,000, which none will finish.
                String query = "SELECT (COUNT(*) AS ?n) (SUM(?v) AS ?sum) WHERE { GRAPH ?g { ?a ex:value ?v . ?b ex:value ?w . ?c ex:tag ?t } }";
                long before = GroupOnIds.PARALLEL.get();
                try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(PREFIX + query))
                        .timeout(300, TimeUnit.MILLISECONDS).build()) {
                    assertThrows(QueryCancelledException.class, () -> {
                        ResultSet rs = qe.execSelect();
                        while (rs.hasNext()) {
                            rs.next();
                        }
                    });
                }
                assertTrue(GroupOnIds.PARALLEL.get() > before, "the query was with workers when it was stopped");
                awaitNoWorkers();
                // A block that reads for long without a row to show for it - every three subjects of the large graph,
                // each turned away by the filter - notices the cancel while it reads.
                String barren = "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?a ex:value ?v . ?b ex:value ?w . ?c ex:value ?x FILTER(?v + ?w + ?x < 0) } }";
                for (boolean onIds : new boolean[] {true, false}) {
                    boolean previous = GroupOnIds.setEnabled(onIds);
                    try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(PREFIX + barren))
                            .timeout(200, TimeUnit.MILLISECONDS).build()) {
                        long started = System.nanoTime();
                        assertThrows(QueryCancelledException.class, () -> {
                            ResultSet rs = qe.execSelect();
                            while (rs.hasNext()) {
                                rs.next();
                            }
                        });
                        long ms = (System.nanoTime() - started) / 1_000_000;
                        assertTrue(ms < 5_000, "stopped " + ms + " ms after it was started, grouped " + (onIds ? "on ids" : "by Jena"));
                    } finally {
                        GroupOnIds.setEnabled(previous);
                    }
                    awaitNoWorkers();
                }
                // And the store answers the next query.
                assertEquals(1, rows(ds, "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ex:rare ?r } }").size());
            }
        } finally {
            restore("beakgraph.scan.parallel.threshold", oldThreshold);
            restore("beakgraph.scan.parallel.minchunk", oldMinChunk);
            restore("beakgraph.group.handoff", oldHandOff);
        }
    }

    /** Throws for one subject of a graph far down the list: by the time it is read, the rows are with workers. */
    public static final class Boom extends FunctionBase1 {
        @Override
        public NodeValue exec(NodeValue v) {
            if (v.asNode().isURI() && v.asNode().getURI().endsWith("n60_5")) {
                throw new IllegalStateException("boom");
            }
            return NodeValue.TRUE;
        }
    }

    @Test
    void whatFailsInAWorkerFailsTheQuery() throws Exception {
        String oldThreshold = System.setProperty("beakgraph.scan.parallel.threshold", "64");
        String oldMinChunk = System.setProperty("beakgraph.scan.parallel.minchunk", "64");
        String oldHandOff = System.setProperty("beakgraph.group.handoff", "64");
        try {
            Path src = dir.resolve("boom.trig");
            Files.writeString(src, many(70, 12));
            File h5 = dir.resolve("boom.trig.h5").toFile();
            HDF5Writer.Builder().setSource(src.toFile()).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
            try (BeakGraph boom = new BeakGraph(new HDF5Reader(h5))) {
                Dataset ds = boom.getDataset();
                FunctionRegistry.get(ds.getContext()).put("http://ex.org/fn#boom", Boom.class);
                String query = "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ex:value ?v FILTER(<http://ex.org/fn#boom>(?s)) } }";
                long before = GroupOnIds.PARALLEL.get();
                IllegalStateException thrown = assertThrows(IllegalStateException.class, () -> rows(ds, query));
                assertEquals("boom", thrown.getMessage());
                assertEquals(1, GroupOnIds.PARALLEL.get() - before, "it was a worker that met it");
                awaitNoWorkers();
                // Jena's group fails the same way, on the thread that asked; and the store answers the next query.
                boolean previous = GroupOnIds.setEnabled(false);
                try {
                    assertThrows(IllegalStateException.class, () -> rows(ds, query));
                } finally {
                    GroupOnIds.setEnabled(previous);
                }
                assertEquals(List.of("n=\"840\"^^xsd:integer "), rows(ds, "SELECT (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ex:value ?v FILTER(?v >= 0) } }"));
            }
        } finally {
            restore("beakgraph.scan.parallel.threshold", oldThreshold);
            restore("beakgraph.scan.parallel.minchunk", oldMinChunk);
            restore("beakgraph.group.handoff", oldHandOff);
        }
    }

    /** Blocks, and the variables each can bind. */
    private static final String[][] BLOCKS = {
        {"?s ex:value ?v", "s v"},
        {"?s ex:tag ?t ; ex:value ?v", "s t v"},
        {"?s ex:value ?v OPTIONAL { ?s ex:rare ?r }", "s v r"},
        {"{ ?s ex:value ?v } UNION { ?s ex:rare ?r }", "s v r"},
        {"?s ex:value ?v FILTER(?v > 20)", "s v"},
        {"?s ex:next ?x . ?x ex:value ?v", "s x v"},
        {"?s ex:half ?v ; ex:tag ?t FILTER(?t != \"t3\")", "s v t"},
        {"?s ?p ?o FILTER(isLiteral(?o))", "s p o"},
        {"{ ?s ?x ?o FILTER(isIRI(?o)) } UNION { ?x ex:tag ?t }", "s x o t"},
    };

    /**
     * Three hundred groups nobody chose: a block, where it runs, some of its
     * variables as keys and some aggregates of the others, drawn by a
     * generator with a fixed seed - every one answered on ids, by workers
     * wherever there is enough to read, and held to plain ARQ over the same
     * data in memory and to Jena's group over the same rows.
     */
    @Test
    void groupsNobodyChoseAreTheGroupsJenaMakes() throws Exception {
        String oldThreshold = System.setProperty("beakgraph.scan.parallel.threshold", "64");
        String oldMinChunk = System.setProperty("beakgraph.scan.parallel.minchunk", "64");
        String oldHandOff = System.setProperty("beakgraph.group.handoff", "64");
        String oldFlush = System.setProperty("beakgraph.group.flush", "16");
        try {
            Path src = dir.resolve("drawn.trig");
            Files.writeString(src, many(70, 300));
            File h5 = dir.resolve("drawn.trig.h5").toFile();
            HDF5Writer.Builder().setSource(src.toFile()).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
            Dataset truth = inMemory(src);
            try (BeakGraph drawn = new BeakGraph(new HDF5Reader(h5))) {
                Dataset ds = drawn.getDataset();
                Random random = new Random(20261115);
                long withWorkers = 0;
                for (int q = 0; q < 300; q++) {
                    String[] block = BLOCKS[random.nextInt(BLOCKS.length)];
                    List<String> vars = new ArrayList<>(List.of(block[1].split(" ")));
                    String where = switch (random.nextInt(4)) {
                        case 0 -> "{ " + block[0] + " }";
                        case 1 -> "{ GRAPH ex:t7 { " + block[0] + " } }";
                        default -> {
                            vars.add("g");
                            yield "{ GRAPH ?g { " + block[0] + " } }";
                        }
                    };
                    List<String> keys = new ArrayList<>();
                    for (int k = random.nextInt(3); k > 0; k--) {
                        String key = vars.get(random.nextInt(vars.size()));
                        if (!keys.contains(key)) keys.add(key);
                    }
                    StringBuilder select = new StringBuilder("SELECT");
                    for (String key : keys) {
                        select.append(" ?").append(key);
                    }
                    int aggregates = (keys.isEmpty() ? 1 : 0) + random.nextInt(3);
                    for (int a = 0; a < aggregates; a++) {
                        String v = "?" + vars.get(random.nextInt(vars.size()));
                        String call = switch (random.nextInt(8)) {
                            case 0 -> "COUNT(*)";
                            case 1 -> "COUNT(" + v + ")";
                            case 2 -> "COUNT(DISTINCT " + v + ")";
                            case 3 -> "SUM(" + v + ")";
                            case 4 -> "AVG(" + v + ")";
                            case 5 -> "MIN(" + v + ")";
                            case 6 -> "MAX(" + v + ")";
                            default -> "SUM(?v)";
                        };
                        select.append(" (").append(call).append(" AS ?a").append(a).append(')');
                    }
                    String query = select + " WHERE " + where;
                    if (!keys.isEmpty()) {
                        query += " GROUP BY ?" + String.join(" ?", keys);
                    }
                    long ids = GroupOnIds.HITS.get();
                    long parallel = GroupOnIds.PARALLEL.get();
                    List<String> rows = rows(ds, query);
                    assertEquals(1, GroupOnIds.HITS.get() - ids, "grouped on ids: " + query);
                    withWorkers += Math.min(1, GroupOnIds.PARALLEL.get() - parallel);
                    assertEquals(rows(truth, query), rows, query);
                    assertEquals(rowsGroupedByJena(ds, query), rows, query);
                }
                assertTrue(withWorkers >= 150, withWorkers + " of the 300 were read by workers");
                awaitNoWorkers();
            }
        } finally {
            restore("beakgraph.scan.parallel.threshold", oldThreshold);
            restore("beakgraph.scan.parallel.minchunk", oldMinChunk);
            restore("beakgraph.group.handoff", oldHandOff);
            restore("beakgraph.group.flush", oldFlush);
        }
    }
}
