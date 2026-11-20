package com.ebremer.beakgraph;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.hdf5.jena.GraphVarBG;
import com.ebremer.beakgraph.hdf5.jena.RangeSelect;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.readers.IndexReader;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.jena.graph.NodeFactory;
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
import org.apache.jena.sparql.core.Quad;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Fewer iterators per row. The union read and the GRAPH ?g walk build one per graph and input row; they
 * now visit only the graphs whose GPOS predicate level holds the pattern's predicate
 * ({@link HDF5Reader#graphsWithPredicate}) - OKN's federation/sawgraph-hydrology-spatial_03 spent 88% of
 * its 82 s building them in all 50 graphs. And each graph's first-level block, which every iterator looks
 * up as it is built, is looked up once ({@link IndexReader#graphBlock}). {@link HDF5Reader#GRAPH_READS}
 * counts the graphs visited; the answers are always Jena's.
 */
class GraphSkippingTest {

    private static final String NS = "http://ex.org/";
    private static final String PREFIX = "PREFIX ex: <" + NS + ">\n";
    private static final String UNION = "urn:x-arq:UnionGraph";

    /** Five named graphs: ex:p1 in g1 only, ex:p2 in g2 only, ex:p3 in g3, g4 and g5; ex:q in g4 and g5. */
    private static final String TRIG = """
            @prefix ex: <http://ex.org/> .
            ex:d ex:p1 ex:x .
            ex:g1 { ex:a ex:p1 ex:b . ex:a ex:p1 ex:c . }
            ex:g2 { ex:a ex:p2 ex:b . }
            ex:g3 { ex:a ex:p3 ex:b . ex:e ex:p3 ex:f . }
            ex:g4 { ex:a ex:p3 ex:b . ex:a ex:q "4" . }
            ex:g5 { ex:h ex:p3 ex:i . ex:h ex:q "5" . }
            """;

    @TempDir
    static Path dir;
    static Path src;
    static BeakGraph bg;
    static Dataset store;
    static Dataset reference;

    @BeforeAll
    static void build() throws Exception {
        src = dir.resolve("skip.trig");
        Files.write(src, TRIG.getBytes(StandardCharsets.UTF_8));
        File h5 = dir.resolve("skip.h5").toFile();
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

    private static List<String> rows(Dataset d, String query) {
        List<String> out = new ArrayList<>();
        try (QueryExecution qe = QueryExecution.dataset(d).query(QueryFactory.create(PREFIX + query)).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                QuerySolution qs = rs.next();
                StringBuilder sb = new StringBuilder();
                for (String v : rs.getResultVars()) sb.append(v).append('=').append(qs.get(v)).append(' ');
                out.add(sb.toString());
            }
        }
        Collections.sort(out);
        return out;
    }

    /** Asserts Jena's answer and the row count, and returns how many graphs the store read for it. */
    private static long graphReads(String query, int expectedRows) {
        long before = HDF5Reader.GRAPH_READS.sum();
        List<String> got = rows(store, query);
        long reads = HDF5Reader.GRAPH_READS.sum() - before;
        assertEquals(rows(reference, query), got, query);
        assertEquals(expectedRows, got.size(), query);
        return reads;
    }

    // ---------------- the union read ----------------

    @Test
    void theUnionReadsOnlyTheGraphsHoldingThePredicate() {
        assertEquals(1, graphReads("SELECT * FROM <" + UNION + "> WHERE { ?s ex:p1 ?o }", 2));
        assertEquals(3, graphReads("SELECT * FROM <" + UNION + "> WHERE { ?s ex:p3 ?o }", 3));
    }

    @Test
    void aPredicateNoGraphHoldsReadsNone() {
        assertEquals(0, graphReads("SELECT * FROM <" + UNION + "> WHERE { ?s ex:nothing ?o }", 0));
    }

    @Test
    void aBoundPredicateVariableNarrowsTheUnionToo() {
        long reads = graphReads("SELECT * FROM <" + UNION + "> WHERE { VALUES ?p { ex:p2 } ?s ?p ?o }", 1);
        assertTrue(reads <= 1, "only g2 holds ex:p2: " + reads);
    }

    @Test
    void anUnboundPredicateReadsEveryGraph() {
        assertEquals(5, graphReads("SELECT * FROM <" + UNION + "> WHERE { ex:h ?p ?o }", 2));
    }

    @Test
    void aJoinOverTheUnionReadsOnlyTheHoldersPerRow() {
        // ?s ex:q ?v: g4 and g5. Per row the subject is bound; the predicate still narrows.
        long reads = graphReads("SELECT * FROM <" + UNION + "> WHERE { ?s ex:q ?v . ?s ex:p3 ?o }", 2);
        assertTrue(reads <= 2 + 2 * 3, "ex:q's 2 graphs, then ex:p3's 3 per row: " + reads);
    }

    /**
     * The union read's dedup set starts small ({@link HDF5Reader#INITIAL_DEDUP_CAPACITY}) and grows:
     * 300 triples in each of three graphs, and 40 triple terms (four variables, the array-keyed set)
     * in each of two, come back once each, as Jena gives them.
     */
    @Test
    void theUnionRemovesRepeatsAsItsSetGrows() throws Exception {
        StringBuilder trig = new StringBuilder("@prefix ex: <http://ex.org/> .\n");
        for (String g : new String[] {"u1", "u2", "u3"}) {
            trig.append("ex:").append(g).append(" {");
            for (int i = 0; i < 300; i++) trig.append(" ex:s").append(i).append(" ex:r ex:o").append(i % 7).append(" .");
            if (!g.equals("u3")) {
                for (int i = 0; i < 40; i++) trig.append(" ex:t").append(i).append(" ex:says <<( ex:a").append(i).append(" ex:b ex:c").append(i % 3).append(" )>> .");
            }
            trig.append(" }\n");
        }
        Path many = dir.resolve("many.trig");
        Files.write(many, trig.toString().getBytes(StandardCharsets.UTF_8));
        File h5 = dir.resolve("many.h5").toFile();
        HDF5Writer.Builder().setSource(many.toFile()).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        DatasetGraph dsg = DatasetGraphFactory.create();
        RDFParser.create().source(many.toUri().toString()).lang(Lang.TRIG).parse(dsg);
        Dataset jena = DatasetFactory.wrap(dsg);
        try (BeakGraph g = new BeakGraph(new HDF5Reader(h5))) {
            for (String[] q : new String[][] {
                {"SELECT * FROM <" + UNION + "> WHERE { ?s ex:r ?o }", "300"},
                {"SELECT * FROM <" + UNION + "> WHERE { ?t ex:says <<( ?a ?b ?c )>> }", "40"},
            }) {
                List<String> got = rows(g.getDataset(), q[0]);
                assertEquals(rows(jena, q[0]), got, q[0]);
                assertEquals(Integer.parseInt(q[1]), got.size(), q[0]);
            }
        }
    }

    // ---------------- the GRAPH ?g walk ----------------

    @Test
    void theWalkVisitsOnlyTheGraphsHoldingEveryPredicate() {
        long walks = GraphVarBG.HITS.get();
        assertEquals(3, graphReads("SELECT ?g ?s WHERE { GRAPH ?g { ?s ex:p3 ?o } }", 4)); // ex:a ex:p3 ex:b in g3 and g4
        assertEquals(walks + 1, GraphVarBG.HITS.get(), "the walk answered");
        assertTrue(graphReads("SELECT ?g ?s WHERE { GRAPH ?g { ?s ex:p3 ?o ; ex:q ?v } }", 2) <= 2 * 2,
                "only g4 and g5 hold both");
    }

    @Test
    void noGraphHoldingEveryPredicateMeansNoRows() {
        assertEquals(0, graphReads("SELECT ?g WHERE { GRAPH ?g { ?s ex:p1 ?o ; ex:p2 ?x } }", 0));
    }

    @Test
    void aWalkWithMoreThanOnePatternKeepsItsOptionals() {
        // Not a plain BGP: every graph is visited, and the OPTIONAL keeps rows without ex:q.
        graphReads("SELECT ?g ?s ?v WHERE { GRAPH ?g { ?s ex:p3 ?o OPTIONAL { ?s ex:q ?v } } }", 4);
    }

    // ---------------- the indexes ----------------

    static List<WriterEngines.Engine> engines() {
        return WriterEngines.all().toList();
    }

    /**
     * {@link IndexReader#graphBlock} against a lookup in the index, for every graph and some ids that are not
     * graphs, at both first levels; and {@link HDF5Reader#graphsWithPredicate} against SPARQL - on every
     * writer engine.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("engines")
    void theIndexFiguresOnEveryEngine(WriterEngines.Engine engine) throws Exception {
        File h5 = dir.resolve("skip-" + engine.name() + ".h5").toFile();
        engine.buildStore(src.toFile(), h5);
        try (BeakGraph g = BG.getBeakGraph(h5)) {
            HDF5Reader reader = (HDF5Reader) g.getReader();
            PositionalDictionaryReader dict = (PositionalDictionaryReader) reader.getDictionary();
            String e = engine.name() + ": ";
            List<Long> ids = new ArrayList<>(dict.streamGraphIds().boxed().toList());
            ids.addAll(List.of(-1L, 0L, dict.getObjects().locate(NodeFactory.createURI(NS + "b")), 999_999L));
            for (Index idx : new Index[] {Index.GSPO, Index.GPOS}) {
                IndexReader ir = reader.getIndexReader(idx);
                char first = idx.name().charAt(1);
                for (long gi : ids) {
                    long[] looked = RangeSelect.lookUpFirstLevelRange(ir, first, gi);
                    long[] kept = ir.graphBlock(first, gi);
                    if (looked == null) {
                        assertNull(kept, e + idx + " graph " + gi);
                    } else {
                        assertArrayEquals(looked, kept, e + idx + " graph " + gi);
                        assertSame(kept, ir.graphBlock(first, gi), e + "kept, not looked up again");
                    }
                }
            }
            long defaultGraph = dict.getGraphs().locate(Quad.defaultGraphIRI);
            for (String p : new String[] {"p1", "p2", "p3", "q", "nothing"}) {
                long[] all = reader.graphsWithPredicate(dict.getPredicates().locate(NodeFactory.createURI(NS + p)));
                // By the term (looked up once, kept for the store's predicates): the same graphs, asked twice.
                for (int i = 0; i < 2; i++) {
                    assertArrayEquals(all, reader.graphsWithPredicate(NodeFactory.createURI(NS + p)), e + "by term, ex:" + p);
                }
                List<String> named = Arrays.stream(all).filter(id -> id != defaultGraph)
                        .mapToObj(id -> dict.getGraphs().extract(id).getURI()).sorted().collect(Collectors.toList());
                List<String> expected = new ArrayList<>();
                try (QueryExecution qe = QueryExecution.dataset(reference)
                        .query(QueryFactory.create(PREFIX + "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ex:" + p + " ?o } }")).build()) {
                    qe.execSelect().forEachRemaining(row -> expected.add(row.get("g").toString()));
                }
                Collections.sort(expected);
                assertEquals(expected, named, e + "named graphs holding ex:" + p);
                // Only ex:p1 is in the default graph too.
                assertEquals(p.equals("p1"), Arrays.stream(all).anyMatch(id -> id == defaultGraph), e + "default graph, ex:" + p);
            }
        }
    }
}
