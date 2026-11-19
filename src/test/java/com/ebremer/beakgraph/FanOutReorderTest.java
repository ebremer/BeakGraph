package com.ebremer.beakgraph;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.VoidMode;
import com.ebremer.beakgraph.hdf5.jena.OpExecutorBG;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.readers.IndexCounts;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The join reorder sees a join's fan-out: {@code ?m ex:of ?p} turns each of 4 parameters into 1,000
 * measurements, which the predicate's rows ÷ distinct objects in GPOS
 * ({@link IndexCounts#predicateRowsAndObjects}) show. Picking the pattern with the fewest matches
 * first - {@code ?p a ex:Parameter}, 4 - and the cheapest next pattern after it read all 4,000
 * measurements to keep the 100 that 50 samples have; OKN's {@code sockg/biomassCarbohydrate} went
 * from 0.45 s to 11.8 s on that. Begun at the 50 samples, the join reads 100 measurements.
 */
class FanOutReorderTest {

    private static final String NS = "http://ex.org/";
    private static final String PREFIX = "PREFIX ex: <" + NS + "> PREFIX rdfs: <" + RDFS.getURI() + "> ";
    private static final String LAB = NS + "lab";

    @TempDir
    static Path dir;
    static Path src;
    static BeakGraph bg;
    static Dataset store;
    static Dataset reference;

    /**
     * lab: 4 parameters; 4,000 measurements, each of one parameter (1,000 a parameter) with a
     * value; 50 samples of 2 measurements each; 1,000 other typed things, so the type counts are
     * not the graph's only figures. And 300 cells, each part of 40 of 100 organs - 400 subjects
     * with ex:partOf, 12,100 rows: 30 a subject - and 10 of the organs part of the abdomen.
     */
    @BeforeAll
    static void build() throws Exception {
        StringBuilder trig = new StringBuilder("@prefix ex: <" + NS + "> .\n@prefix rdfs: <" + RDFS.getURI() + "> .\nex:lab {\n");
        for (int k = 0; k < 4; k++) {
            trig.append("ex:p").append(k).append(" a ex:Parameter ; rdfs:label \"parameter ").append(k).append("\" .\n");
        }
        for (int i = 0; i < 4000; i++) {
            trig.append("ex:m").append(i).append(" ex:of ex:p").append(i % 4).append(" ; ex:value ").append(i).append(" .\n");
        }
        for (int j = 0; j < 50; j++) {
            trig.append("ex:s").append(j).append(" a ex:Sample ; ex:hasMeasurement ex:m").append(2 * j)
                .append(" , ex:m").append(2 * j + 1).append(" .\n");
        }
        for (int i = 0; i < 1000; i++) {
            trig.append("ex:x").append(i).append(" a ex:Other .\n");
        }
        for (int c = 0; c < 300; c++) {
            trig.append("ex:c").append(c).append(" a ex:Cell");
            for (int k = 0; k < 40; k++) {
                trig.append(" ; ex:partOf ex:o").append((c + k) % 100);
            }
            trig.append(" .\n");
        }
        for (int o = 0; o < 100; o++) {
            trig.append("ex:o").append(o).append(" ex:partOf ex:").append(o < 10 ? "abdomen" : "elsewhere").append(" .\n");
        }
        trig.append("}\n");
        src = dir.resolve("lab.trig");
        Files.writeString(src, trig);
        File h5 = dir.resolve("lab.h5").toFile();
        HDF5Writer.Builder().setVoidMode(VoidMode.EXACT).setSource(src.toFile()).setDestination(h5)
                .setSpatial(false).setFeatures(false).build().write();
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

    /** Runs {@code query} on the store, asserts Jena's answer and its row count, and returns how many BGPs were reordered. */
    private static long reorders(String query, int expectedRows) {
        long before = OpExecutorBG.REORDERS.get();
        List<String> got = rows(store, query);
        long reordered = OpExecutorBG.REORDERS.get() - before;
        assertEquals(rows(reference, query), got, query);
        assertEquals(expectedRows, got.size(), query);
        return reordered;
    }

    private static final Var S = Var.alloc("s"), M = Var.alloc("m"), P = Var.alloc("p"), V = Var.alloc("v"), L = Var.alloc("l");
    private static Node ex(String local) {
        return NodeFactory.createURI(NS + local);
    }

    /** The samples' measurements with their parameters, written beginning at the 4 parameters. */
    private static List<Triple> fromTheParameters() {
        return List.of(
                Triple.create(P, RDF.type.asNode(), ex("Parameter")),
                Triple.create(P, RDFS.label.asNode(), L),
                Triple.create(M, ex("of"), P),
                Triple.create(M, ex("value"), V),
                Triple.create(S, ex("hasMeasurement"), M),
                Triple.create(S, RDF.type.asNode(), ex("Sample")));
    }

    @Test
    void theLabsViewBeginsAtTheSamples() {
        // At the 50 samples or at their 100 measurements - about the same - never at the parameters.
        BeakGraph lab = (BeakGraph) store.asDatasetGraph().getGraph(ex("lab"));
        List<Triple> written = fromTheParameters();
        BasicPattern out = lab.getReorderTransform().reorder(BasicPattern.wrap(written));
        assertTrue(out.get(0).equals(written.get(5)) || out.get(0).equals(written.get(4)), "the samples first: " + out);
        assertTrue(out.getList().indexOf(written.get(4)) < out.getList().indexOf(written.get(2)),
                "each measurement reached from its sample, not from its parameter: " + out);
    }

    @Test
    void aQueryBegunAtTheParametersIsReordered() {
        String q = "SELECT ?s ?v ?l FROM <" + LAB + "> WHERE { ?p a ex:Parameter ; rdfs:label ?l . ?m ex:of ?p ; ex:value ?v ."
                + " ?s ex:hasMeasurement ?m ; a ex:Sample }";
        assertEquals(1, reorders(q, 100));
    }

    @Test
    void aQueryBegunAtTheSamplesIsLeftAsWritten() {
        // The cheapest-pattern-first pick moved ?p a ex:Parameter (4 matches) to the front.
        String q = "SELECT ?s ?p FROM <" + LAB + "> WHERE { ?s ex:hasMeasurement ?m ; a ex:Sample . ?m ex:of ?p . ?p a ex:Parameter }";
        assertEquals(0, reorders(q, 100));
    }

    @Test
    void theSameInAGraphWalk() {
        String q = "SELECT ?s ?p WHERE { GRAPH ?g { ?s ex:hasMeasurement ?m ; a ex:Sample . ?m ex:of ?p . ?p a ex:Parameter } }";
        assertEquals(0, reorders(q, 100));
    }

    /** The cells of the abdomen's organs, written beginning at every cell. */
    private static List<Triple> fromTheCells() {
        Var c = Var.alloc("c"), o = Var.alloc("o");
        return List.of(
                Triple.create(c, RDF.type.asNode(), ex("Cell")),
                Triple.create(c, ex("partOf"), o),
                Triple.create(o, ex("partOf"), ex("abdomen")));
    }

    @Test
    void theAbdomensCellsAreReachedFromItsOrgans() {
        // Divided by every subject in the graph (5,454), ex:partOf looked like 2 rows a subject, and
        // reading 300 cells' 12,000 parts looked cheaper than 10 organs' 1,200 cells.
        BeakGraph lab = (BeakGraph) store.asDatasetGraph().getGraph(ex("lab"));
        List<Triple> written = fromTheCells();
        BasicPattern out = lab.getReorderTransform().reorder(BasicPattern.wrap(written));
        assertEquals(written.get(2), out.get(0), "the 10 organs of the abdomen first: " + out);
    }

    @Test
    void aQueryBegunAtTheCellsIsReordered() {
        String q = "SELECT ?c ?o FROM <" + LAB + "> WHERE { ?c a ex:Cell ; ex:partOf ?o . ?o ex:partOf ex:abdomen }";
        assertEquals(1, reorders(q, 1200));
    }

    // ---------------- the counts themselves ----------------

    static List<WriterEngines.Engine> engines() {
        return WriterEngines.all().toList();
    }

    /**
     * {@code IndexCounts.predicateRowsAndObjects}, {@code distinctSubjectsWithPredicate} and
     * {@code samplePairs} against COUNT(*), COUNT(DISTINCT ?o) and COUNT(DISTINCT ?s), on every
     * writer engine's indexes.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("engines")
    void predicateFiguresMatchCounts(WriterEngines.Engine engine) throws Exception {
        File h5 = dir.resolve("counts-" + engine.name() + ".h5").toFile();
        engine.buildStore(src.toFile(), h5);
        try (BeakGraph g = BG.getBeakGraph(h5)) {
            HDF5Reader reader = (HDF5Reader) g.getReader();
            PositionalDictionaryReader dict = (PositionalDictionaryReader) reader.getDictionary();
            long lab = dict.getGraphs().locate(ex("lab"));
            String e = engine.name() + ": ";
            IndexCounts.PairSample pairs = IndexCounts.samplePairs(reader, lab, 1 << 14);
            assertTrue(pairs.exact(), e + "the lab's ~9,800 pairs are all read");
            for (String p : new String[] {"of", "value", "hasMeasurement", "partOf"}) {
                long pi = dict.getPredicates().locate(ex(p));
                long[] counted = IndexCounts.predicateRowsAndObjects(reader, lab, pi);
                long subjects = IndexCounts.distinctSubjectsWithPredicate(reader, lab, pi);
                List<String> star = rows(g.getDataset(), "SELECT (COUNT(*) AS ?n) (COUNT(DISTINCT ?o) AS ?d) (COUNT(DISTINCT ?s) AS ?ds) FROM <"
                        + LAB + "> WHERE { ?s ex:" + p + " ?o }");
                assertEquals(List.of("n=\"" + counted[0] + "\"^^xsd:integer d=\"" + counted[1] + "\"^^xsd:integer ds=\"" + subjects
                        + "\"^^xsd:integer "), star, e + p);
                assertEquals(subjects, pairs.subjects(pi), e + p + ": a predicate's pairs are its distinct subjects");
            }
            long partOf = dict.getPredicates().locate(ex("partOf"));
            assertEquals(400, IndexCounts.distinctSubjectsWithPredicate(reader, lab, partOf), e + "ex:partOf: 300 cells and 100 organs");
            // A sample of 1,000 of the ~9,800 pairs: ex:of's 4,000 subjects within 15%.
            double sampled = IndexCounts.samplePairs(reader, lab, 1000).subjects(dict.getPredicates().locate(ex("of")));
            assertTrue(Math.abs(sampled - 4000) < 600, e + "ex:of's subjects from a sample: " + sampled);
            long type = dict.getPredicates().locate(RDF.type.asNode());
            assertArrayEquals(new long[] {4 + 50 + 1000 + 300, 4}, IndexCounts.predicateRowsAndObjects(reader, lab, type), e + "rdf:type");
            assertEquals(0, IndexCounts.distinctSubjectsWithPredicate(reader, 999_999, type), e + "an absent graph");
            assertArrayEquals(new long[] {4000, 4}, IndexCounts.predicateRowsAndObjects(reader, lab, dict.getPredicates().locate(ex("of"))), e + "ex:of");
            assertArrayEquals(new long[] {0, 0}, IndexCounts.predicateRowsAndObjects(reader, 999_999, type), e + "an absent graph");
            assertArrayEquals(new long[] {0, 0}, IndexCounts.predicateRowsAndObjects(reader, lab, 0), e + "an absent predicate");
        }
    }
}
