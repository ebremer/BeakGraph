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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.jena.graph.Node;
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
import org.apache.jena.vocabulary.RDF;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The join reorder costs {@code ?s <p> <o>} by its exact matches in the graphs a query reads
 * ({@link IndexCounts#quadsWithPredicate}). The store-wide estimate divides by the distinct
 * objects; here, as in the 38-billion-quad OKN store, that puts both patterns of
 * {@code ?f a ex:Facility ; ex:code ex:c7} below one match, so they tied and ran as written:
 * every facility first, then each one's code. With the counts the 5 matches of the code run
 * first. OKN's {@code federation/fio-spatial-facilities-in-state} timed out on exactly this.
 */
class ExactCountReorderTest {

    private static final String NS = "http://ex.org/";
    private static final String PREFIX = "PREFIX ex: <" + NS + "> ";
    private static final String UNION = "urn:x-arq:UnionGraph";

    @TempDir
    static Path dir;
    static Path src;
    static BeakGraph bg;
    static Dataset store;
    static Dataset reference;

    /**
     * g1: 2000 facilities, 400 codes of 5 facilities each. g2: 100 facilities without a code and
     * 3000 distinct literals, which lift the distinct objects (~3,400) above both predicates'
     * counts (2,100 and 2,000) - so both estimates fall below 1 and tie.
     */
    @BeforeAll
    static void build() throws Exception {
        StringBuilder trig = new StringBuilder("@prefix ex: <" + NS + "> .\nex:d ex:val \"d\" .\nex:g1 {\n");
        for (int i = 0; i < 2000; i++) {
            trig.append("ex:f").append(i).append(" a ex:Facility ; ex:code ex:c").append(i % 400).append(" .\n");
        }
        trig.append("}\nex:g2 {\n");
        for (int i = 0; i < 100; i++) {
            trig.append("ex:h").append(i).append(" a ex:Facility .\n");
        }
        for (int i = 0; i < 3000; i++) {
            trig.append("ex:x").append(i).append(" ex:val \"v").append(i).append("\" .\n");
        }
        trig.append("}\n");
        src = dir.resolve("facilities.trig");
        Files.writeString(src, trig);
        File h5 = dir.resolve("facilities.h5").toFile();
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

    @Test
    void theCodeRunsFirstInANamedGraph() {
        assertEquals(1, reorders("SELECT ?f FROM <" + NS + "g1> WHERE { ?f a ex:Facility ; ex:code ex:c7 }", 5));
    }

    @Test
    void theCodeRunsFirstOverTheUnionGraph() {
        assertEquals(1, reorders("SELECT ?f FROM <" + UNION + "> WHERE { ?f a ex:Facility ; ex:code ex:c7 }", 5));
    }

    @Test
    void theCodeRunsFirstInAGraphWalk() {
        assertEquals(1, reorders("SELECT ?g ?f WHERE { GRAPH ?g { ?f a ex:Facility ; ex:code ex:c7 } }", 5));
    }

    @Test
    void theCodeRunsFirstWhenValuesBindsIt() {
        assertEquals(1, reorders("SELECT ?f FROM <" + NS + "g1> WHERE { VALUES ?c { ex:c7 } ?f a ex:Facility ; ex:code ?c }", 5));
    }

    @Test
    void aQueryAlreadyInTheRightOrderIsLeftAsWritten() {
        assertEquals(0, reorders("SELECT ?f FROM <" + NS + "g1> WHERE { ?f ex:code ex:c7 ; a ex:Facility }", 5));
    }

    @Test
    void anAbsentCodeRunsFirstAndEndsTheQuery() {
        assertEquals(1, reorders("SELECT ?f FROM <" + NS + "g1> WHERE { ?f a ex:Facility ; ex:code ex:nothing }", 0));
    }

    // ---------------- the counts themselves ----------------

    static List<WriterEngines.Engine> engines() {
        return WriterEngines.all().toList();
    }

    /** {@code IndexCounts.quadsWithPredicate} against COUNT(*), on every writer engine's GPOS. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("engines")
    void exactCountsMatchCountStar(WriterEngines.Engine engine) throws Exception {
        File h5 = dir.resolve("counts-" + engine.name() + ".h5").toFile();
        engine.buildStore(src.toFile(), h5);
        try (BeakGraph g = BG.getBeakGraph(h5)) {
            HDF5Reader reader = (HDF5Reader) g.getReader();
            PositionalDictionaryReader dict = (PositionalDictionaryReader) reader.getDictionary();
            Node type = RDF.type.asNode();
            Node code = NodeFactory.createURI(NS + "code");
            Node facility = NodeFactory.createURI(NS + "Facility");
            long g1 = dict.getGraphs().locate(NodeFactory.createURI(NS + "g1"));
            long g2 = dict.getGraphs().locate(NodeFactory.createURI(NS + "g2"));
            long pType = dict.getPredicates().locate(type), pCode = dict.getPredicates().locate(code);
            long oFacility = dict.getObjects().locate(facility), oC7 = dict.getObjects().locate(NodeFactory.createURI(NS + "c7"));
            String e = engine.name() + ": ";
            assertEquals(2000, IndexCounts.quadsWithPredicate(reader, g1, pType, oFacility), e + "?s a ex:Facility in g1");
            assertEquals(100, IndexCounts.quadsWithPredicate(reader, g2, pType, oFacility), e + "?s a ex:Facility in g2");
            assertEquals(5, IndexCounts.quadsWithPredicate(reader, g1, pCode, oC7), e + "?s ex:code ex:c7 in g1");
            assertEquals(2000, IndexCounts.quadsWithPredicate(reader, g1, pCode, 0), e + "?s ex:code ?o in g1");
            assertEquals(0, IndexCounts.quadsWithPredicate(reader, g2, pCode, 0), e + "no codes in g2");
            assertEquals(0, IndexCounts.quadsWithPredicate(reader, g1, pCode, oFacility), e + "an object the predicate never has");
            assertEquals(0, IndexCounts.quadsWithPredicate(reader, 999_999, pType, oFacility), e + "an absent graph");
            assertEquals(0, IndexCounts.quadsWithPredicate(reader, g1, 0, 0), e + "an absent predicate");
            for (String[] c : new String[][] {{"g1", "?s a ex:Facility"}, {"g2", "?s a ex:Facility"}, {"g1", "?s ex:code ex:c7"}}) {
                long id = dict.getGraphs().locate(NodeFactory.createURI(NS + c[0]));
                long counted = c[1].contains("code")
                        ? IndexCounts.quadsWithPredicate(reader, id, pCode, oC7)
                        : IndexCounts.quadsWithPredicate(reader, id, pType, oFacility);
                List<String> star = rows(g.getDataset(), "SELECT (COUNT(*) AS ?n) FROM <" + NS + c[0] + "> WHERE { " + c[1] + " }");
                assertEquals(List.of("n=\"" + counted + "\"^^xsd:integer "), star, e + c[1] + " in " + c[0]);
            }
        }
    }

    /** {@code HDF5Reader.graphIdsOf}: the graphs a read visits, for each kind of view. */
    @Test
    void graphIdsAreTheGraphsAReadVisits() {
        HDF5Reader reader = (HDF5Reader) bg.getReader();
        PositionalDictionaryReader dict = (PositionalDictionaryReader) reader.getDictionary();
        long g1 = dict.getGraphs().locate(NodeFactory.createURI(NS + "g1"));
        long g2 = dict.getGraphs().locate(NodeFactory.createURI(NS + "g2"));
        long dflt = dict.getGraphs().locate(Quad.defaultGraphIRI);
        Set<Long> union = ids(reader.graphIdsOf(List.of(Quad.unionGraph)));
        assertTrue(union.containsAll(Set.of(g1, g2)), "the union graph is every named graph: " + union);
        assertFalse(union.contains(dflt), "and never the default graph");
        assertEquals(Set.of(g1), ids(reader.graphIdsOf(List.of(NodeFactory.createURI(NS + "g1")))));
        assertEquals(Set.of(dflt), ids(reader.graphIdsOf(List.of(Quad.defaultGraphIRI))));
        assertEquals(Set.of(g1, dflt), ids(reader.graphIdsOf(List.of(NodeFactory.createURI(NS + "g1"), Quad.defaultGraphIRI))));
        assertEquals(Set.of(), ids(reader.graphIdsOf(List.of(NodeFactory.createURI(NS + "absent")))));
    }

    private static Set<Long> ids(long[] a) {
        return Arrays.stream(a).boxed().collect(Collectors.toSet());
    }
}
