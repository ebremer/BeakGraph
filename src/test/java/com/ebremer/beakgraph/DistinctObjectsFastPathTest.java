package com.ebremer.beakgraph;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.jena.DistinctTermFastPath;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code SELECT DISTINCT ?o WHERE { ?s <p> ?o }} answered by {@link DistinctTermFastPath} from the GPOS object
 * level under the predicate - one graph's list streamed, several graphs' merged without repeats - must return
 * exactly Jena's answer over the same data, in every graph form; and every other shape falls back. OKN's
 * {@code scales/scales-ontology-event-labels} read every docket entry to de-duplicate 82 labels: 11.8 s.
 */
class DistinctObjectsFastPathTest {

    private static final String NS = "http://ex.org/";
    private static final String PREFIX = "PREFIX ex: <" + NS + "> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n";
    private static final String UNION = "urn:x-arq:UnionGraph";

    /**
     * ex:label: in the default graph "a" twice, and 1 beside "01"^^xsd:integer - equal values, two terms; in g1 and
     * g2 "d" in both, so the union holds it once; none in g3.
     */
    private static final String TRIG = """
            @prefix ex: <http://ex.org/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
            ex:s1 ex:label "a", "b" .
            ex:s2 ex:label "a" .
            ex:s3 ex:label "c"@en, 1, "01"^^xsd:integer .
            ex:s4 ex:other ex:x .
            ex:g1 { ex:a ex:label "a", "d" . ex:b ex:label ex:iri1 . }
            ex:g2 { ex:c ex:label "d", "e" . ex:c ex:other "a" . }
            ex:g3 { ex:e ex:other "z" . }
            """;

    @TempDir
    static Path dir;
    static Path src;
    static BeakGraph bg;
    static Dataset store;
    static Dataset reference;

    @BeforeAll
    static void build() throws Exception {
        src = dir.resolve("objects.trig");
        Files.write(src, TRIG.getBytes(StandardCharsets.UTF_8));
        File h5 = dir.resolve("objects.h5").toFile();
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

    private static List<String> terms(Dataset d, String query) {
        List<String> out = new ArrayList<>();
        try (QueryExecution qe = QueryExecution.dataset(d).query(QueryFactory.create(PREFIX + query)).build()) {
            var rs = qe.execSelect();
            String var = rs.getResultVars().get(0);
            rs.forEachRemaining(row -> out.add(row.get(var).toString()));
        }
        return out;
    }

    /** Asserts the store's answer is Jena's (as a set, or in order with ORDER BY) and whether the fast path gave it. */
    private static List<String> check(String query, boolean expectFastPath) {
        long before = DistinctTermFastPath.HITS.get();
        List<String> got = terms(store, query);
        assertEquals(expectFastPath ? 1 : 0, DistinctTermFastPath.HITS.get() - before, "fast-path activations for: " + query);
        List<String> expected = terms(reference, query);
        if (query.contains("ORDER BY")) {
            assertEquals(expected, got, query);
        } else {
            assertEquals(new HashSet<>(expected), new HashSet<>(got), query);
            assertEquals(got.size(), new HashSet<>(got).size(), "no repeats: " + query);
        }
        return got;
    }

    private static Set<String> set(List<String> l) {
        return new HashSet<>(l);
    }

    @Test
    void theDefaultGraphsObjects() {
        // "a" once; 1 and "01"^^xsd:integer are two terms.
        assertEquals(5, check("SELECT DISTINCT ?o WHERE { ?s ex:label ?o }", true).size());
    }

    @Test
    void aNamedGraphsObjects() {
        assertEquals(Set.of("a", "d", NS + "iri1"), set(check("SELECT DISTINCT ?o WHERE { GRAPH ex:g1 { ?s ex:label ?o } }", true)));
        assertEquals(Set.of("d", "e"), set(check("SELECT DISTINCT ?o FROM ex:g2 WHERE { ?s ex:label ?o }", true)));
    }

    @Test
    void theUnionGraphHoldsAnObjectInTwoGraphsOnce() {
        assertEquals(Set.of("a", "d", NS + "iri1", "e"), set(check("SELECT DISTINCT ?o FROM <" + UNION + "> WHERE { ?s ex:label ?o }", true)));
        check("SELECT DISTINCT ?o WHERE { GRAPH <" + UNION + "> { ?s ex:label ?o } }", true);
    }

    @Test
    void everyNamedGraphUnderAGraphVariable() {
        assertEquals(Set.of("a", "d", NS + "iri1", "e"), set(check("SELECT DISTINCT ?o WHERE { GRAPH ?g { ?s ex:label ?o } }", true)));
        // FROM NAMED makes a dataset of its own: as for DISTINCT ?p, only the store's is read off the index.
        check("SELECT DISTINCT ?o FROM NAMED ex:g2 FROM NAMED ex:g3 WHERE { GRAPH ?g { ?s ex:label ?o } }", false);
    }

    @Test
    void inOrder() {
        check("SELECT DISTINCT ?o WHERE { ?s ex:label ?o } ORDER BY ?o", true);
        check("SELECT DISTINCT ?o FROM <" + UNION + "> WHERE { ?s ex:label ?o } ORDER BY DESC(?o)", true);
    }

    @Test
    void aLimitStopsTheRead() {
        long before = DistinctTermFastPath.HITS.get();
        List<String> got = terms(store, "SELECT DISTINCT ?o FROM <" + UNION + "> WHERE { ?s ex:label ?o } LIMIT 2");
        assertEquals(1, DistinctTermFastPath.HITS.get() - before);
        assertEquals(2, set(got).size());
        assertTrue(Set.of("a", "d", NS + "iri1", "e").containsAll(got), got.toString());
    }

    @Test
    void anAbsentPredicateOrGraphHasNone() {
        assertEquals(List.of(), check("SELECT DISTINCT ?o WHERE { ?s ex:nothing ?o }", true));
        assertEquals(List.of(), check("SELECT DISTINCT ?o WHERE { GRAPH ex:g3 { ?s ex:label ?o } }", true));
        assertEquals(List.of(), check("SELECT DISTINCT ?o WHERE { GRAPH ex:nope { ?s ex:label ?o } }", true));
    }

    @Test
    void otherShapesFallBack() {
        check("SELECT DISTINCT ?s WHERE { ?s ex:label ?o }", false);
        check("SELECT DISTINCT ?o WHERE { ?o ex:label ?o }", false);
        check("SELECT DISTINCT ?o WHERE { ?s ex:label ?o FILTER(isLiteral(?o)) }", false);
        check("SELECT DISTINCT ?o WHERE { VALUES ?s { ex:s1 } ?s ex:label ?o }", false);
        check("SELECT DISTINCT ?o WHERE { ex:s1 ex:label ?o }", false);
        check("SELECT DISTINCT ?o WHERE { GRAPH ?g { ?g ex:label ?o } }", false);
        check("SELECT DISTINCT ?o WHERE { VALUES ?g { ex:g1 } GRAPH ?g { ?s ex:label ?o } }", false);
        check("SELECT DISTINCT ?o FROM ex:g1 FROM ex:g2 WHERE { ?s ex:label ?o }", false);
        check("SELECT DISTINCT ?o WHERE { ?s ex:label ?o . ?s ex:other ?x }", false);
    }

    /** Every writer engine, the disk-based ones included: the fast path's answers are Jena's. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("com.ebremer.beakgraph.WriterEngines#all")
    void onEveryEngine(WriterEngines.Engine engine) throws Exception {
        File h5 = dir.resolve("objects-" + engine.name() + ".h5").toFile();
        engine.buildStore(src.toFile(), h5);
        try (BeakGraph other = BG.getBeakGraph(h5)) {
            for (String q : new String[] {
                    "SELECT DISTINCT ?o WHERE { ?s ex:label ?o } ORDER BY ?o",
                    "SELECT DISTINCT ?o FROM <" + UNION + "> WHERE { ?s ex:label ?o } ORDER BY ?o",
                    "SELECT DISTINCT ?o WHERE { GRAPH ?g { ?s ex:label ?o } } ORDER BY ?o",
                    "SELECT DISTINCT ?o WHERE { GRAPH ex:g1 { ?s ex:label ?o } } ORDER BY ?o"}) {
                long before = DistinctTermFastPath.HITS.get();
                List<String> got = terms(other.getDataset(), q);
                assertEquals(1, DistinctTermFastPath.HITS.get() - before, engine.name() + ": fast path for " + q);
                assertEquals(terms(reference, q), got, engine.name() + ": " + q);
            }
        }
    }
}
