package com.ebremer.beakgraph.hdf5.jena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.apache.jena.sparql.expr.E_GreaterThanOrEqual;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.expr.ExprVar;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.util.NodeFactoryExtra;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression tests for BG-329: range-filter pushdown snapped its bound to the
 * {@link ValueCluster} of ADJACENT value-equal ids, which presumes the
 * dictionary orders the constant's value space the way ARQ compares it.
 * Two spaces break that: composite (cdt:List / cdt:Map) literals are stored
 * in exact lexical order while ARQ compares by value ("[1, 2]" and "[1,2]"
 * are ARQ-equal but separated by "[1, 3]"), and language-tagged literals are
 * blocked by exact tag while ARQ matches tags case-insensitively ("m"@EN and
 * "m"@en). Bounds derived from such constants landed inside the ARQ-equal set
 * and silently dropped boundary rows. Constants from those spaces are no
 * longer range hints at all; the OpFilter answers the comparison alone.
 * <p>
 * The composite case is reproduced here. The language case is not reachable
 * from parsed data with Jena 6, which canonicalises tag case at node creation
 * (asserted below so a change in that behaviour surfaces); its gate is kept
 * as cheap defence and the queries are checked against Jena regardless.
 */
class FilterBoundsValueSpaceTest {

    private static final String CDT = "http://w3id.org/awslabs/neptune/SPARQL-CDTs/";
    private static final String TTL = """
        @prefix ex:  <http://ex.org/> .
        @prefix cdt: <http://w3id.org/awslabs/neptune/SPARQL-CDTs/> .
        ex:l1 ex:list "[1, 2]"^^cdt:List .
        ex:l2 ex:list "[1, 3]"^^cdt:List .
        ex:l3 ex:list "[1,2]"^^cdt:List .
        ex:l4 ex:list "[0]"^^cdt:List .
        ex:l5 ex:list "[2]"^^cdt:List .
        ex:m1 ex:map "{\\"a\\" : 1}"^^cdt:Map .
        ex:m2 ex:map "{\\"a\\":1}"^^cdt:Map .
        ex:m3 ex:map "{\\"b\\":1}"^^cdt:Map .
        ex:n1 ex:name "m"@EN .
        ex:n2 ex:name "m"@en .
        ex:n3 ex:name "n"@en .
        ex:n4 ex:name "a"@en .
        ex:n5 ex:name "m"@fr .
        ex:n6 ex:name "z"@en-GB .
        ex:n7 ex:name "z"@EN-gb .
        """;
    private static final String PREFIX =
        "PREFIX ex: <http://ex.org/> PREFIX cdt: <" + CDT + "> ";

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;
    static Dataset reference;

    @BeforeAll
    static void buildAndOpen() throws Exception {
        File src = dir.resolve("spaces.ttl").toFile();
        File h5 = dir.resolve("spaces.ttl.h5").toFile();
        Files.writeString(src.toPath(), TTL, StandardCharsets.UTF_8);
        HDF5Writer.Builder().setSource(src).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        bg = new BeakGraph(new HDF5Reader(h5));
        ds = bg.getDataset();
        Model m = ModelFactory.createDefaultModel();
        RDFDataMgr.read(m, new StringReader(TTL), null, Lang.TURTLE);
        reference = DatasetFactory.create(m);
    }

    @AfterAll
    static void close() {
        if (bg != null) bg.close();
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

    private static void assertMatchesJena(String query, boolean expectRows) {
        List<String> expected = rows(reference, query);
        if (expectRows) {
            assertFalse(expected.isEmpty(), "reference has no rows; the case would be vacuous: " + query);
        }
        assertEquals(expected, rows(ds, query), query);
    }

    /** Every ordering comparison, constant on either side, over a bound-predicate and an all-variable pattern. */
    private static List<String> comparisons(String pattern, String var, String constant) {
        List<String> qs = new ArrayList<>();
        for (String op : new String[]{">", ">=", "<", "<="}) {
            qs.add(PREFIX + "SELECT * WHERE { " + pattern + " FILTER(" + var + " " + op + " " + constant + ") }");
            qs.add(PREFIX + "SELECT * WHERE { " + pattern + " FILTER(" + constant + " " + op + " " + var + ") }");
        }
        return qs;
    }

    @Test
    void compositeListConstantsMatchJena() {
        for (String c : new String[]{"\"[1,2]\"^^cdt:List", "\"[1, 2]\"^^cdt:List", "\"[1, 3]\"^^cdt:List"}) {
            for (String q : comparisons("?s ex:list ?l", "?l", c)) assertMatchesJena(q, false);
            for (String q : comparisons("?s ?p ?l", "?l", c)) assertMatchesJena(q, false);
        }
        // Not vacuous: ARQ does order lists by value, so >= "[1,2]" must return
        // the two spellings of [1,2] as well as [1,3] and [2].
        assertMatchesJena(PREFIX + "SELECT ?s WHERE { ?s ex:list ?l FILTER(?l >= \"[1,2]\"^^cdt:List) }", true);
        assertEquals(4, rows(ds, PREFIX + "SELECT ?s WHERE { ?s ex:list ?l FILTER(?l >= \"[1,2]\"^^cdt:List) }").size());
    }

    @Test
    void compositeMapConstantsMatchJena() {
        for (String c : new String[]{"\"{\\\"a\\\":1}\"^^cdt:Map", "\"{\\\"a\\\" : 1}\"^^cdt:Map"}) {
            for (String q : comparisons("?s ex:map ?m", "?m", c)) assertMatchesJena(q, false);
        }
    }

    @Test
    void languageTaggedConstantsMatchJena() {
        for (String c : new String[]{"\"m\"@en", "\"m\"@EN", "\"a\"@en", "\"z\"@en-gb", "\"m\"@fr"}) {
            for (String q : comparisons("?s ex:name ?n", "?n", c)) assertMatchesJena(q, false);
            for (String q : comparisons("?s ?p ?n", "?n", c)) assertMatchesJena(q, false);
        }
        // Not vacuous: same-tag rows compare by lexical form, so >= "m"@en must
        // return n1, n2 and n3.
        String q = PREFIX + "SELECT ?s WHERE { ?s ex:name ?n FILTER(?n >= \"m\"@en) }";
        assertMatchesJena(q, true);
        assertEquals(3, rows(ds, q).size());
    }

    @Test
    void jenaCanonicalisesLanguageTagCase() {
        // "m"@EN and "m"@en are ONE term in Jena 6, in the store and in the
        // query: the language half of BG-329 cannot arise from parsed data. If
        // this stops holding, the language gate in FilterBounds becomes
        // load-bearing and this class needs a genuine mixed-case fixture.
        assertEquals("en", NodeFactory.createLiteralLang("m", "EN").getLiteralLanguage());
        String q = PREFIX + "SELECT ?s WHERE { ?s ex:name \"m\"@EN }";
        assertEquals(2, rows(ds, q).size(), "n1 and n2 hold the same canonical term");
        assertMatchesJena(q, true);
    }

    @Test
    void valueOrderedSpacesStillProduceHints() {
        assertTrue(FilterBounds.orderAgreesWithArq(NodeFactoryExtra.intToNode(5)));
        assertTrue(FilterBounds.orderAgreesWithArq(NodeFactory.createLiteralString("abc")));
        assertTrue(FilterBounds.orderAgreesWithArq(NodeFactory.createURI("http://ex.org/x")));
        assertFalse(FilterBounds.orderAgreesWithArq(NodeFactory.createLiteralLang("m", "en")));
        assertFalse(FilterBounds.orderAgreesWithArq(
                NodeValue.parse("\"[1,2]\"^^<" + CDT + "List>").asNode()));
        assertFalse(FilterBounds.orderAgreesWithArq(
                NodeValue.parse("\"{\\\"a\\\":1}\"^^<" + CDT + "Map>").asNode()));

        // The scan itself drops the unsound hint and keeps the sound one.
        ExprList filter = new ExprList();
        filter.add(new E_GreaterThanOrEqual(new ExprVar("o"), NodeValue.makeInteger(5)));
        filter.add(new E_GreaterThanOrEqual(new ExprVar("o"), NodeValue.makeLangString("m", "en")));
        filter.add(new E_GreaterThanOrEqual(new ExprVar("o"), NodeValue.parse("\"[1,2]\"^^<" + CDT + "List>")));
        List<String> seen = new ArrayList<>();
        FilterBounds.scan(filter, (var, op, value) -> seen.add(var + " " + op + " " + value));
        assertEquals(List.of("?o >= \"5\"^^xsd:integer"), seen);
    }
}
