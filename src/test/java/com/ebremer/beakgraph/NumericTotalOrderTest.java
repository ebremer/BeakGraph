package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.lib.NodeComparator;
import com.ebremer.beakgraph.core.lib.NumericOrder;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.io.StringReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.jena.datatypes.xsd.XSDDatatype;
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
import org.apache.jena.sparql.expr.NodeValue;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression tests for BG-18: NodeComparator compared mixed numeric datatypes
 * through NodeValue.compareAlways, i.e. SPARQL promotion (decimal vs float
 * as floats), which is lossy; same-datatype decimals compared exactly; and
 * ties broke lexically. Mixing an exact order with a lossy one and a lexical
 * tie-break is cyclic - a cyclic comparator makes sorts input-order dependent
 * and binary searches miss stored terms. Numbers now order by exact value,
 * and range pushdown widens its bounds to the promotion neighbourhood so
 * ARQ's lossy comparisons still find every row.
 */
class NumericTotalOrderTest {

    private static final NodeComparator CMP = NodeComparator.INSTANCE;

    private static Node dec(String lex) { return NodeFactory.createLiteralDT(lex, XSDDatatype.XSDdecimal); }
    private static Node flt(String lex) { return NodeFactory.createLiteralDT(lex, XSDDatatype.XSDfloat); }
    private static Node dbl(String lex) { return NodeFactory.createLiteralDT(lex, XSDDatatype.XSDdouble); }
    private static Node integer(String lex) { return NodeFactory.createLiteralDT(lex, XSDDatatype.XSDinteger); }

    private static void assertTotallyOrdered(List<Node> nodes) {
        for (int i = 0; i < nodes.size(); i++) {
            for (int j = 0; j < nodes.size(); j++) {
                int ij = Integer.signum(CMP.compare(nodes.get(i), nodes.get(j)));
                int ji = Integer.signum(CMP.compare(nodes.get(j), nodes.get(i)));
                assertEquals(-ij, ji, "antisymmetry: " + nodes.get(i) + " vs " + nodes.get(j));
                if (i != j) assertTrue(ij != 0, "distinct terms must not tie: " + nodes.get(i) + " vs " + nodes.get(j));
                for (int k = 0; k < nodes.size(); k++) {
                    int jk = Integer.signum(CMP.compare(nodes.get(j), nodes.get(k)));
                    int ik = Integer.signum(CMP.compare(nodes.get(i), nodes.get(k)));
                    if (ij < 0 && jk < 0) {
                        assertTrue(ik < 0, "cycle: " + nodes.get(i) + " < " + nodes.get(j) + " < " + nodes.get(k) + " but not " + nodes.get(i) + " < " + nodes.get(k));
                    }
                }
            }
        }
    }

    @Test
    void decimalFloatPromotionCycleIsGone() {
        // Exactly: 0.100000000099 < 0.10000000010. Both promote to 0.1f, as does
        // the float, so compareAlways tied each with it and broke the tie
        // lexically: "…0010" < "…005" < "…0099" - a 3-cycle.
        Node a = dec("0.100000000099");
        Node b = dec("0.10000000010");
        Node f = flt("0.10000000005");
        assertEquals(0, Integer.signum(NodeValue.compare(NodeValue.makeNode(a), NodeValue.makeNode(f))),
                "precondition: ARQ promotion ties the decimal with the float");
        assertTotallyOrdered(List.of(a, b, f));
        assertTrue(CMP.compare(a, b) < 0);
        assertTrue(CMP.compare(b, f) < 0, "0.1000000001 < 0.1f (= 0.100000001490116…) exactly");
    }

    @Test
    void mixedNumericLiteralsAreTotallyOrdered() {
        Random rnd = new Random(18);
        List<Node> nodes = new ArrayList<>(List.of(
                flt("NaN"), dbl("NaN"), flt("INF"), dbl("-INF"), dbl("INF"), flt("-INF"),
                integer("9007199254740993"), dec("9007199254740992.5"), dbl("9007199254740992"),
                integer("9999999999999999999"), integer("10000000000000000000"),
                flt("0.1"), dec("0.1"), dbl("0.1"), dec("0.100000001490116119384765625"),
                integer("1"), dec("1.0"), flt("1"), dbl("1.0"), integer("-1")));
        for (int i = 0; i < 40; i++) {
            long base = rnd.nextLong() >> rnd.nextInt(60);
            nodes.add(integer(Long.toString(base)));
            nodes.add(dec(base + "." + rnd.nextInt(1000)));
            nodes.add(flt(Float.toString(base * 1e-3f)));
            nodes.add(dbl(Double.toString(base * 1e-3)));
        }
        assertTotallyOrdered(nodes);
        // The order agrees with exact numeric value wherever values differ.
        Collections.shuffle(nodes, rnd);
        nodes.sort(CMP);
        for (int i = 1; i < nodes.size(); i++) {
            NodeValue p = NodeValue.makeNode(nodes.get(i - 1)), q = NodeValue.makeNode(nodes.get(i));
            assertTrue(NumericOrder.compare(p, q) <= 0, nodes.get(i - 1) + " sorted before " + nodes.get(i));
        }
    }

    @Test
    void promotionEdgesBracketEveryValueArqAcceptsAsEqual() {
        NodeValue c = NodeValue.makeNode(flt("0.1"));
        BigDecimal low = NumericOrder.lowerEdge(c, true, true);
        BigDecimal high = NumericOrder.upperEdge(c, true, true);
        // Decimals that ARQ compares EQUAL to 0.1f (they round to it) lie within the edges.
        for (String lex : new String[]{"0.1", "0.10000000010", "0.100000000099", "0.1000000015", "0.10000000149"}) {
            NodeValue d = NodeValue.makeNode(dec(lex));
            assertEquals(0, Integer.signum(NodeValue.compare(d, c)), lex + " promotes to 0.1f");
            BigDecimal x = NumericOrder.exact(d);
            assertTrue(x.compareTo(low) >= 0 && x.compareTo(high) <= 0, lex + " within [" + low + ", " + high + "]");
        }
        // An integer constant is widened only where float / double rows exist.
        NodeValue big = NodeValue.makeNode(integer("4611686018427387905"));
        assertEquals(new BigDecimal("4611686018427387905"), NumericOrder.lowerEdge(big, false, false));
        assertTrue(NumericOrder.lowerEdge(big, true, false).compareTo(new BigDecimal("4611686018427387905")) < 0);
    }

    // --- end to end: range pushdown over mixed numeric data matches Jena ---

    private static final String TTL = """
        @prefix ex:  <http://ex.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        ex:a ex:v "0.100000000099"^^xsd:decimal .
        ex:b ex:v "0.10000000010"^^xsd:decimal .
        ex:c ex:v "0.10000000005"^^xsd:float .
        ex:d ex:v "0.1"^^xsd:float .
        ex:e ex:v "0.1"^^xsd:decimal .
        ex:f ex:v "0.1"^^xsd:double .
        ex:g ex:v "0.1000000015"^^xsd:decimal .
        ex:h ex:v "0.7"^^xsd:decimal .
        ex:i ex:v "0.7"^^xsd:float .
        ex:j ex:v 1 .
        ex:k ex:v "1.0"^^xsd:float .
        ex:l ex:v "9007199254740993"^^xsd:integer .
        ex:m ex:v "9007199254740992"^^xsd:double .
        ex:n ex:v "9007199254740992.5"^^xsd:decimal .
        ex:o ex:v "9999999999999999999"^^xsd:integer .
        ex:p ex:v "10000000000000000000"^^xsd:integer .
        ex:q ex:v "-2.5"^^xsd:double .
        ex:r ex:v "NaN"^^xsd:double .
        ex:s ex:v "INF"^^xsd:float .
        ex:t ex:v "-INF"^^xsd:double .
        ex:u ex:v "hello" .
        """;
    private static final String PREFIX = "PREFIX ex: <http://ex.org/> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ";

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;
    static Dataset reference;

    @BeforeAll
    static void build() throws Exception {
        File src = dir.resolve("num.ttl").toFile();
        File h5 = dir.resolve("num.h5").toFile();
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
                out.add(qs.get("s").asNode().getURI());
            }
        }
        Collections.sort(out);
        return out;
    }

    @Test
    void everyStoredTermIsFoundAgain() {
        // A cyclic comparator's first symptom: a term the writer stored that the
        // reader's binary search cannot locate. Look every stored object term
        // up by itself, exactly as the engine resolves a concrete literal. (The
        // store's OWN terms: float/double lexical forms are canonicalized at
        // write time, so the source spelling is not what is stored.)
        int looked = 0;
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create(PREFIX + "SELECT ?s ?o WHERE { ?s ex:v ?o }")).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                QuerySolution qs = rs.next();
                String subject = qs.get("s").asNode().getURI();
                String literal = org.apache.jena.sparql.util.FmtUtils.stringForNode(qs.get("o").asNode());
                String q = PREFIX + "SELECT ?s WHERE { ?s ex:v " + literal + " }";
                assertTrue(rows(ds, q).contains(subject), "stored term not found again: " + q);
                looked++;
            }
        }
        assertEquals(21, looked);
        assertEquals(rows(reference, PREFIX + "SELECT ?s WHERE { ?s ex:v ?o }"), rows(ds, PREFIX + "SELECT ?s WHERE { ?s ex:v ?o }"));
    }

    @Test
    void rangeFiltersOverMixedNumericsMatchJena() {
        String[] constants = {
            "\"0.1\"^^xsd:float", "\"0.1\"^^xsd:decimal", "\"0.1\"^^xsd:double", "\"0.10000000010\"^^xsd:decimal",
            "\"0.10000000005\"^^xsd:float", "\"0.7\"^^xsd:float", "\"0.7\"^^xsd:decimal", "1", "\"1.0\"^^xsd:float",
            "\"9007199254740993\"^^xsd:integer", "\"9007199254740992\"^^xsd:double", "\"9999999999999999999\"^^xsd:integer",
            "\"-2.5\"^^xsd:double", "\"INF\"^^xsd:float", "\"NaN\"^^xsd:double", "0",
        };
        int compared = 0;
        for (String c : constants) {
            for (String op : new String[]{">", ">=", "<", "<="}) {
                for (String pattern : new String[]{"?s ex:v ?o", "?s ?p ?o"}) {
                    for (String filter : new String[]{"?o " + op + " " + c, c + " " + op + " ?o"}) {
                        String q = PREFIX + "SELECT ?s WHERE { " + pattern + " FILTER(" + filter + ") }";
                        assertEquals(rows(reference, q), rows(ds, q), q);
                        compared++;
                    }
                }
            }
        }
        assertEquals(constants.length * 16, compared);
        assertFalse(rows(reference, PREFIX + "SELECT ?s WHERE { ?s ex:v ?o FILTER(?o >= \"0.1\"^^xsd:float) }").isEmpty());
    }
}
