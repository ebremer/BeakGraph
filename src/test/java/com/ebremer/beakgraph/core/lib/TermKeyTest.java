package com.ebremer.beakgraph.core.lib;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.halcyon.hilbert.WKTDatatype;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.vocabulary.RDF;
import org.junit.jupiter.api.Test;

/**
 * Sorts run on {@link TermKey}s, and ids are ranks in the order they give:
 * that order must be {@link NodeComparator}'s for EVERY pair of terms, or a
 * store's dictionary is not the one its readers search. Held here pair by
 * pair on terms of every kind the comparator has a branch for, and hardest
 * where the keys decide by themselves: numbers of every datatype whose
 * nearest doubles are equal, next to each other, or out of a double's range.
 */
class TermKeyTest {

    private static Node typed(String lexical, RDFDatatype datatype) {
        return NodeFactory.createLiteralDT(lexical, datatype);
    }

    private static final RDFDatatype[] NUMERIC = {
        XSDDatatype.XSDinteger, XSDDatatype.XSDint, XSDDatatype.XSDlong, XSDDatatype.XSDshort, XSDDatatype.XSDbyte,
        XSDDatatype.XSDnonNegativeInteger, XSDDatatype.XSDunsignedLong, XSDDatatype.XSDdecimal, XSDDatatype.XSDfloat,
        XSDDatatype.XSDdouble};

    /** Terms of every kind; the comparator's own tables and the zoo the memo comparator was held to before the keys. */
    private static List<Node> kinds() {
        List<Node> nodes = new ArrayList<>(NodeComparatorRankTableTest.expectedOrder());
        nodes.add(null);
        nodes.add(Quad.defaultGraphIRI);
        nodes.add(Quad.defaultGraphNodeGenerated);
        String[] specials = {"\u0000", "\u007F", "þ", "ÿ", "Ā", "名", "🐦", "a", "z", " "};
        for (int pos = 0; pos <= 8; pos += 2) {
            for (String s : specials) {
                String label = "x".repeat(pos) + s;
                nodes.add(NodeFactory.createURI("http://ex.org/" + label));
                nodes.add(NodeFactory.createURI(label));
                nodes.add(NodeFactory.createBlankNode("b" + label));
                nodes.add(NodeFactory.createBlankNode(label));
            }
        }
        for (String s : new String[] {"ab", "abcdefg", "abcdefgh", "abcdefgH", "urn:x-arq:DefaultGraph", "urn:x-arq:DefaultGraphNode", "relative.png"}) {
            nodes.add(NodeFactory.createURI(s));
            nodes.add(NodeFactory.createBlankNode(s));
        }
        for (String s : new String[] {"", "alpha", "beta", "ÿĀ", "10", "9", "POLYGON ((0 0, 1 0, 1 1, 0 0))"}) {
            nodes.add(NodeFactory.createLiteralString(s));
            nodes.add(NodeFactory.createLiteralLang(s, "en"));
            nodes.add(NodeFactory.createLiteralLang(s, "fr"));
            nodes.add(NodeFactory.createLiteralDirLang(s, "ar", "rtl"));
            nodes.add(NodeFactory.createLiteralDirLang(s, "ar", "ltr"));
            nodes.add(typed(s, XSDDatatype.XSDnormalizedString));
            nodes.add(typed(s, WKTDatatype.INSTANCE));
            nodes.add(typed(s, TypeMapper.getInstance().getSafeTypeByName("http://ex.org/datatype")));
            nodes.add(typed(s, TypeMapper.getInstance().getSafeTypeByName("http://ex.org/other")));
            nodes.add(typed(s, RDF.dtXMLLiteral));
        }
        for (String s : new String[] {"POLYGON ((112821 19290, 112822 19292, 112821 19290))", "POLYGON ((112821 19290, 112822 19293, 112821 19290))",
                "POINT (1 2)", "<http://www.opengis.net/def/crs/EPSG/0/4326> POINT (1 2)", "not wkt"}) {
            nodes.add(typed(s, WKTDatatype.INSTANCE));
        }
        for (String s : new String[] {"true", "false", "1", "0", "maybe"}) {
            nodes.add(typed(s, XSDDatatype.XSDboolean));
        }
        for (String s : new String[] {"2020-01-02T00:00:00", "2020-01-02T08:00:00+14:00", "2020-01-01T20:00:00Z", "2020-01-01T20:00:00+00:00",
                "2020-01-02T00:00:00.5Z", "not a date"}) {
            nodes.add(typed(s, XSDDatatype.XSDdateTime));
            nodes.add(typed(s, XSDDatatype.XSDdateTimeStamp));
        }
        nodes.add(typed("2020-01-02", XSDDatatype.XSDdate));
        nodes.add(typed("2020-01-02Z", XSDDatatype.XSDdate));
        nodes.add(typed("2020-01-03+14:00", XSDDatatype.XSDdate));
        nodes.add(typed("10:00:00", XSDDatatype.XSDtime));
        nodes.add(typed("10:00:00Z", XSDDatatype.XSDtime));
        nodes.add(typed("2020", XSDDatatype.XSDgYear));
        nodes.add(typed("2020-01", XSDDatatype.XSDgYearMonth));
        nodes.add(typed("--01", XSDDatatype.XSDgMonth));
        nodes.add(typed("--01-02", XSDDatatype.XSDgMonthDay));
        nodes.add(typed("---02", XSDDatatype.XSDgDay));
        for (String s : new String[] {"P1D", "PT24H", "P1M", "P1M35D", "P2M1D", "P1Y", "P400D", "-P1D", "PT0S", "P1Y2M3DT4H5M6.7S"}) {
            nodes.add(typed(s, XSDDatatype.XSDduration));
        }
        nodes.add(typed("P1D", XSDDatatype.XSDdayTimeDuration));
        nodes.add(typed("P1Y", XSDDatatype.XSDyearMonthDuration));
        RDFDatatype list = NodeFactory.getType("http://w3id.org/awslabs/neptune/SPARQL-CDTs/List");
        RDFDatatype map = NodeFactory.getType("http://w3id.org/awslabs/neptune/SPARQL-CDTs/Map");
        for (String s : new String[] {"[1, 2]", "[9]", "[10]", "[5", "[]"}) {
            nodes.add(typed(s, list));
        }
        for (String s : new String[] {"{\"a\": 1}", "{\"b\": 2}", "{"}) {
            nodes.add(typed(s, map));
        }
        Node a = NodeFactory.createURI("http://ex.org/a");
        Node b = NodeFactory.createURI("http://ex.org/b");
        nodes.add(NodeFactory.createTripleTerm(Triple.create(a, b, a)));
        nodes.add(NodeFactory.createTripleTerm(Triple.create(a, b, b)));
        nodes.add(NodeFactory.createTripleTerm(Triple.create(a, b, typed("1", XSDDatatype.XSDint))));
        nodes.add(NodeFactory.createTripleTerm(Triple.create(a, b, typed("1.0", XSDDatatype.XSDdouble))));
        nodes.add(NodeFactory.createTripleTerm(Triple.create(a, b,
                NodeFactory.createTripleTerm(Triple.create(b, a, NodeFactory.createLiteralDirLang("n", "ar", "rtl"))))));
        return nodes;
    }

    /** Numbers where the nearest double decides, where it cannot, and where there is none. */
    private static List<Node> numbers() {
        List<Node> nodes = new ArrayList<>();
        String[] lexicals = {"0", "-0", "+0", "1", "-1", "01", "2", "10", "127", "128", "255", "256", "32767", "65535", "2147483647", "2147483648",
            "9007199254740991", "9007199254740992", "9007199254740993", "9007199254740994", "-9007199254740993",
            "9223372036854775807", "9223372036854775808", "18446744073709551615", "123456789012345678901234567890",
            "0.0", "-0.0", "1.0", "1.00", "1.5", "0.1", "0.10", "0.1000000000000000055511151231257827021181583404541015625",
            "0.1000000000000000055511151231257827021181583404541015624", "0.1000000000000000055511151231257827021181583404541015626",
            "0.100000001490116119384765625", "0.3", "0.30000000000000004", "0.299999999999999988897769753748434595763683319091796875",
            "1e0", "1E0", "1e3", "1.0e3", "1000", "1e-320", "4.9E-324", "2.4E-324", "2.5E-324", "1.7976931348623157E308", "1.7976931348623158E308",
            "1.797693134862315807E308", "1.8E308", "1e400", "-1e400", "3.4028235E38", "3.4028236E38", "1e39", "1.4E-45", "16777216", "16777217",
            "INF", "-INF", "+INF", "NaN", "abc", "", " 12 ", "1.", ".5", "5e", "0x10"};
        // 10^400 and its neighbours, written out: past every double, and apart only as exact values.
        String big = "1" + "0".repeat(400);
        for (RDFDatatype datatype : NUMERIC) {
            for (String lexical : lexicals) {
                nodes.add(typed(lexical, datatype));
            }
            nodes.add(typed(big, datatype));
            nodes.add(typed(big.substring(0, 400) + "1", datatype));
            nodes.add(typed("-" + big, datatype));
        }
        return nodes;
    }

    /** Random numbers, each also as the terms whose values lie beside it and on it in the other datatypes. */
    private static List<Node> randomNumbers(long seed, int n) {
        Random rnd = new Random(seed);
        List<Node> nodes = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            double d = switch (rnd.nextInt(5)) {
                case 0 -> rnd.nextInt(2000) - 1000;
                case 1 -> rnd.nextDouble();
                case 2 -> Double.longBitsToDouble(rnd.nextLong());
                case 3 -> (rnd.nextDouble() - 0.5) * 1e18;
                default -> Math.scalb(rnd.nextDouble(), rnd.nextInt(120) - 60);
            };
            if (Double.isNaN(d) || Double.isInfinite(d)) {
                continue;
            }
            nodes.add(typed(Double.toString(d), XSDDatatype.XSDdouble));
            nodes.add(typed(Float.toString((float) d), XSDDatatype.XSDfloat));
            BigDecimal exact = new BigDecimal(d);
            if (exact.scale() < 400 && exact.precision() < 400) {
                nodes.add(typed(exact.toPlainString(), XSDDatatype.XSDdecimal));
                // The numbers half a step and one step of the last digit to either side: same nearest double, another value.
                BigDecimal step = BigDecimal.ONE.movePointLeft(exact.scale() + 1);
                nodes.add(typed(exact.add(step).toPlainString(), XSDDatatype.XSDdecimal));
                nodes.add(typed(exact.subtract(step).toPlainString(), XSDDatatype.XSDdecimal));
                // Halfway to the next double up: the point where the nearest double changes.
                double up = Math.nextUp(d);
                if (!Double.isInfinite(up)) {
                    BigDecimal half = exact.add(new BigDecimal(up)).divide(BigDecimal.valueOf(2));
                    if (half.scale() < 500) {
                        nodes.add(typed(half.toPlainString(), XSDDatatype.XSDdecimal));
                    }
                }
            }
            BigInteger whole = exact.toBigInteger();
            if (whole.bitLength() < 200) {
                nodes.add(typed(whole.toString(), XSDDatatype.XSDinteger));
                nodes.add(typed(whole.add(BigInteger.ONE).toString(), XSDDatatype.XSDinteger));
                if (whole.bitLength() < 63) {
                    nodes.add(typed(whole.toString(), XSDDatatype.XSDlong));
                }
            }
        }
        return nodes;
    }

    private static void assertSameOrder(List<Node> nodes) {
        TermKey[] keys = new TermKey[nodes.size()];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = new TermKey(nodes.get(i));
        }
        for (int i = 0; i < keys.length; i++) {
            for (int j = 0; j < keys.length; j++) {
                int expected = Integer.signum(NodeComparator.INSTANCE.compare(nodes.get(i), nodes.get(j)));
                if (expected != Integer.signum(TermKey.compare(keys[i], keys[j]))) {
                    assertEquals(expected, Integer.signum(TermKey.compare(keys[i], keys[j])),
                            "order of (" + nodes.get(i) + ", " + nodes.get(j) + ")");
                }
            }
        }
    }

    @Test
    void everyPairOfKindsIsInTheComparatorsOrder() {
        assertSameOrder(kinds());
    }

    @Test
    void everyPairOfNumbersIsInTheComparatorsOrder() {
        assertSameOrder(numbers());
    }

    @Test
    void numbersBesideEachOtherAreInTheComparatorsOrder() {
        // Sorted by the comparator, a number's neighbours are the ones nearest to it: the pairs the nearest double decides
        // least. Each is asked of the keys with the 40 terms after it; all pairs of 1,200 more are asked outright.
        List<Node> nodes = randomNumbers(11, 4000);
        nodes.addAll(numbers());
        List<Node> sorted = new ArrayList<>(nodes);
        sorted.sort(NodeComparator.INSTANCE);
        TermKey[] keys = new TermKey[sorted.size()];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = new TermKey(sorted.get(i));
        }
        int strict = 0;
        for (int i = 0; i < keys.length; i++) {
            for (int j = i; j < Math.min(keys.length, i + 40); j++) {
                int expected = Integer.signum(NodeComparator.INSTANCE.compare(sorted.get(i), sorted.get(j)));
                assertEquals(expected, Integer.signum(TermKey.compare(keys[i], keys[j])), "order of (" + sorted.get(i) + ", " + sorted.get(j) + ")");
                assertEquals(-expected, Integer.signum(TermKey.compare(keys[j], keys[i])), "order of (" + sorted.get(j) + ", " + sorted.get(i) + ")");
                if (expected < 0) {
                    strict++;
                }
            }
        }
        assertTrue(strict > 100_000, "pairs of different numbers asked: " + strict);
        assertSameOrder(randomNumbers(12, 250));
    }

    @Test
    void aSortOnKeysIsASortOnTheComparator() {
        List<Node> nodes = kinds();
        nodes.remove(null);                      // a list of terms; the default graph is in it by its names
        nodes.addAll(numbers());
        nodes.addAll(randomNumbers(13, 500));
        nodes.addAll(nodes.subList(0, 200));     // the same terms again
        Collections.shuffle(nodes, new Random(5));
        List<Node> expected = new ArrayList<>(nodes);
        expected.sort(NodeComparator.INSTANCE);
        Node[] array = nodes.toArray(Node[]::new);
        NodeSorter.parallelSort(array);
        assertEquals(expected, Arrays.asList(array));
        List<TermKey> keys = new ArrayList<>();
        for (Node n : nodes) {
            keys.add(new TermKey(n));
        }
        keys.sort(TermKey.ORDER);
        List<Node> byKeys = new ArrayList<>();
        for (TermKey k : keys) {
            byKeys.add(k.node());
        }
        assertEquals(expected, byKeys);
    }

    @Test
    void oneKeySharedByManyThreadsAnswersAsOne() {
        // Rows of a term share its key, and a parallel sort asks it from several threads before it has worked anything out.
        List<Node> terms = randomNumbers(17, 300);
        terms.addAll(kinds());
        terms.remove(null);
        for (int round = 0; round < 20; round++) {
            List<TermKey> distinct = new ArrayList<>();
            for (Node n : terms) {
                distinct.add(new TermKey(n));
            }
            TermKey[] rows = new TermKey[distinct.size() * 64];
            Random rnd = new Random(round);
            for (int i = 0; i < rows.length; i++) {
                rows[i] = distinct.get(rnd.nextInt(distinct.size()));
            }
            Arrays.parallelSort(rows, TermKey.ORDER);
            for (int i = 1; i < rows.length; i++) {
                assertTrue(NodeComparator.INSTANCE.compare(rows[i - 1].node(), rows[i].node()) <= 0,
                        "round " + round + ": " + rows[i - 1] + " before " + rows[i]);
            }
        }
    }

    @Test
    void keysOfOneTermAreEqual() {
        Node n = typed("1.5", XSDDatatype.XSDdouble);
        TermKey a = new TermKey(n);
        TermKey b = new TermKey(typed("1.5", XSDDatatype.XSDdouble));
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertEquals(0, TermKey.compare(a, b));
        assertNotEquals(a, new TermKey(typed("1.50", XSDDatatype.XSDdouble)));
        assertEquals(new TermKey(null), new TermKey(null));
        assertEquals(0, TermKey.compare(new TermKey(null), new TermKey(Quad.defaultGraphIRI)), "null is the default graph");
        assertEquals(n, a.node());
    }

    @Test
    void whatIsNotATermIsRefusedAsTheComparatorRefusesIt() {
        Node variable = Var.alloc("x");
        Node iri = NodeFactory.createURI("http://ex.org/a");
        assertThrows(IllegalArgumentException.class, () -> NodeComparator.INSTANCE.compare(variable, iri));
        assertThrows(IllegalArgumentException.class, () -> TermKey.compare(new TermKey(variable), new TermKey(iri)));
        assertThrows(IllegalArgumentException.class, () -> TermKey.compare(new TermKey(iri), new TermKey(variable)));
        TermKey shared = new TermKey(variable);
        assertThrows(IllegalArgumentException.class, () -> TermKey.compare(shared, shared));
        // The default graph is decided before the other side is looked at, there as here.
        assertEquals(Integer.signum(NodeComparator.INSTANCE.compare(Quad.defaultGraphIRI, variable)),
                Integer.signum(TermKey.compare(new TermKey(Quad.defaultGraphIRI), new TermKey(variable))));
    }

    @Test
    void theNearestDoubleNeverReversesTwoNumbers() {
        List<Node> nodes = randomNumbers(19, 1500);
        nodes.addAll(numbers());
        List<NodeValue> values = new ArrayList<>();
        for (Node n : nodes) {
            NodeValue v = NodeValue.makeNode(n);
            if (v.isNumber() && NumericOrder.isFinite(v)) {
                values.add(v);
            }
        }
        assertTrue(values.size() > 5000, "finite numbers: " + values.size());
        Random rnd = new Random(23);
        for (int k = 0; k < 400_000; k++) {
            NodeValue a = values.get(rnd.nextInt(values.size()));
            NodeValue b = values.get(rnd.nextInt(values.size()));
            int exact = Integer.signum(NumericOrder.exact(a).compareTo(NumericOrder.exact(b)));
            double x = NumericOrder.nearest(a);
            double y = NumericOrder.nearest(b);
            if (x < y) {
                assertEquals(-1, exact, a + " < " + b);
            } else if (x > y) {
                assertEquals(1, exact, a + " > " + b);
            } else if (NumericOrder.isBinary(a) && NumericOrder.isBinary(b)) {
                assertEquals(0, exact, a + " = " + b);
            }
            assertEquals(exact, Integer.signum(NumericOrder.compare(a, b)), "NumericOrder.compare of " + a + " and " + b);
        }
    }
}
