package com.ebremer.beakgraph.core.lib;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Pins the literal order SPECIFICATIONS.md §6.2 declares normative for format
 * v5: the fixed cross-space rank (string &lt; language-tagged &lt; numeric
 * &lt; boolean &lt; instant &lt; date &lt; time &lt; duration &lt; cdt:List
 * &lt; cdt:Map &lt; unknown / ill-formed), the instant-kind rank and the
 * within-space value orders. Dictionary ids ARE comparator ranks and readers
 * re-derive them by binary search with the same comparator, so this order is
 * on-disk format. Most of it is delegated to Jena's {@code
 * NodeValue.compareAlways}, which every in-tree test exercises with one JVM
 * on both the write and the read side - drift there would pass CI unnoticed
 * (BG-267). A failure here means Jena (or a branch of NodeComparator) changed
 * the order: bump {@code Params.FORMAT_VERSION} or pin the old order natively;
 * do NOT edit the expectation.
 */
class NodeComparatorRankTableTest {

    private static final String LIST = "http://w3id.org/awslabs/neptune/SPARQL-CDTs/List";
    private static final String MAP = "http://w3id.org/awslabs/neptune/SPARQL-CDTs/Map";

    @BeforeAll
    static void initJena() {
        org.apache.jena.sys.JenaSystem.init();
    }

    private static Node dt(String lex, XSDDatatype type) {
        return NodeFactory.createLiteralDT(lex, type);
    }

    private static Node dt(String lex, String datatypeUri) {
        return NodeFactory.createLiteralDT(lex, TypeMapper.getInstance().getSafeTypeByName(datatypeUri));
    }

    /** SPECIFICATIONS.md §6.2 order, one or two representatives per value space. */
    static List<Node> expectedOrder() {
        List<Node> order = new ArrayList<>();
        // xsd:string / simple literals
        order.add(NodeFactory.createLiteralString("a"));
        order.add(dt("b", XSDDatatype.XSDstring));
        // language-tagged: (tag, lexical form, direction) with absent < ltr
        order.add(NodeFactory.createLiteralLang("a", "en"));
        order.add(NodeFactory.createLiteralDirLang("a", "en", "ltr"));
        order.add(NodeFactory.createLiteralLang("b", "en"));
        // numeric, any XSD numeric datatype, by exact value (2 < 10)
        order.add(dt("1", XSDDatatype.XSDint));
        order.add(dt("1.5", XSDDatatype.XSDdouble));
        order.add(dt("2", XSDDatatype.XSDint));
        order.add(dt("10", XSDDatatype.XSDint));
        // boolean: false < true
        order.add(dt("false", XSDDatatype.XSDboolean));
        order.add(dt("true", XSDDatatype.XSDboolean));
        // the instant space, by kind rank: gDay < gMonth < gMonthDay < gYear < gYearMonth < dateTime
        order.add(dt("---01", XSDDatatype.XSDgDay));
        order.add(dt("--01", XSDDatatype.XSDgMonth));
        order.add(dt("--01-01", XSDDatatype.XSDgMonthDay));
        order.add(dt("2020", XSDDatatype.XSDgYear));
        order.add(dt("2020-01", XSDDatatype.XSDgYearMonth));
        order.add(dt("2020-01-01T00:00:00Z", XSDDatatype.XSDdateTime));
        // date, time
        order.add(dt("2020-01-01", XSDDatatype.XSDdate));
        order.add(dt("00:00:00", XSDDatatype.XSDtime));
        // duration: months first, then seconds
        order.add(dt("P1D", XSDDatatype.XSDduration));
        order.add(dt("P1M", XSDDatatype.XSDduration));
        // composites: List < Map, lexical inside
        order.add(dt("[1]", LIST));
        order.add(dt("{}", MAP));
        // unknown datatypes and ill-formed literals: lexical form, then datatype
        order.add(dt("x", "urn:unknown"));
        order.add(dt("zz", XSDDatatype.XSDint));
        return order;
    }

    @Test
    void everyPairFollowsTheDocumentedRank() {
        List<Node> order = expectedOrder();
        for (int i = 0; i < order.size(); i++) {
            assertEquals(0, NodeComparator.INSTANCE.compare(order.get(i), order.get(i)), order.get(i) + " vs itself");
            for (int j = i + 1; j < order.size(); j++) {
                Node a = order.get(i);
                Node b = order.get(j);
                assertTrue(NodeComparator.INSTANCE.compare(a, b) < 0,
                        "SPECIFICATIONS.md §6.2 puts " + a + " before " + b);
                assertTrue(NodeComparator.INSTANCE.compare(b, a) > 0,
                        "SPECIFICATIONS.md §6.2 puts " + b + " after " + a);
            }
        }
    }

    @Test
    void sortingReproducesTheTableFromAnyInputOrder() {
        List<Node> expected = expectedOrder();
        for (int seed = 0; seed < 8; seed++) {
            List<Node> shuffled = new ArrayList<>(expected);
            Collections.shuffle(shuffled, new Random(seed));
            shuffled.sort(NodeComparator.INSTANCE);
            assertEquals(expected, shuffled, "seed " + seed);
        }
    }
}
