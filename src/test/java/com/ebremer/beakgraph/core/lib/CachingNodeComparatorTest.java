package com.ebremer.beakgraph.core.lib;

import java.util.ArrayList;
import java.util.List;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.Quad;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

/**
 * The caching comparator must be ORDER-IDENTICAL to the shared
 * NodeComparator on every node kind - it only relocates where the
 * Node-to-NodeValue conversion result is stored. Includes the value spaces
 * with special-cased ordering (temporal, duration) and value-equal but
 * term-distinct literals, plus the memo's cap-reset path.
 */
class CachingNodeComparatorTest {

    private static List<Node> zoo() {
        List<Node> nodes = new ArrayList<>();
        nodes.add(Quad.defaultGraphIRI);
        nodes.add(Quad.defaultGraphNodeGenerated);
        nodes.add(NodeFactory.createBlankNode("b0"));
        nodes.add(NodeFactory.createBlankNode("b1"));
        nodes.add(NodeFactory.createURI("http://ex.org/a"));
        nodes.add(NodeFactory.createURI("http://ex.org/b"));
        nodes.add(NodeFactory.createURI("relative.png"));
        nodes.add(NodeFactory.createLiteralString("alpha"));
        nodes.add(NodeFactory.createLiteralString("beta"));
        nodes.add(NodeFactory.createLiteralLang("alpha", "en"));
        nodes.add(NodeFactory.createLiteralDT("1", XSDDatatype.XSDint));
        nodes.add(NodeFactory.createLiteralDT("1", XSDDatatype.XSDlong));
        nodes.add(NodeFactory.createLiteralDT("1.0", XSDDatatype.XSDdouble));
        nodes.add(NodeFactory.createLiteralDT("2", XSDDatatype.XSDint));
        nodes.add(NodeFactory.createLiteralDT("10", XSDDatatype.XSDint));
        nodes.add(NodeFactory.createLiteralDT("123456789012345678901234567890", XSDDatatype.XSDinteger));
        nodes.add(NodeFactory.createLiteralDT("abc", XSDDatatype.XSDint)); // ill-typed
        nodes.add(NodeFactory.createLiteralDT("2020-01-02T00:00:00", XSDDatatype.XSDdateTime));
        nodes.add(NodeFactory.createLiteralDT("2020-01-02T08:00:00+14:00", XSDDatatype.XSDdateTime));
        nodes.add(NodeFactory.createLiteralDT("2020-01-01T20:00:00Z", XSDDatatype.XSDdateTime));
        nodes.add(NodeFactory.createLiteralDT("2020-01-02", XSDDatatype.XSDdate));
        nodes.add(NodeFactory.createLiteralDT("P1D", XSDDatatype.XSDduration));
        nodes.add(NodeFactory.createLiteralDT("PT24H", XSDDatatype.XSDduration));
        nodes.add(NodeFactory.createLiteralDT("P1M", XSDDatatype.XSDduration));
        nodes.add(NodeFactory.createLiteralDT("true", XSDDatatype.XSDboolean));
        return nodes;
    }

    @Test
    void orderIsIdenticalToTheSharedComparator() {
        CachingNodeComparator cached = new CachingNodeComparator(1 << 16);
        List<Node> nodes = zoo();
        for (Node a : nodes) {
            for (Node b : nodes) {
                int expected = Integer.signum(NodeComparator.INSTANCE.compare(a, b));
                assertEquals(expected, Integer.signum(cached.compare(a, b)),
                        "order of (" + a + ", " + b + ")");
                // Memo hits must not change the answer either.
                assertEquals(expected, Integer.signum(cached.compare(a, b)),
                        "cached order of (" + a + ", " + b + ")");
            }
        }
    }

    @Test
    void capResetKeepsAnswersCorrect() {
        // A cap of 2 forces constant clear/reconvert cycles.
        CachingNodeComparator cached = new CachingNodeComparator(2);
        List<Node> nodes = zoo();
        for (int round = 0; round < 3; round++) {
            for (int i = 0; i < nodes.size(); i++) {
                for (int j = 0; j < nodes.size(); j++) {
                    assertEquals(
                            Integer.signum(NodeComparator.INSTANCE.compare(nodes.get(i), nodes.get(j))),
                            Integer.signum(cached.compare(nodes.get(i), nodes.get(j))));
                }
            }
        }
    }
}
