package com.ebremer.beakgraph.core.lib;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;
import org.junit.jupiter.api.Test;

/**
 * Pins SPECIFICATIONS.md §6.1 for RDF-array sequences (format v7): macro rank
 * 5, after every triple term; element by element through the whole
 * comparator; a slot below every term, the default graph's names included;
 * a proper prefix first. On-disk format, as NodeComparatorRankTableTest is:
 * do not edit the expectation, bump the format.
 */
class NodeComparatorSequenceOrderTest {

    private static final Node A = NodeFactory.createURI("http://ex.org/a");
    private static final Node B = NodeFactory.createURI("http://ex.org/b");
    private static final Node BN = NodeFactory.createBlankNode("b0");
    private static final Node ONE = NodeFactory.createLiteralDT("1", XSDDatatype.XSDinteger);
    private static final Node TWO = NodeFactory.createLiteralDT("2", XSDDatatype.XSDint);
    private static final Node TEN = NodeFactory.createLiteralDT("10", XSDDatatype.XSDint);
    private static final Node U = SeqTerms.UNDEF;

    private static Node seq(Node... elements) {
        return SeqTerms.create(List.of(elements));
    }

    static List<Node> expectedOrder() {
        List<Node> order = new ArrayList<>();
        order.add(BN);
        order.add(A);
        order.add(TEN);
        order.add(NodeFactory.createTripleTerm(Triple.create(A, B, A)));
        order.add(seq());                                  // the empty sequence: a prefix of every other
        order.add(seq(U));                                 // a slot below every term ...
        order.add(seq(U, U));
        order.add(seq(U, ONE));
        order.add(seq(Quad.defaultGraphIRI));              // ... the default graph's name included
        order.add(seq(BN));
        order.add(seq(A));
        order.add(seq(A, U));
        order.add(seq(A, A));
        order.add(seq(ONE));
        order.add(seq(ONE, U));
        order.add(seq(ONE, TWO));
        order.add(seq(TWO));                               // elements by value: 2 < 10
        order.add(seq(TEN));
        order.add(seq(NodeFactory.createTripleTerm(Triple.create(A, B, A))));
        order.add(seq(seq()));                             // a nested sequence ranks as a sequence: after every triple term
        order.add(seq(seq(), U));
        order.add(seq(seq(U)));
        order.add(seq(seq(ONE)));
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
                assertTrue(NodeComparator.INSTANCE.compare(a, b) < 0, "§6.1 puts " + a + " before " + b);
                assertTrue(NodeComparator.INSTANCE.compare(b, a) > 0, "§6.1 puts " + b + " after " + a);
                assertTrue(TermKey.compare(new TermKey(a), new TermKey(b)) < 0, "TermKey: " + a + " before " + b);
            }
        }
    }

    @Test
    void sortingReproducesTheTableFromAnyInputOrder() {
        List<Node> expected = expectedOrder();
        for (int seed = 0; seed < 8; seed++) {
            List<Node> shuffled = new ArrayList<>(expected);
            Collections.shuffle(shuffled, new Random(seed));
            assertEquals(expected, NodeSorter.parallelSort(shuffled), "seed " + seed);
        }
    }

    @Test
    void equalSequencesCompareEqualAndASlotOutsideOneIsNoTerm() {
        assertEquals(0, NodeComparator.INSTANCE.compare(seq(ONE, U, seq(A)), seq(ONE, U, seq(A))));
        assertThrows(IllegalArgumentException.class, () -> NodeComparator.INSTANCE.compare(U, A));
    }
}
