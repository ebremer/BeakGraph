package com.ebremer.beakgraph.core.lib;

import static org.junit.jupiter.api.Assertions.assertEquals;
import com.ebremer.beakgraph.hdf5.Index;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.Quad;
import org.junit.jupiter.api.Test;

/**
 * BG-249: no dictionary sort asks the shared comparator for every pair; a
 * sort runs on {@link TermKey}s. The result must be exactly the order of the
 * shared {@link NodeComparator#INSTANCE} on a literal-heavy population with
 * value-equal but term-distinct literals, temporal and numeric promotions,
 * language tags and duplicates; and the comparator a sorted list is checked
 * with ({@link NodeSorter#adjacentOrder}) must be that comparator.
 */
class NodeSorterTest {

    private static List<Node> population(int n, long seed) {
        Random rnd = new Random(seed);
        List<Node> nodes = new ArrayList<>(n);
        nodes.add(Quad.defaultGraphIRI);
        nodes.add(Quad.defaultGraphNodeGenerated);
        for (int i = 0; i < n; i++) {
            int v = rnd.nextInt(200);
            switch (rnd.nextInt(9)) {
                case 0 -> nodes.add(NodeFactory.createLiteralDT(Integer.toString(v), XSDDatatype.XSDint));
                case 1 -> nodes.add(NodeFactory.createLiteralDT(Integer.toString(v), XSDDatatype.XSDlong));
                case 2 -> nodes.add(NodeFactory.createLiteralDT(v + ".0", XSDDatatype.XSDdecimal));
                case 3 -> nodes.add(NodeFactory.createLiteralDT(v + ".5", XSDDatatype.XSDdouble));
                case 4 -> nodes.add(NodeFactory.createLiteralString("s" + (v % 40)));
                case 5 -> nodes.add(NodeFactory.createLiteralLang("s" + (v % 40), v % 2 == 0 ? "en" : "de"));
                case 6 -> nodes.add(NodeFactory.createLiteralDT(String.format("2020-01-%02dT%02d:00:00Z", 1 + v % 28, v % 24), XSDDatatype.XSDdateTime));
                case 7 -> nodes.add(NodeFactory.createURI("http://ex.org/r" + (v % 60)));
                default -> nodes.add(NodeFactory.createBlankNode("b" + (v % 30)));
            }
        }
        return nodes;
    }

    @Test
    void parallelSortMatchesTheSharedComparator() {
        List<Node> nodes = population(6000, 1);
        List<Node> expected = new ArrayList<>(nodes);
        expected.sort(NodeComparator.INSTANCE);
        assertEquals(expected, NodeSorter.parallelSort(nodes), "List overload");
        Node[] array = nodes.toArray(Node[]::new);
        NodeSorter.parallelSort(array);
        assertEquals(expected, Arrays.asList(array), "in-place array overload");
        HashSet<Node> set = new HashSet<>(nodes);
        List<Node> expectedSet = new ArrayList<>(set);
        expectedSet.sort(NodeComparator.INSTANCE);
        assertEquals(expectedSet, NodeSorter.parallelSort(set), "Set overload");
    }

    @Test
    void adjacentOrderIsTheComparatorAlongAListAndOffIt() {
        List<Node> sorted = population(4000, 4);
        sorted.sort(NodeComparator.INSTANCE);
        NodeComparator walk = NodeSorter.adjacentOrder();
        for (int i = 1; i < sorted.size(); i++) {
            assertEquals(Integer.signum(NodeComparator.INSTANCE.compare(sorted.get(i - 1), sorted.get(i))),
                    Integer.signum(walk.compare(sorted.get(i - 1), sorted.get(i))), "neighbours at " + i);
        }
        // Asked in any order it is still the comparator: what it remembers of a term is found by the term itself.
        Random rnd = new Random(5);
        for (int k = 0; k < 50_000; k++) {
            Node a = sorted.get(rnd.nextInt(sorted.size()));
            Node b = sorted.get(rnd.nextInt(sorted.size()));
            assertEquals(Integer.signum(NodeComparator.INSTANCE.compare(a, b)), Integer.signum(walk.compare(a, b)),
                    "order of (" + a + ", " + b + ")");
        }
        // Each walk has a comparator of its own.
        assertEquals(false, NodeSorter.adjacentOrder() == NodeSorter.adjacentOrder());
    }

    @Test
    void quadOrdersAgreeOnTheAdjacentComparator() {
        List<Node> nodes = population(400, 2);
        Random rnd = new Random(3);
        List<Quad> quads = new ArrayList<>();
        for (int i = 0; i < 3000; i++) {
            quads.add(new Quad(NodeFactory.createURI("http://g/" + rnd.nextInt(3)),
                    NodeFactory.createURI("http://s/" + rnd.nextInt(50)),
                    NodeFactory.createURI("http://p/" + rnd.nextInt(5)),
                    nodes.get(rnd.nextInt(nodes.size()))));
        }
        for (Index idx : Index.values()) {
            List<Quad> expected = new ArrayList<>(quads);
            expected.sort(idx.getComparator());
            List<Quad> actual = new ArrayList<>(quads);
            // It remembers two terms: a sort asks it of far more, in no order.
            actual.sort(idx.getComparator(NodeSorter.adjacentOrder()));
            assertEquals(expected, actual, idx.name());
        }
    }
}
