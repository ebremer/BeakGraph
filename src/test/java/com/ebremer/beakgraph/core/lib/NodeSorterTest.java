package com.ebremer.beakgraph.core.lib;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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
 * BG-249: every dictionary and quad sort now runs on a per-sort
 * {@link CachingNodeComparator}; the result must be exactly the order of the
 * shared {@link NodeComparator#INSTANCE} on a literal-heavy population with
 * value-equal but term-distinct literals, temporal and numeric promotions,
 * language tags and duplicates.
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
    void sortComparatorIsAFreshMemoEachTime() {
        NodeComparator a = NodeSorter.sortComparator(10);
        NodeComparator b = NodeSorter.sortComparator(10);
        assertInstanceOf(CachingNodeComparator.class, a);
        assertInstanceOf(CachingNodeComparator.class, b);
        assertEquals(false, a == b);
        // Any size, including absurd ones, yields a usable comparator.
        NodeSorter.sortComparator(0).compare(Quad.defaultGraphIRI, NodeFactory.createURI("http://x"));
        NodeSorter.sortComparator(Long.MAX_VALUE).compare(Quad.defaultGraphIRI, NodeFactory.createURI("http://x"));
    }

    @Test
    void quadOrdersAgreeOnAMemoizingComparator() {
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
            // A tiny cap forces constant memo resets mid-sort.
            actual.sort(idx.getComparator(new CachingNodeComparator(3)));
            assertEquals(expected, actual, idx.name());
            List<Quad> viaSorter = new ArrayList<>(quads);
            viaSorter.sort(idx.getComparator(NodeSorter.sortComparator(quads.size())));
            assertEquals(expected, viaSorter, idx.name() + " via NodeSorter.sortComparator");
        }
    }
}
