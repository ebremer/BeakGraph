package com.ebremer.beakgraph.core.lib;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.expr.NodeValue;

/**
 * The NodeComparator sorts behind dictionary construction. A sort runs on
 * {@link TermKey}s: each term's place in the order is worked out once, by the
 * thread that first compares it, and a comparison reads two of those. The
 * comparator itself, asked for every pair, converted each literal to a value
 * on every comparison - through Jena's one global cache and its eviction lock
 * at first (BG-249), then through a memo of the sort's own, whose lookup by
 * the literal's text was in turn a fifth of a build (PERFORMANCE.md). The
 * order is exactly NodeComparator's.
 */
public class NodeSorter {

    public static ArrayList<Node> parallelSort(Set<Node> nodeSet) {
        Node[] array = nodeSet.toArray(new Node[0]);
        parallelSort(array);
        return new ArrayList<>(Arrays.asList(array));
    }

    public static ArrayList<Node> parallelSort(List<Node> nodeSet) {
        Node[] array = nodeSet.toArray(new Node[0]);
        parallelSort(array);
        return new ArrayList<>(Arrays.asList(array));
    }

    /** Sorts {@code array} in place, in NodeComparator order. */
    public static void parallelSort(Node[] array) {
        TermKey[] keys = new TermKey[array.length];
        Arrays.parallelSetAll(keys, i -> new TermKey(array[i]));
        Arrays.parallelSort(keys, TermKey.ORDER);
        Arrays.parallelSetAll(array, i -> keys[i].node());
    }

    /**
     * The comparator itself - not the keys - for a walk along a sorted list
     * that compares each term with the next: (a, b), (b, c), ... It is how a
     * list sorted on keys is held to NodeComparator before it becomes a
     * dictionary, so it must not be the keys' code. A literal's value is made
     * once, for the two comparisons it takes part in, and without Jena's
     * cache. For one thread; a comparator serves one walk.
     */
    public static NodeComparator adjacentOrder() {
        return new NodeComparator() {
            private Node last;
            private Node before;
            private NodeValue lastValue;
            private NodeValue beforeValue;

            @Override
            protected NodeValue nodeValue(Node n) {
                if (n == last) {
                    return lastValue;
                }
                if (n == before) {
                    return beforeValue;
                }
                NodeValue value = NodeValues.of(n);
                before = last;
                beforeValue = lastValue;
                last = n;
                lastValue = value;
                return value;
            }
        };
    }
}
