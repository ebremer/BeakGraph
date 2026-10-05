package com.ebremer.beakgraph.core.lib;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.jena.graph.Node;

/**
 * The NodeComparator sorts behind dictionary construction. Every sort runs on
 * a fresh, PER-SORT {@link CachingNodeComparator}: the shared
 * {@link NodeComparator#INSTANCE} converts each literal to a NodeValue through
 * Jena's one global bounded cache on every comparison, so a parallel sort of
 * a literal-heavy population did O(n log n) conversions with every core
 * contending on that cache's eviction lock - the memo used to be wired into
 * the -method 4/5 term sorters only (BG-249). The order is exactly
 * NodeComparator's.
 */
public class NodeSorter {

    /** Largest memo one sort keeps, in entries; the memo resets when full. */
    static final int MAX_MEMO = 1 << 22;

    /** A fresh memoizing comparator sized for a sort of {@code distinctNodes} nodes. */
    public static NodeComparator sortComparator(long distinctNodes) {
        return new CachingNodeComparator((int) Math.max(1 << 16, Math.min(distinctNodes, MAX_MEMO)));
    }

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
        Arrays.parallelSort(array, sortComparator(array.length));
    }
}
