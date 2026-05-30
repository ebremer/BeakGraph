package com.ebremer.beakgraph.core.lib;

import java.util.ArrayList;
import org.apache.jena.graph.Node;

public class NodeSearch {

    /**
     * Manual implementation of binary search.
     * @param list The sorted list of Nodes to search.
     * @param y The Node to find.
     * @return The index of the node, or -1 if not found.
     */
    public static int findPosition(ArrayList<Node> list, Node y) {
        int low = 0;
        int high = list.size() - 1;

        while (low <= high) {
            // Using unsigned shift to prevent overflow for very large lists
            int mid = (low + high) >>> 1;
            Node midVal = list.get(mid);
            int cmp = NodeComparator.INSTANCE.compare(midVal, y);
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return mid; // Key found
            }
        }
        return -1; // Key not found
    }
}
