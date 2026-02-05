package com.ebremer.beakgraph.core.lib;

import java.util.ArrayList;
import java.util.Collections;
import org.apache.jena.graph.Node;

public class NodeSearch {
    
    public static int findPosition2(ArrayList<Node> list, Node y) {
        int index = Collections.binarySearch(list, y, NodeComparator.INSTANCE);
        if (index >= 0) {
            return index;
        } else {
            return -1;
        }
    }

    /**
     * Manual implementation of binary search.
     * @param list The sorted list of Nodes to search.
     * @param y The Node to find.
     * @return The index of the node, or -1 if not found.
     */
    public static int findPosition(ArrayList<Node> list, Node y) {
    //    IO.println(y+" =================== findPosition ============================");        
    /*
        if (y.isLiteral()) {
            IO.println(y.getLiteralDatatypeURI());
            if (y.getLiteralDatatypeURI().contains("dateTime")) {
                list
                    .stream()
                    .filter(n->!n.toString().contains("POLYGON"))
                    .filter(n->!n.toString().startsWith("_:b"))
                    .forEach(a->IO.println(a));
            }
        }*/
  //      IO.println("=================== begin Search ============================");
        int low = 0;
        int high = list.size() - 1;

        while (low <= high) {
            // Using unsigned shift to prevent overflow for very large lists
            int mid = (low + high) >>> 1;
            Node midVal = list.get(mid);
            //IO.println(y + " : HUNT : " + midVal);
            int cmp = NodeComparator.INSTANCE.compare(midVal, y);
            if (cmp < 0) {
              //  IO.println( midVal + " < " + y );
                low = mid + 1;
            } else if (cmp > 0) {
//                IO.println( y + " < " + midVal );
                high = mid - 1;
            } else {
                return mid; // Key found
            }
        }
        return -1; // Key not found
    }
}