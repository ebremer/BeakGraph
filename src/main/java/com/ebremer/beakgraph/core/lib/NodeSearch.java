package com.ebremer.beakgraph.core.lib;

import java.util.ArrayList;
import java.util.Collections;
import org.apache.jena.graph.Node;

public class NodeSearch {
    
    public static int findPosition(ArrayList<Node> list, Node y) {
        int index = Collections.binarySearch(list, y, NodeComparator.INSTANCE);
        if (index >= 0) {
            return index;
        } else {
            return -1;
        }
    }
}