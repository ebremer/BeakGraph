package com.ebremer.beakgraph.hdtish;

import java.util.ArrayList;
import java.util.Collections;
import org.apache.jena.graph.Node;

public class NodeSearch {
    public static final NodeComparator comp = new NodeComparator();
    
    public static int findPosition(ArrayList<Node> list, Node y) {
        int index = Collections.binarySearch(list, y, comp);
        if (index >= 0) {
            return index;
        } else {
            return -1;
        }
    }
}