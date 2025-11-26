package com.ebremer.beakgraph.core.lib;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.jena.graph.Node;

public class NodeSorter {
    public static ArrayList<Node> sort2(HashSet<Node> nodes) {
        ArrayList<Node> list = new ArrayList<>(nodes);
        list.sort( NodeComparator.INSTANCE );
        return list;
    }
   
    public static ArrayList<Node> parallelSort( Set<Node> nodeSet ) {
        Node[] array = nodeSet.toArray(new Node[0]);
        Arrays.parallelSort(array, NodeComparator.INSTANCE);
        return new ArrayList<>(Arrays.asList(array));
    }
}
