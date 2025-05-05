package com.ebremer.beakgraph.hdtish;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.jena.graph.Node;

public class NodeSorter {
    public static ArrayList<Node> sort(HashSet<Node> nodes) {
        ArrayList<Node> list = new ArrayList<>(nodes);
        list.sort(new NodeComparator());
        return list;
    }
   
    public static ArrayList<Node> parallelSort( Set<Node> nodeSet ) {
        Node[] array = nodeSet.toArray(new Node[0]);
        Arrays.parallelSort(array, new NodeComparator());
        return new ArrayList<>(Arrays.asList(array));
    }
}
