package com.ebremer.beakgraph.hdtish;

import java.util.ArrayList;
import java.util.HashSet;
import org.apache.jena.graph.Node;

public class NodeSorter {
    public static ArrayList<Node> sortNodes(HashSet<Node> nodes) {
        ArrayList<Node> list = new ArrayList<>(nodes);
        list.sort(new NodeComparator());
        return list;
    }
}