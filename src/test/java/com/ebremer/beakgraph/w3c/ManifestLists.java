package com.ebremer.beakgraph.w3c;

import java.util.Iterator;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFList;
import org.apache.jena.rdf.model.RDFNode;

/**
 * The members of a test manifest's list ({@code mf:entries}, {@code mf:include}). Under
 * rdf-array a Turtle collection is one sequence term, not an {@code rdf:first}/{@code rdf:rest}
 * ladder, so a runner has to read both: rdf-array.md, "Test manifests".
 */
public final class ManifestLists {

    private ManifestLists() {}

    public static Iterator<RDFNode> members(RDFNode list) {
        Node node = list.asNode();
        if (node.isNodeSeq()) {
            Model model = list.getModel();
            return node.getSeq().stream().map(model::asRDFNode).iterator();
        }
        return list.as(RDFList.class).iterator();
    }
}
