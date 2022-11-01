package com.ebremer.beakgraph.rdf;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;

public class BG {
    private static final Model m = ModelFactory.createDefaultModel();
    public static final String NS = "https://www.ebremer.com/beakgraph/ns/";
    public static final Resource BeakGraph = m.createResource(NS+"BeakGraph");
    public static final Resource BeakDataSet = m.createResource(NS+"BeakDataSet");
    public static final Resource PredicateVector = m.createResource(NS+"PredicateVector");
    public static final Resource Dictionary = m.createResource(NS+"Dictionary");
}
