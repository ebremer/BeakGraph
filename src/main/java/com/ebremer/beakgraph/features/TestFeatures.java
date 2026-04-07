package com.ebremer.beakgraph.features;

import com.ebremer.beakgraph.features.pyradiomics.Gen2DFeatures;
import com.ebremer.ns.GEO;
import com.ebremer.ns.HAL;
import com.ebremer.ns.PYR;
import java.util.ArrayList;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.XSD;

public class TestFeatures {
    public static void main2(String[] args) {
        Model m = ModelFactory.createDefaultModel();
        Resource f = m.createResource("http://example.org/feature/1");
        f.addProperty(RDF.type, GEO.Geometry);
        String wkt = "POLYGON((0 0, 100 0, 100 100, 0 100, 0 0))";
        MajorMinor.Add(f, wkt);
        Gen2DFeatures.Generate(f, wkt);
        m.setNsPrefix("pyr", PYR.NS);
        m.setNsPrefix("xsd", XSD.NS);
        m.setNsPrefix("geo", GEO.NS);
        m.setNsPrefix("hal", HAL.NS);
        m.write(System.out, "TURTLE");
    }
    
    public static void main(String[] args) {
        ArrayList<Quad> quads = new ArrayList<>();
        Node f = NodeFactory.createURI("http://example.org/feature/1");
        Node graph = Quad.defaultGraphIRI;
        quads.add(Quad.create(graph, f, RDF.type.asNode(), GEO.Geometry.asNode()));
        String wkt = "POLYGON((0 0, 100 0, 100 100, 0 100, 0 0))";
        MajorMinor.Add(quads, f, wkt);
        Gen2DFeatures.Generate(quads, f, wkt);
        Model m = ModelFactory.createDefaultModel();
        for (Quad q : quads) {
            if (q.getGraph().equals(Quad.defaultGraphIRI)) m.getGraph().add(q.asTriple());
        }
        m.setNsPrefix("pyr", PYR.NS);
        m.setNsPrefix("xsd", XSD.NS);
        m.setNsPrefix("geo", GEO.NS);
        m.setNsPrefix("hal", HAL.NS);
        m.write(System.out, "TURTLE");
    }
}