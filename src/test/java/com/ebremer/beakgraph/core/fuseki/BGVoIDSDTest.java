package com.ebremer.beakgraph.core.fuseki;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sys.JenaSystem;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.VOID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests that VoID/SD generation does not derive a bogus, relative
 * {@code void:uriSpace} from document-relative (storage-form) IRIs.
 */
class BGVoIDSDTest {

    @BeforeAll
    static void initJena() {
        JenaSystem.init();
    }

    private static Quad typeQuad(String subject, String type) {
        return Quad.create(Quad.defaultGraphIRI,
                NodeFactory.createURI(subject),
                RDF.type.asNode(),
                NodeFactory.createURI(type));
    }

    @Test
    void relativeSubjectsDoNotProduceAUriSpace() {
        BGVoIDSD v = new BGVoIDSD("https://example.org/ds");
        // relative subjects sharing a long prefix - would wrongly yield a
        // relative void:uriSpace if not skipped
        v.add(typeQuad("subdirectory/alpha", "https://schema.org/Thing"));
        v.add(typeQuad("subdirectory/beta", "https://schema.org/Thing"));
        Model m = v.getModel();
        assertFalse(m.listStatements(null, VOID.uriSpace, (RDFNode) null).hasNext(),
                "relative IRIs must not produce a void:uriSpace");
    }

    @Test
    void absoluteSubjectsStillProduceAUriSpace() {
        BGVoIDSD v = new BGVoIDSD("https://example.org/ds");
        v.add(typeQuad("http://data.example.org/records/alpha", "https://schema.org/Thing"));
        v.add(typeQuad("http://data.example.org/records/beta", "https://schema.org/Thing"));
        Model m = v.getModel();
        assertTrue(m.listStatements(null, VOID.uriSpace, (RDFNode) null).hasNext(),
                "absolute IRIs with a shared namespace should still yield a void:uriSpace");
    }
}
