package com.ebremer.beakgraph.core.fuseki;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.TextDirection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The VoID HyperLogLog hash must be TERM-distinct (PLAN C.7 / 2.6): composite
 * (cdt:) literals have no canonical form, so value-equal lexically-distinct
 * literals are distinct terms; and rdf:dirLangString terms differing only in
 * base direction are distinct terms. Hash collisions here would make the
 * statistics graph undercount distinct objects.
 */
class DistinctNodeCounterCdtTest {

    private static final String LIST = "http://w3id.org/awslabs/neptune/SPARQL-CDTs/List";

    @BeforeAll
    static void initJena() {
        org.apache.jena.sys.JenaSystem.init();
    }

    private static Node list(String lex) {
        return NodeFactory.createLiteralDT(lex, TypeMapper.getInstance().getSafeTypeByName(LIST));
    }

    @Test
    void compositeLiteralHashIsTermDistinct() {
        assertTrue(DistinctNodeCounter.hash(list("[1, 2]")) != DistinctNodeCounter.hash(list("[1,2]")),
                "value-equal but lexically distinct composite literals are distinct terms");
        assertEquals(DistinctNodeCounter.hash(list("[1, 2]")), DistinctNodeCounter.hash(list("[1, 2]")));
    }

    @Test
    void baseDirectionIsPartOfTheHash() {
        Node plain = NodeFactory.createLiteralLang("x", "en");
        Node ltr = NodeFactory.createLiteralDirLang("x", "en", TextDirection.LTR);
        Node rtl = NodeFactory.createLiteralDirLang("x", "en", TextDirection.RTL);
        assertTrue(DistinctNodeCounter.hash(plain) != DistinctNodeCounter.hash(ltr));
        assertTrue(DistinctNodeCounter.hash(ltr) != DistinctNodeCounter.hash(rtl));
        assertEquals(DistinctNodeCounter.hash(ltr),
                DistinctNodeCounter.hash(NodeFactory.createLiteralDirLang("x", "en", TextDirection.LTR)));
    }
}
