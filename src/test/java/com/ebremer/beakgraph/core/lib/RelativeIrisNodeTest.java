package com.ebremer.beakgraph.core.lib;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;
import org.junit.jupiter.api.Test;

/**
 * BG-432 / BG-297: the ONE node relativizer every engine delegates to.
 * Pins the storage forms the two former copies each produced.
 */
class RelativeIrisNodeTest {

    private static Node uri(String s) {
        return NodeFactory.createURI(s);
    }

    @Test
    void sentinelBasedIrisBecomeRelativeReferences() {
        assertEquals(uri(""), RelativeIris.relativizeNode(uri(RelativeIris.SENTINEL_BASE)));
        assertEquals(uri("#frag"), RelativeIris.relativizeNode(uri(RelativeIris.SENTINEL_BASE + "#frag")));
        assertEquals(uri("?q=1"), RelativeIris.relativizeNode(uri(RelativeIris.SENTINEL_BASE + "?q=1")));
        assertEquals(uri("x.png"), RelativeIris.relativizeNode(uri(RelativeIris.SENTINEL_DIR + "x.png")));
        assertEquals(uri("a/b.png"), RelativeIris.relativizeNode(uri(RelativeIris.SENTINEL_DIR + "a/b.png")));
        String oneUp = RelativeIris.SENTINEL_PREFIX + "d/".repeat(RelativeIris.SENTINEL_DEPTH - 1) + "up.png";
        assertEquals(uri("../up.png"), RelativeIris.relativizeNode(uri(oneUp)));
        String twoUp = RelativeIris.SENTINEL_PREFIX + "d/".repeat(RelativeIris.SENTINEL_DEPTH - 2) + "up2.png";
        assertEquals(uri("../../up2.png"), RelativeIris.relativizeNode(uri(twoUp)));
        assertEquals(uri("/root.png"), RelativeIris.relativizeNode(uri(RelativeIris.SENTINEL_PREFIX + "root.png")));
    }

    @Test
    void everythingElseIsTheSameInstance() {
        Node abs = uri("http://ex.org/a");
        assertSame(abs, RelativeIris.relativizeNode(abs));
        Node blank = NodeFactory.createBlankNode("b");
        assertSame(blank, RelativeIris.relativizeNode(blank));
        Node lit = NodeFactory.createLiteralString(RelativeIris.SENTINEL_BASE);
        assertSame(lit, RelativeIris.relativizeNode(lit), "a literal is never rewritten, whatever it says");
        assertSame(null, RelativeIris.relativizeNode(null));
        Quad q = new Quad(abs, abs, abs, lit);
        assertSame(q, RelativeIris.relativizeQuad(q), "nothing to change: the same quad instance");
    }

    @Test
    void tripleTermsAreRelativizedHoweverDeep() {
        Node p = uri("http://ex.org/p");
        Node inner = NodeFactory.createTripleTerm(Triple.create(uri(RelativeIris.SENTINEL_BASE), p, uri(RelativeIris.SENTINEL_DIR + "x")));
        Node outer = NodeFactory.createTripleTerm(Triple.create(uri("http://ex.org/s"), p, inner));
        Node expectedInner = NodeFactory.createTripleTerm(Triple.create(uri(""), p, uri("x")));
        Node expected = NodeFactory.createTripleTerm(Triple.create(uri("http://ex.org/s"), p, expectedInner));
        assertEquals(expected, RelativeIris.relativizeNode(outer));
        Quad q = new Quad(uri(RelativeIris.SENTINEL_DIR + "g"), uri("http://ex.org/s"), p, outer);
        Quad r = RelativeIris.relativizeQuad(q);
        assertEquals(uri("g"), r.getGraph());
        assertEquals(expected, r.getObject());
    }
}
