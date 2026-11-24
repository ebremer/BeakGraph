package com.ebremer.beakgraph.core.lib;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.util.NodeCmp;
import org.junit.jupiter.api.Test;

/**
 * Pins {@link NodeComparator}'s dirLangString tie-break, the
 * hard prerequisite for storing rdf:dirLangString: Jena 6.1.0's
 * {@code NodeCmp.compareRDFTerms} answers 0 for DISTINCT dirLangString
 * literals (it ignores both the language tag and the base direction), and
 * dictionary ids are comparator ranks - so without the tie-break, distinct
 * terms collapse onto one id.
 */
class NodeComparatorDirLangTest {

    @org.junit.jupiter.api.BeforeAll
    static void initJena() {
        // Comparator tests touch TypeMapper/NodeValue without building a store
        // first; initialize Jena explicitly so class-init order cannot NPE.
        org.apache.jena.sys.JenaSystem.init();
    }

    private static int sign(Node a, Node b) {
        return Integer.signum(NodeComparator.INSTANCE.compare(a, b));
    }

    /**
     * Pins the gap the tie-break exists to repair. Jena 6.3.0's own NodeCmp
     * closed it, but in the same release moved its order to code points
     * (GH-4300, GH-4365), so NodeComparator no longer delegates to Jena: it
     * keeps Jena 6.2.0's order in FrozenNodeCmp, gap included, and
     * NodeComparator#compareExactLiteralTerms repairs the gap there.
     */
    @Test
    void frozenNodeCmpGapStillPresent() {
        Node ltr = NodeFactory.createLiteralDirLang("abc", "en", "ltr");
        Node rtl = NodeFactory.createLiteralDirLang("abc", "en", "rtl");
        Node fr = NodeFactory.createLiteralDirLang("abc", "fr", "rtl");
        assertEquals(0, FrozenNodeCmp.compareRDFTerms(ltr, rtl),
                "FrozenNodeCmp must keep Jena 6.2.0's order, which ignores the base direction");
        assertEquals(0, FrozenNodeCmp.compareRDFTerms(ltr, fr),
                "FrozenNodeCmp must keep Jena 6.2.0's order, which ignores a dirLangString's language tag");
    }

    /**
     * The upstream canary: Jena 6.3.0 orders distinct dirLangString literals.
     * Should this fail, Jena changed NodeCmp again; nothing here depends on it.
     */
    @Test
    void jenaNodeCmpOrdersDirLangStringsSince630() {
        Node ltr = NodeFactory.createLiteralDirLang("abc", "en", "ltr");
        Node rtl = NodeFactory.createLiteralDirLang("abc", "en", "rtl");
        assertTrue(NodeCmp.compareRDFTerms(ltr, rtl) != 0, "Jena 6.3.0 tells base directions apart");
    }

    /**
     * BG-370: NodeComparator compares language tags case-sensitively, which
     * is a total order over terms only because Jena canonicalizes every tag
     * at construction (NodeFactory.createLiteralLang / createLiteralDirLang
     * -> LangTagX.formatLanguageTag). When this test fails, that precondition
     * is gone and the comparator needs a case-insensitive primary key on the
     * tag (see the comment in NodeComparator's language branch).
     */
    @Test
    void jenaTagFormattingStillPresent() {
        assertEquals("en-US", NodeFactory.createLiteralLang("x", "EN-us").getLiteralLanguage());
        assertEquals(NodeFactory.createLiteralLang("x", "en"), NodeFactory.createLiteralLang("x", "EN"));
        assertEquals(0, NodeComparator.INSTANCE.compare(
                NodeFactory.createLiteralLang("x", "EN"), NodeFactory.createLiteralLang("x", "en")));
        assertEquals(NodeFactory.createLiteralDirLang("x", "EN", "ltr"),
                NodeFactory.createLiteralDirLang("x", "en", "ltr"));
        assertEquals(0, NodeComparator.INSTANCE.compare(
                NodeFactory.createLiteralDirLang("x", "EN", "ltr"),
                NodeFactory.createLiteralDirLang("x", "en", "ltr")));
    }

    @Test
    void distinctDirLangTermsGetDistinctRanks() {
        Node enLtr = NodeFactory.createLiteralDirLang("abc", "en", "ltr");
        Node enRtl = NodeFactory.createLiteralDirLang("abc", "en", "rtl");
        Node frLtr = NodeFactory.createLiteralDirLang("abc", "fr", "ltr");
        Node frRtl = NodeFactory.createLiteralDirLang("abc", "fr", "rtl");

        Node[] all = {enLtr, enRtl, frLtr, frRtl};
        for (Node a : all) {
            for (Node b : all) {
                if (a.equals(b)) {
                    assertEquals(0, sign(a, b));
                } else {
                    assertTrue(sign(a, b) != 0, "distinct terms must not tie: " + a + " vs " + b);
                    assertEquals(-sign(b, a), sign(a, b), "antisymmetry: " + a + " vs " + b);
                }
            }
        }
        // The chosen refinement: language tag first, then absent < ltr < rtl.
        assertEquals(-1, sign(enLtr, enRtl));
        assertEquals(-1, sign(enRtl, frLtr));
    }

    @Test
    void orderingIsATotalOrderAcrossLangKinds() {
        List<Node> cand = new ArrayList<>();
        for (String lex : new String[]{"abc", "zzz"}) {
            cand.add(NodeFactory.createLiteralString(lex));
            cand.add(NodeFactory.createLiteralLang(lex, "en"));
            cand.add(NodeFactory.createLiteralLang(lex, "fr"));
            cand.add(NodeFactory.createLiteralDirLang(lex, "en", "ltr"));
            cand.add(NodeFactory.createLiteralDirLang(lex, "en", "rtl"));
            cand.add(NodeFactory.createLiteralDirLang(lex, "fr", "ltr"));
            cand.add(NodeFactory.createLiteralDirLang(lex, "fr", "rtl"));
        }
        int asymmetry = 0;
        int intransitive = 0;
        for (Node a : cand) {
            for (Node b : cand) {
                if (sign(a, b) != -sign(b, a)) {
                    asymmetry++;
                }
            }
        }
        for (Node a : cand) {
            for (Node b : cand) {
                for (Node c : cand) {
                    if (sign(a, b) < 0 && sign(b, c) < 0 && sign(a, c) >= 0) {
                        intransitive++;
                    }
                }
            }
        }
        assertEquals(0, asymmetry);
        assertEquals(0, intransitive);
    }

    @Test
    void shuffledSortsAgree() {
        List<Node> pool = new ArrayList<>();
        for (int k = 0; k < 20; k++) {
            pool.add(NodeFactory.createLiteralDirLang("v" + (k % 7), (k % 2 == 0) ? "en" : "fr",
                    (k % 3 == 0) ? "ltr" : "rtl"));
            pool.add(NodeFactory.createLiteralLang("v" + (k % 7), "en"));
        }
        List<Node> reference = null;
        for (int seed = 0; seed < 8; seed++) {
            List<Node> shuffled = new ArrayList<>(pool);
            Collections.shuffle(shuffled, new Random(seed));
            shuffled.sort(NodeComparator.INSTANCE);
            if (reference == null) {
                reference = shuffled;
            } else {
                assertEquals(reference, shuffled, "sort order depended on input order (seed " + seed + ")");
            }
        }
    }
}
