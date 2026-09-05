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
     * The canary: this pins the UPSTREAM Jena bug the tie-break exists to
     * repair. When this test fails, Jena has fixed NodeCmp - file nothing,
     * celebrate, and consider simplifying
     * NodeComparator#compareExactLiteralTerms (its refinement then becomes
     * unreachable but stays harmless).
     */
    @Test
    void jenaNodeCmpGapStillPresent() {
        Node ltr = NodeFactory.createLiteralDirLang("abc", "en", "ltr");
        Node rtl = NodeFactory.createLiteralDirLang("abc", "en", "rtl");
        Node fr = NodeFactory.createLiteralDirLang("abc", "fr", "rtl");
        assertEquals(0, NodeCmp.compareRDFTerms(ltr, rtl),
                "Jena fixed NodeCmp for base direction - see this test's javadoc");
        assertEquals(0, NodeCmp.compareRDFTerms(ltr, fr),
                "Jena fixed NodeCmp for dirLangString language tags - see this test's javadoc");
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
