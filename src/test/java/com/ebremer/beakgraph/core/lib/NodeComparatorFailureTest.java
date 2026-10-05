package com.ebremer.beakgraph.core.lib;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.expr.NodeValue;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * BG-19: a literal whose value cannot be built used to be handled by a
 * catch-all around the whole comparison, which ordered that ONE pair by term
 * while every other pair involving the same literal compared by value - the
 * pairwise value/term mix that is cyclic (50 &lt; 7 by term, 7 &lt; 100 by
 * value, 100 &lt; 50 by term) and makes the sort input-order dependent. The
 * fallback is now per literal: the failing literal is classified as an
 * unparseable literal against every partner, so the order stays total.
 */
class NodeComparatorFailureTest {

    @BeforeAll
    static void initJena() {
        org.apache.jena.sys.JenaSystem.init();
    }

    /** Fails to build the value of one specific literal. */
    private static final class Failing extends NodeComparator {
        private final Node poison;

        Failing(Node poison) {
            this.poison = poison;
        }

        @Override
        protected NodeValue nodeValue(Node n) {
            if (n.equals(poison)) {
                throw new IllegalArgumentException("cannot build a value for " + n);
            }
            return super.nodeValue(n);
        }
    }

    private static Node num(String lex) {
        return NodeFactory.createLiteralDT(lex, XSDDatatype.XSDint);
    }

    private static int sign(NodeComparator cmp, Node a, Node b) {
        return Integer.signum(cmp.compare(a, b));
    }

    @Test
    void aLiteralWhoseValueFailsToBuildIsOrderedConsistentlyAgainstEveryPartner() {
        Node poison = num("50");
        Failing cmp = new Failing(poison);
        List<Node> pool = new ArrayList<>();
        for (String lex : new String[]{"5", "7", "50", "60", "100", "1000"}) {
            pool.add(num(lex));
        }
        pool.add(NodeFactory.createLiteralDT("5.5", XSDDatatype.XSDdouble));
        pool.add(NodeFactory.createLiteralDT("55.5", XSDDatatype.XSDdouble));
        pool.add(NodeFactory.createLiteralString("a"));
        pool.add(NodeFactory.createLiteralString("zz"));
        pool.add(NodeFactory.createLiteralDT("true", XSDDatatype.XSDboolean));
        pool.add(NodeFactory.createLiteralDT("2020-01-01T00:00:00Z", XSDDatatype.XSDdateTime));
        pool.add(NodeFactory.createLiteralDT("zz", XSDDatatype.XSDint)); // ill-formed
        pool.add(NodeFactory.createLiteralDT("x", TypeMapper.getInstance().getSafeTypeByName("urn:unknown")));

        int asymmetry = 0;
        int intransitive = 0;
        for (Node a : pool) {
            for (Node b : pool) {
                if (sign(cmp, a, b) != -sign(cmp, b, a)) {
                    asymmetry++;
                }
                for (Node c : pool) {
                    if (sign(cmp, a, b) < 0 && sign(cmp, b, c) < 0 && sign(cmp, a, c) >= 0) {
                        intransitive++;
                    }
                }
            }
        }
        assertEquals(0, asymmetry, "compare(a,b) must be the negation of compare(b,a)");
        assertEquals(0, intransitive, "a < b < c must imply a < c: the failing literal must not be term-ordered per pair");

        // The failing literal is filed with the unparseable literals: after every
        // well-formed value, ordered by term among its cluster.
        assertTrue(cmp.compare(num("1000"), poison) < 0, "a well-formed number ranks before the unparseable cluster");
        assertTrue(cmp.compare(poison, NodeFactory.createLiteralDT("zz", XSDDatatype.XSDint)) < 0, "\"50\" before \"zz\" by term");

        List<Node> reference = null;
        for (int seed = 0; seed < 12; seed++) {
            List<Node> shuffled = new ArrayList<>(pool);
            Collections.shuffle(shuffled, new Random(seed));
            shuffled.sort(cmp);
            if (reference == null) {
                reference = shuffled;
            } else {
                assertEquals(reference, shuffled, "sort order depended on input order (seed " + seed + ")");
            }
        }
    }
}
