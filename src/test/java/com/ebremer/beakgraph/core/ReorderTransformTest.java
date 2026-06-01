package com.ebremer.beakgraph.core;

import java.util.HashMap;
import java.util.Map;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for the GSPO/GPOS-aware join reorder, driven by synthetic VoID stats so the ordering
 * decisions are exact and deterministic.
 */
class ReorderTransformTest {

    private static final Node COMMON = NodeFactory.createURI("http://ex/common"); // 995 triples
    private static final Node RARE   = NodeFactory.createURI("http://ex/rare");   // 5 triples
    private static final Node OBJ    = NodeFactory.createURI("http://ex/obj");
    private static final Node S1     = NodeFactory.createURI("http://ex/s1");

    /** T=1000, Ds=100, Do=500, Dp=2, with a skewed predicate distribution. */
    private static BGReorderTransform skewed() {
        Map<Node, Long> cP = new HashMap<>();
        cP.put(COMMON, 995L);
        cP.put(RARE, 5L);
        return new BGReorderTransform(VoidStats.of(1000, 100, 500, 2, cP));
    }

    @Test
    void selectivePredicatePlacedFirst() {
        Var x = Var.alloc("x"), y = Var.alloc("y"), z = Var.alloc("z");
        BasicPattern bp = new BasicPattern();
        bp.add(Triple.create(x, COMMON, y));   // index 0: common predicate (995)
        bp.add(Triple.create(x, RARE, z));     // index 1: rare predicate (5)

        BasicPattern out = skewed().reorder(bp);
        assertEquals(RARE, out.get(0).getPredicate(), "the rare predicate should be evaluated first");
        assertEquals(COMMON, out.get(1).getPredicate());
    }

    @Test
    void objectOnlyPatternDeferredUntilJoinable() {
        Var a = Var.alloc("a"), b = Var.alloc("b");
        BasicPattern bp = new BasicPattern();
        bp.add(Triple.create(a, b, OBJ));       // index 0: object-only -> no GSPO/GPOS prefix
        bp.add(Triple.create(S1, RARE, a));     // index 1: subject + predicate bound -> index prefix

        BasicPattern out = skewed().reorder(bp);
        // The indexable triple must run first; the object-only one is deferred until ?a is bound.
        assertEquals(S1, out.get(0).getSubject(), "the indexable triple should run first");
        assertEquals(OBJ, out.get(1).getObject(), "the object-only triple should be deferred");
    }
}
