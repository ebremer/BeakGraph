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
import static org.junit.jupiter.api.Assertions.assertNotEquals;

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

    // ---- exact counts (Counts), in a store shaped like the 38-billion-quad OKN store ----

    private static final Node TYPE      = NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    private static final Node FACILITY  = NodeFactory.createURI("http://w3id.org/fio/v1/fio#Facility");
    private static final Node INDUSTRY  = NodeFactory.createURI("http://w3id.org/fio/v1/fio#ofIndustry");
    private static final Node LANDFILL  = NodeFactory.createURI("http://w3id.org/fio/v1/naics#NAICS-562212");
    private static final Node CONNECTED = NodeFactory.createURI("http://purl.org/spatialai/spatial/spatial-full#connectedTo");
    private static final Node REGION3   = NodeFactory.createURI("http://stko-kwg.geog.ucsb.edu/lod/ontology/AdministrativeRegion_3");

    /** The OKN store's shape: dividing by its ~8.7 billion distinct objects puts every ?s <p> <o> below one match. */
    private static BGReorderTransform okn(BGReorderTransform.Counts counts) {
        Map<Node, Long> cP = new HashMap<>();
        cP.put(TYPE, 6_413_078_641L);
        cP.put(INDUSTRY, 7_504_626L);
        cP.put(CONNECTED, 152_433_602L);
        return new BGReorderTransform(VoidStats.of(38_281_567_289L, 4_000_000_000L, 8_713_000_895L, 68_868, cP), counts);
    }

    /** The matches each pattern has in the OKN store. */
    private static long oknMatches(Node p, Node o) {
        if (p.equals(TYPE) && FACILITY.equals(o)) return 4_860_757L;
        if (p.equals(INDUSTRY) && LANDFILL.equals(o)) return 14_306L;
        if (p.equals(TYPE) && REGION3.equals(o)) return 30_000L;
        if (p.equals(CONNECTED) && o == null) return 152_433_602L;
        return -1;
    }

    @Test
    void withoutExactCountsTwoTypedPatternsTieAndKeepTheirWrittenOrder() {
        // The fault the counts fix: both estimates fall below 1, are clamped to 1, and tie.
        Var f = Var.alloc("f");
        BasicPattern bp = new BasicPattern();
        bp.add(Triple.create(f, TYPE, FACILITY));
        bp.add(Triple.create(f, INDUSTRY, LANDFILL));
        assertEquals(TYPE, okn(null).reorder(bp).get(0).getPredicate());
    }

    @Test
    void exactCountsPutTheSelectivePatternFirst() {
        Var f = Var.alloc("f");
        BasicPattern bp = new BasicPattern();
        bp.add(Triple.create(f, TYPE, FACILITY));      // 4,860,757 matches
        bp.add(Triple.create(f, INDUSTRY, LANDFILL));  // 14,306 matches
        BasicPattern out = okn(ReorderTransformTest::oknMatches).reorder(bp);
        assertEquals(INDUSTRY, out.get(0).getPredicate(), "the landfills, not every facility, should be read first");
        assertEquals(TYPE, out.get(1).getPredicate());
    }

    @Test
    void anExactCountGovernsOnlyWhileTheSubjectIsUnbound() {
        // fio-spatial-facilities-in-state's core: once ?t is bound by connectedTo, the region
        // check is a lookup per row, not its 30,000 matches - so it goes last, not second.
        Var f = Var.alloc("f"), t = Var.alloc("t");
        BasicPattern bp = new BasicPattern();
        bp.add(Triple.create(f, TYPE, FACILITY));
        bp.add(Triple.create(f, INDUSTRY, LANDFILL));
        bp.add(Triple.create(f, CONNECTED, t));
        bp.add(Triple.create(t, TYPE, REGION3));
        BasicPattern out = okn(ReorderTransformTest::oknMatches).reorder(bp);
        assertEquals(INDUSTRY, out.get(0).getPredicate());
        assertEquals(t, out.get(3).getSubject(), "the region check waits until ?t is bound: " + out);
    }

    @Test
    void aPatternWithNoMatchesGoesFirst() {
        Var f = Var.alloc("f");
        BasicPattern bp = new BasicPattern();
        bp.add(Triple.create(f, TYPE, FACILITY));
        bp.add(Triple.create(f, INDUSTRY, LANDFILL));
        BasicPattern out = okn((p, o) -> p.equals(TYPE) ? 0 : oknMatches(p, o)).reorder(bp);
        assertEquals(TYPE, out.get(0).getPredicate(), "nothing to join: the empty pattern ends the BGP at once");
    }

    @Test
    void theBoundPlaceholderIsNeverCountedAsATerm() {
        // GRAPH ?g's planner hands BeakGraph.BOUND for a variable bound earlier in the block.
        Var f = Var.alloc("f");
        BasicPattern bp = new BasicPattern();
        bp.add(Triple.create(f, CONNECTED, BeakGraph.BOUND));
        bp.add(Triple.create(f, BeakGraph.BOUND, LANDFILL));
        bp.add(Triple.create(f, INDUSTRY, LANDFILL));
        BasicPattern out = okn((p, o) -> {
            assertNotEquals(BeakGraph.BOUND, p, "the placeholder predicate was counted");
            assertNotEquals(BeakGraph.BOUND, o, "the placeholder object was counted");
            return oknMatches(p, o);
        }).reorder(bp);
        assertEquals(3, out.size());
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
