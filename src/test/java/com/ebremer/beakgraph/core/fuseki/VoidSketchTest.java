package com.ebremer.beakgraph.core.fuseki;

import com.ebremer.beakgraph.core.lib.HyperLogLog;
import java.util.List;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.VOID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

/**
 * The bounded-memory VoID statistics: HyperLogLog accuracy, the
 * exact-then-sketch spill of {@link DistinctNodeCounter}, determinism, and -
 * most importantly - that BELOW the spill threshold every reported VoID
 * number is exactly what the old retained-set implementation produced.
 */
class VoidSketchTest {

    @Test
    void hyperLogLogIsAccurateAtScale() {
        HyperLogLog hll = new HyperLogLog();
        int n = 1_000_000;
        for (long i = 0; i < n; i++) {
            hll.add(HyperLogLog.mix64(i * 0x9E3779B97F4A7C15L + 12345));
        }
        long est = hll.estimate();
        // 2^14 registers -> sigma ~0.81%; 3 sigma bound with margin.
        assertTrue(Math.abs(est - n) < n * 0.03,
                "estimate " + est + " must be within 3% of " + n);
    }

    @Test
    void counterIsExactBelowLimitAndCloseAboveIt() {
        DistinctNodeCounter small = new DistinctNodeCounter(1 << 16);
        for (int i = 0; i < 5_000; i++) {
            small.add(NodeFactory.createURI("http://ex.org/r" + (i % 1_000)));
        }
        assertTrue(small.isExact());
        assertEquals(1_000, small.count(), "below the limit the count is exact");

        DistinctNodeCounter big = new DistinctNodeCounter(1_000);
        int n = 50_000;
        for (int i = 0; i < n; i++) {
            big.add(NodeFactory.createURI("http://ex.org/r" + i));
        }
        assertFalse(big.isExact(), "the counter must have spilled to the sketch");
        assertTrue(Math.abs(big.count() - n) < n * 0.05,
                "sketched count " + big.count() + " must be within 5% of " + n);

        // Determinism: same inputs -> same estimate, regardless of duplicates.
        DistinctNodeCounter again = new DistinctNodeCounter(1_000);
        for (int round = 0; round < 2; round++) {
            for (int i = 0; i < n; i++) {
                again.add(NodeFactory.createURI("http://ex.org/r" + i));
            }
        }
        assertEquals(big.count(), again.count(), "estimates are a pure function of the distinct set");
    }

    @Test
    void smallStoreVoidNumbersAreExact() {
        BGVoIDSD v = new BGVoIDSD("https://ebremer.com/void/");
        for (int i = 0; i < 200; i++) {
            v.add(new Quad(Quad.defaultGraphIRI,
                    NodeFactory.createURI("http://ex.org/data/s" + (i % 40)),
                    NodeFactory.createURI("http://ex.org/p" + (i % 5)),
                    NodeFactory.createURI("http://ex.org/data/o" + (i % 60))));
        }
        for (int i = 0; i < 30; i++) {
            v.add(new Quad(Quad.defaultGraphIRI,
                    NodeFactory.createURI("http://ex.org/data/s" + i),
                    RDF.type.asNode(),
                    NodeFactory.createURI("http://ex.org/Article")));
        }
        Model m = v.getModel();
        assertEquals(40, one(m, VOID.distinctSubjects), "distinct subjects exact");
        assertEquals(61, one(m, VOID.distinctObjects), "distinct objects exact (60 + the class)");
        assertEquals(1, one(m, VOID.classes));
        assertEquals(30, one(m, VOID.entities), "typed instances exact");
        assertEquals(230, one(m, VOID.triples));
        // uriSpace: LCP of http://ex.org/data/s*, cut to the last '/'
        List<String> spaces = m.listObjectsOfProperty(VOID.uriSpace)
                .mapWith(n -> n.asLiteral().getString()).toList();
        assertEquals(List.of("http://ex.org/data/"), spaces, "incremental LCP must match the old set-based one");
        assertTrue(m.listObjectsOfProperty(VOID.vocabulary)
                        .mapWith(n -> n.asResource().getURI()).toSet()
                        .contains("http://ex.org/"),
                "object/predicate namespaces must land in void:vocabulary");
    }

    private static long one(Model m, org.apache.jena.rdf.model.Property p) {
        var it = m.listObjectsOfProperty(p);
        assertTrue(it.hasNext(), "expected a value for " + p);
        return it.next().asLiteral().getLong();
    }
}
