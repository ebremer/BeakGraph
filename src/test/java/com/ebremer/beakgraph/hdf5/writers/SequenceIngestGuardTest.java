package com.ebremer.beakgraph.hdf5.writers;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.lib.CompoundTerms;
import com.ebremer.beakgraph.core.lib.SeqTerms;
import java.util.List;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;
import org.junit.jupiter.api.Test;

/**
 * The §9.5 guards on sequences and slots (format v7), exercised directly:
 * the fork's parsers already refuse most of these inputs, so the guards are
 * defence in depth for programmatic and future sources, as the predicate
 * guard is (IngestContractTest).
 */
class SequenceIngestGuardTest {

    private static final Node G = Quad.defaultGraphIRI;
    private static final Node S = NodeFactory.createURI("http://ex.org/s");
    private static final Node P = NodeFactory.createURI("http://ex.org/p");
    private static final Node ONE = NodeFactory.createLiteralByValue(1);
    private static final Node SEQ = SeqTerms.create(List.of(ONE, SeqTerms.UNDEF));

    private static String refused(Quad q) {
        PositionalDictionaryWriterBuilder b = new PositionalDictionaryWriterBuilder();
        return assertThrows(IllegalStateException.class, () -> b.processQuadForTest(q), q.toString()).getMessage();
    }

    @Test
    void aSequenceIsStoredAsAnObject() {
        PositionalDictionaryWriterBuilder b = new PositionalDictionaryWriterBuilder();
        assertDoesNotThrow(() -> b.processQuadForTest(new Quad(G, S, P, SEQ)));
        assertDoesNotThrow(() -> b.processQuadForTest(new Quad(G, S, P, SeqTerms.create(List.of(SEQ, SEQ)))));
        assertEquals(2, b.getStats().numSequences, "( 1 undef ) and ( ( 1 undef ) ( 1 undef ) ), each once");
        assertTrue(b.getLiterals().contains(SEQ) && b.getLiterals().contains(ONE));
        assertTrue(!b.getUniqueObjects().contains(ONE), "an element is no role-list object");
    }

    @Test
    void aSequenceIsRefusedAnywhereButAsAnObject() {
        String graph = refused(new Quad(SEQ, S, P, ONE));
        assertTrue(graph.startsWith("Unexpected graph node type") && graph.contains("only as an object"), graph);
        String subject = refused(new Quad(G, SEQ, P, ONE));
        assertTrue(subject.startsWith("Unexpected subject node type") && subject.contains("only as an object"), subject);
        String predicate = assertThrows(IllegalStateException.class,
                () -> PositionalDictionaryWriterBuilder.requirePredicate(SEQ)).getMessage();
        assertTrue(predicate.startsWith("Unexpected predicate node type (not URI") && predicate.contains("only as an object"), predicate);
    }

    @Test
    void undefIsRefusedOutsideASequence() {
        String object = refused(new Quad(G, S, P, SeqTerms.UNDEF));
        assertTrue(object.startsWith("Unexpected object node type") && object.contains("undef may stand only"), object);
        String subject = refused(new Quad(G, SeqTerms.UNDEF, P, ONE));
        assertTrue(subject.contains("undef may stand only"), subject);
    }

    @Test
    void aSequenceInsideATripleTermIsRefused() {
        Node tt = NodeFactory.createTripleTerm(Triple.create(S, P, SEQ));
        String m = refused(new Quad(G, S, P, tt));
        assertTrue(m.startsWith("Unexpected triple-term object") && m.contains("inside a triple term"), m);
        // ... at any depth of triple terms, and inside a sequence's triple term.
        Node nested = NodeFactory.createTripleTerm(Triple.create(S, P, tt));
        assertTrue(refused(new Quad(G, S, P, nested)).contains("inside a triple term"));
        assertTrue(refused(new Quad(G, S, P, SeqTerms.create(List.of(tt)))).contains("inside a triple term"));
    }

    @Test
    void aTripleTermInsideASequenceIsStored() {
        PositionalDictionaryWriterBuilder b = new PositionalDictionaryWriterBuilder();
        Node tt = NodeFactory.createTripleTerm(Triple.create(S, P, ONE));
        assertDoesNotThrow(() -> b.processQuadForTest(new Quad(G, S, P, SeqTerms.create(List.of(tt, ONE)))));
        assertEquals(1, b.getStats().numTripleTerms);
    }

    @Test
    void nestingPastTheBoundIsRefused() {
        Node atBound = deep(SeqTerms.MAX_DEPTH);
        PositionalDictionaryWriterBuilder b = new PositionalDictionaryWriterBuilder();
        assertDoesNotThrow(() -> b.processQuadForTest(new Quad(G, S, P, atBound)));
        assertSame(atBound, CompoundTerms.map(atBound, n -> n), "map keeps an unchanged term");
        Node past = deep(SeqTerms.MAX_DEPTH + 1);
        assertTrue(refused(new Quad(G, S, P, past)).contains("nested deeper than"));
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> CompoundTerms.map(past, n -> n));
        assertTrue(ex.getMessage().contains("nested deeper than"), ex.getMessage());
    }

    @Test
    void mapRebuildsOnlyWhatChangedAndKeepsSlots() {
        Node rel = NodeFactory.createURI("http://ex.org/x");
        Node seq = SeqTerms.create(List.of(ONE, SeqTerms.UNDEF, SeqTerms.create(List.of(rel))));
        assertSame(seq, CompoundTerms.map(seq, n -> n));
        Node two = NodeFactory.createLiteralByValue(2);
        Node mapped = CompoundTerms.map(seq, n -> n.equals(rel) ? two : n);
        assertEquals(SeqTerms.create(List.of(ONE, SeqTerms.UNDEF, SeqTerms.create(List.of(two)))), mapped);
        assertSame(SeqTerms.UNDEF, mapped.getSeq().get(1));
    }

    /** A sequence of {@code levels} nested levels: ( ( ... ( ) ... ) ). */
    private static Node deep(int levels) {
        Node seq = SeqTerms.create(List.of());
        for (int i = 1; i < levels; i++) {
            seq = SeqTerms.create(List.of(seq));
        }
        return seq;
    }
}
