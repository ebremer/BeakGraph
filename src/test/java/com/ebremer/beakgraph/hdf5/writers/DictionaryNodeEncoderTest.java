package com.ebremer.beakgraph.hdf5.writers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.ebremer.beakgraph.core.lib.DataType;
import com.ebremer.beakgraph.hdf5.DictionarySinks;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.TextDirection;
import org.apache.jena.graph.Triple;
import org.junit.jupiter.api.Test;

/**
 * BG-298: the one dictionary node encoder, pinned on list-backed sinks so the
 * exact column contents for every node kind are the contract both the RAM
 * and the streaming dictionary writers now share.
 */
class DictionaryNodeEncoderTest {

    static final class Longs implements DictionarySinks.LongSink {
        final List<Long> values = new ArrayList<>();
        @Override public void writeInteger(int value) { values.add((long) value); }
        @Override public void writeLong(long value) { values.add(value); }
        @Override public long getNumEntries() { return values.size(); }
    }

    static final class Strings implements DictionarySinks.StringSink {
        final List<String> values = new ArrayList<>();
        @Override public void add(String item) { values.add(item); }
        @Override public long getNumEntries() { return values.size(); }
    }

    static final class Reals implements DictionarySinks.RealSink {
        final List<Double> values = new ArrayList<>();
        @Override public void writeFloat(float value) { values.add((double) value); }
        @Override public void writeDouble(double value) { values.add(value); }
        @Override public long getNumEntries() { return values.size(); }
    }

    @Test
    void everyNodeKindLandsInItsColumn() {
        Longs offsets = new Longs(), datatypes = new Longs(), typed = new Longs(), integers = new Longs(), longs = new Longs(),
                langTags = new Longs(), langDirs = new Longs();
        Reals floats = new Reals(), doubles = new Reals();
        Strings iri = new Strings(), strings = new Strings();
        Map<String, Long> dtLookup = new HashMap<>();
        dtLookup.put(XSDDatatype.XSDstring.getURI(), 1L);
        dtLookup.put(XSDDatatype.XSDint.getURI(), 2L);
        Map<String, Long> langLookup = new HashMap<>();
        langLookup.put("en", 1L);
        List<Node> hooked = new ArrayList<>();
        DictionaryNodeEncoder enc = new DictionaryNodeEncoder("literals", 12, offsets, datatypes, typed, integers, longs,
                floats, doubles, iri, strings, langTags, langDirs, dtLookup, langLookup, true,
                tt -> { hooked.add(tt); return 7L; });
        Node tt = NodeFactory.createTripleTerm(Triple.create(NodeFactory.createURI("http://ex.org/a"),
                NodeFactory.createURI("http://ex.org/b"), NodeFactory.createURI("http://ex.org/c")));
        List<Node> nodes = List.of(
                NodeFactory.createBlankNode("b"),
                NodeFactory.createURI("http://ex.org/x"),
                NodeFactory.createURI("relative.png"),
                NodeFactory.createLiteralDT("5", XSDDatatype.XSDint),
                NodeFactory.createLiteralDT("6000000000", XSDDatatype.XSDlong),
                NodeFactory.createLiteralDT("1.5", XSDDatatype.XSDfloat),
                NodeFactory.createLiteralDT("2.5", XSDDatatype.XSDdouble),
                NodeFactory.createLiteralString("plain"),
                NodeFactory.createLiteralLang("hello", "en"),
                NodeFactory.createLiteralDirLang("shalom", "en", TextDirection.RTL),
                NodeFactory.createLiteralDT("abc", XSDDatatype.XSDint),   // ill-typed: strings, datatype kept
                tt);
        for (Node n : nodes) enc.encode(n);
        assertEquals(12, enc.encoded());
        int B = DataType.BNODE.ordinal(), I = DataType.IRI.ordinal(), R = DataType.RELATIVE_IRI.ordinal(), INT = DataType.INTEGER.ordinal(),
                L = DataType.LONG.ordinal(), F = DataType.FLOAT.ordinal(), D = DataType.DOUBLE.ordinal(), S = DataType.STRING.ordinal(),
                TT = DataType.TRIPLE_TERM.ordinal();
        assertEquals(List.of((long) B, (long) I, (long) R, (long) INT, (long) L, (long) F, (long) D, (long) S, (long) S, (long) S, (long) S, (long) TT),
                datatypes.values, "datatypes");
        assertEquals(List.of(0L, 0L, 1L, 0L, 0L, 0L, 0L, 0L, 1L, 2L, 3L, 7L), offsets.values, "offsets: positions in each store, the hook's value for the triple term");
        assertEquals(List.of(0L, 0L, 0L, 2L, 0L, 0L, 0L, 1L, 0L, 0L, 2L, 0L), typed.values, "typedLiterals: datatype ids, 0 for unknown types and non-literals");
        assertEquals(List.of(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 1L, 1L, 0L, 0L), langTags.values, "langTags");
        assertEquals(List.of(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 2L, 0L, 0L), langDirs.values, "langDirs: rtl = 2");
        assertEquals(List.of("http://ex.org/x", "relative.png"), iri.values);
        assertEquals(List.of("plain", "hello", "shalom", "abc"), strings.values);
        assertEquals(List.of(5L), integers.values);
        assertEquals(List.of(6000000000L), longs.values);
        assertEquals(List.of(1.5), floats.values);
        assertEquals(List.of(2.5), doubles.values);
        assertEquals(List.of(tt), hooked);
    }

    @Test
    void missingBuffersFailLoudly() {
        Longs offsets = new Longs(), datatypes = new Longs();
        DictionaryNodeEncoder noStrings = new DictionaryNodeEncoder("entities", 1, offsets, datatypes, null, null, null,
                null, null, new Strings(), null, null, null, Map.of(), Map.of(), false, null);
        assertThrows(IllegalStateException.class, () -> noStrings.encode(NodeFactory.createLiteralString("x")));
        assertThrows(IllegalStateException.class, () -> noStrings.encode(NodeFactory.createTripleTerm(Triple.create(
                NodeFactory.createURI("http://a"), NodeFactory.createURI("http://b"), NodeFactory.createURI("http://c")))));
        assertThrows(IllegalStateException.class, () -> noStrings.encode(NodeFactory.createVariable("v")));
        assertEquals(0, enc(noStrings));
    }

    private static long enc(DictionaryNodeEncoder e) {
        return e.encoded();
    }
}
