package com.ebremer.beakgraph.hdf5.writers;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The rules of record classes (RDFARRY.md §3.5a) one sequence at a time:
 * signatures and their spelling, unification of list items, the leaves that
 * pack, and that every packed date and dateTime rebuilds its exact lexical
 * form.
 */
class RecordArraysTest {

    private static final SequenceArrays.TimeUnit US = SequenceArrays.TimeUnit.US;

    /** The sequence {@code list} (Turtle, prefixes {@code :} and {@code xsd:}). */
    private static Node seq(String list) {
        Graph g = RDFParser.fromString("""
                @prefix : <http://ex.org/> .
                @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
                :s :p %s .
                """.formatted(list), Lang.TURTLE).toGraph();
        return g.find().next().getObject();
    }

    private static String signature(String list) {
        RecordArrays.RecRow row = RecordArrays.probe(1, seq(list), US, 1);
        return (row == null) ? null : row.signature();
    }

    @Test
    void signaturesSpellTheTypeStructure() {
        assertEquals("(str,i64)", signature("( \"a\" 1 )"));
        assertEquals("list(str)", signature("( \"a\" \"b\" \"c\" )"));
        assertEquals("list((str,str,(i64,str)))", signature(
                "( ( \"8480-6\" \"systolic\" ( 120 \"mm[Hg]\" ) ) ( \"8462-4\" \"diastolic\" ( 80 \"mm[Hg]\" ) ) )"));
        assertEquals("(str,f64[3])", signature("( \"name\" ( 1.0E0 2.0E0 3.0E0 ) )"), "a numeric block is one column");
        assertEquals("(str,i64[2x2])", signature("( \"m\" ( ( 1 2 ) ( 3 4 ) ) )"));
        assertEquals("(iri,lang,bool)", signature("( :a \"chat\"@fr true )"));
        assertEquals("(i64,long)", signature("( 1 \"2\"^^xsd:long )"), "xsd:integer and xsd:long are different leaves");
        assertEquals("(i64,f64)", signature("( 1 2.5E0 )"), "mixed numbers are never converted to one type");
        // An inner list is a tuple: inner lists of other lengths are other signatures.
        assertEquals("list((str,(str,str)))", signature("( ( \"a\" ( \"x\" \"y\" ) ) )"));
        assertEquals("((str,str),(str))", signature("( ( \"a\" \"b\" ) ( \"c\" ) )"));
    }

    @Test
    void aSlotTakesTheLeafAnotherItemHas() {
        assertEquals("list(str)", signature("( \"a\" undef \"c\" )"));
        assertEquals("list((str,i64))", signature("( ( \"a\" undef ) ( undef 2 ) )"));
        assertEquals("(str,f64[3])", signature("( \"e\" ( 1.0E0 undef 3.0E0 ) )"), "a slot at a leaf of a block");
        assertNull(signature("( ( \"a\" undef ) ( \"b\" undef ) )"), "a position no item types");
        assertNull(signature("( ( \"a\" 1 ) undef )"), "a slot where an item belongs");
        // A component with a value and no unit is still the others' tuple, not a block of two integers.
        assertEquals("list((str,(i64,str)))", signature("( ( \"a\" ( 120 undef ) ) ( \"b\" ( 80 \"mm[Hg]\" ) ) )"));
        // A block a slot fills is typed by another item.
        assertEquals("list((str,i64[2]))", signature("( ( \"a\" ( 1 2 ) ) ( \"b\" ( undef undef ) ) )"));
        assertNull(signature("( \"m\" ( ( 1 2 ) undef ) )"), "a slot where a row of a block belongs");
        assertEquals("list(i64)", signature("( undef 1 )"), "numeric: section 3.5 takes it first in a store");
    }

    @Test
    void whatHasNoRecordSignature() {
        assertNull(signature("( \"a\" _:b )"), "a blank node");
        assertNull(signature("( \"a\" 0.5 )"), "xsd:decimal");
        assertNull(signature("( \"a\" ( ) )"), "an empty sequence");
        assertNull(signature("( )"));
        assertNull(signature("( \"a\" \"05\"^^xsd:integer )"), "a non-canonical integer");
        assertNull(signature("( \"a\" \"x\"^^<http://ex.org/dt> )"), "an unknown datatype");
        assertNull(signature("( \"a\" \"b\\u0000c\" )"), "U+0000, where h5py ends a string");
        assertEquals("list(str)", signature("( \"a\" \"\\U0001F600\" )"), "a surrogate pair is a character");
        assertNull(RecordArrays.probe(1, seq("( \"a\" )"), US, 2), "fewer leaves than -arrays-min");
    }

    @Test
    void aLoneSurrogateDoesNotPack() {
        assertNull(RecordArrays.typed(NodeFactory.createLiteralString("a\uD800"), US));
        assertNull(RecordArrays.typed(NodeFactory.createLiteralString("\uDC00b"), US));
        assertNotNull(RecordArrays.typed(NodeFactory.createLiteralString("\uD83D\uDE00"), US));
    }

    @Test
    void aDirectionalStringDoesNotPack() {
        assertNull(RecordArrays.typed(NodeFactory.createLiteralDirLang("abc", "ar", "rtl"), US));
        RecordArrays.Typed t = RecordArrays.typed(NodeFactory.createLiteralLang("abc", "en-GB"), US);
        assertEquals("lang", t.leaf().spelling());
        assertArrayEquals(new String[]{"abc", "en-GB"}, (String[]) t.value());
    }

    private static Node dateTime(String lex) {
        return NodeFactory.createLiteralDT(lex, XSDDatatype.XSDdateTime);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "2024-01-01T10:00:00", "2024-01-01T10:00:00Z", "2024-01-01T10:00:00.5Z", "2024-01-01T10:00:00.123456+05:00",
        "1969-12-31T23:59:59.25", "0001-01-01T00:00:00Z", "9999-12-31T23:59:59.999999-14:00", "2024-02-29T00:00:00+14:00",
        "1970-01-01T00:00:00-00:30"})
    void aPackedDateTimeRebuildsItsLexicalForm(String lex) {
        RecordArrays.Typed t = RecordArrays.typed(dateTime(lex), US);
        assertNotNull(t, lex);
        String zone = t.leaf().zone();
        assertEquals(lex, RecordArrays.formatDateTime((Long) t.value(), zone, US));
        assertEquals("dt" + zone, t.leaf().spelling());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "2024-01-01T10:00:00.50Z", "2024-01-01T10:00:00.0", "2024-01-01T10:00:00+00:00", "2024-01-01T10:00:00-00:00",
        "2024-01-01T24:00:00", "2023-02-29T00:00:00", "0000-01-01T00:00:00", "12024-01-01T00:00:00",
        "2024-01-01T10:00:00.1234567Z", "2024-01-01T10:00:00+14:30", "2024-01-01T10:00", "2024-01-01T10:00:60"})
    void aDateTimeThatWouldNotRebuildDoesNotPack(String lex) {
        assertNull(RecordArrays.typed(dateTime(lex), US), lex);
    }

    @Test
    void aZonedDateTimeIsItsUtcInstant() {
        RecordArrays.Typed z = RecordArrays.typed(dateTime("2024-01-01T10:00:00+05:00"), US);
        RecordArrays.Typed utc = RecordArrays.typed(dateTime("2024-01-01T05:00:00Z"), US);
        assertEquals(utc.value(), z.value());
        assertEquals(1_704_085_200_000_000L, z.value());
        // The same instant in another zone is another term, so another leaf.
        assertTrue(!z.leaf().equals(utc.leaf()));
    }

    @Test
    void theTimeUnitBoundsTheFraction() {
        Node ms = dateTime("2024-01-01T10:00:00.123Z");
        assertEquals(1_704_103_200_123L, RecordArrays.typed(ms, SequenceArrays.TimeUnit.MS).value());
        assertNull(RecordArrays.typed(dateTime("2024-01-01T10:00:00.1234Z"), SequenceArrays.TimeUnit.MS));
        assertNull(RecordArrays.typed(ms, SequenceArrays.TimeUnit.S));
        assertEquals(1_704_103_200L, RecordArrays.typed(dateTime("2024-01-01T10:00:00Z"), SequenceArrays.TimeUnit.S).value());
        assertEquals(1_704_103_200_123_456_789L,
                RecordArrays.typed(dateTime("2024-01-01T10:00:00.123456789Z"), SequenceArrays.TimeUnit.NS).value());
        assertNull(RecordArrays.typed(dateTime("2400-01-01T00:00:00Z"), SequenceArrays.TimeUnit.NS),
                "past int64 nanoseconds (2262)");
        assertEquals(SequenceArrays.TimeUnit.MS, SequenceArrays.TimeUnit.of("ms"));
        assertThrows(IllegalArgumentException.class, () -> SequenceArrays.TimeUnit.of("min"));
    }

    @Test
    void aPackedDateRebuildsItsLexicalForm() {
        for (String lex : List.of("2024-02-29", "1970-01-01", "1969-12-31", "0001-01-01", "9999-12-31")) {
            RecordArrays.Typed t = RecordArrays.typed(NodeFactory.createLiteralDT(lex, XSDDatatype.XSDdate), US);
            assertNotNull(t, lex);
            assertEquals(lex, RecordArrays.formatDate((Long) t.value()));
        }
        assertEquals(19_782L, RecordArrays.typed(NodeFactory.createLiteralDT("2024-02-29", XSDDatatype.XSDdate), US).value());
        for (String lex : List.of("2023-02-29", "2024-01-01Z", "2024-1-01", "0000-01-01", "2024-01-01+05:00")) {
            assertNull(RecordArrays.typed(NodeFactory.createLiteralDT(lex, XSDDatatype.XSDdate), US), lex);
        }
    }

    @Test
    void aSignatureParsesBackToItself() {
        for (String s : List.of("(str,i64)", "list(str)", "list((str,str,(i64,str)))", "(str,f64[768])",
                "(dt+05:00,dtZ,dt,date)", "list((iri,lang,bool[2x3]))", "(i64,long,i32,f32)")) {
            RecordArrays.Parsed p = RecordArrays.parse(s);
            assertEquals(s, (p.list() ? "list(" + RecordArrays.spell(p.item()) + ")" : RecordArrays.spell(p.item())));
        }
        for (String bad : List.of("str", "(str", "(str,)", "(nope)", "(f64[0])", "list(str))", "(dt+00:00)", "(str[2])")) {
            assertThrows(IllegalArgumentException.class, () -> RecordArrays.parse(bad), bad);
        }
    }

    @Test
    void columnsArePositionPaths() {
        RecordArrays.Parsed p = RecordArrays.parse("list((str,str,(i64,str),f64[2x3]))");
        List<RecordArrays.Column> columns = RecordArrays.columns(p.item());
        assertEquals(List.of("f1", "f2", "f3_1", "f3_2", "f4"), columns.stream().map(RecordArrays.Column::name).toList());
        assertEquals(6, columns.get(4).width());
        assertEquals(4, columns.get(4).offset());
        assertEquals(List.of("f"), RecordArrays.columns(RecordArrays.parse("list(str)").item())
                .stream().map(RecordArrays.Column::name).toList(), "a list of leaves has one column");
    }

    @Test
    void recordClassesNumberAfterTheOthers() {
        assertEquals(List.of("f64_r", "i64_7", "rec_1", "rec_2", "rec_10"),
                SequenceArrays.classOrder(List.of("rec_10", "i64_7", "rec_2", "f64_r", "rec_1")));
    }
}
