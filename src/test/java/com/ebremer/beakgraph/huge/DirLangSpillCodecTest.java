package com.ebremer.beakgraph.huge;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ebremer.beakgraph.core.lib.Stats;
import com.ebremer.beakgraph.hdf5.Types;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.TextDirection;
import org.apache.jena.vocabulary.RDF;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The disk pipeline's handling of rdf:dirLangString, driven directly. This
 * REPLACES DirLangSpillGuardTest per its stated lifecycle: with format v4 the
 * spill codec carries the direction (tag T_LITERAL_DIRLANG) and the streaming
 * dictionary encoder writes the langDirs column, so the former rejection
 * assertions become exact round-trip assertions - the codec's "term identity
 * is preserved" contract now genuinely covers base direction.
 */
class DirLangSpillCodecTest {

    @TempDir
    static Path dir;

    private static Node roundTrip(Node n) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        NodeCodec.writeNode(new DataOutputStream(bos), n);
        return NodeCodec.readNode(new DataInputStream(new ByteArrayInputStream(bos.toByteArray())));
    }

    private static Stats stringStats() {
        Stats s = new Stats();
        s.numStrings = 1;
        s.longestStringLength = 6;
        s.shortestStringLength = 5;
        return s;
    }

    @Test
    void nodeCodecRoundTripsBaseDirectionExactly() throws IOException {
        Node ltr = NodeFactory.createLiteralDirLang("hello", "en", TextDirection.LTR);
        Node rtl = NodeFactory.createLiteralDirLang("shalom", "he", TextDirection.RTL);
        assertEquals(ltr, roundTrip(ltr));
        assertEquals(rtl, roundTrip(rtl));
    }

    @Test
    void nodeCodecStillRoundTripsOrdinaryTermsExactly() throws IOException {
        Node lang = NodeFactory.createLiteralLang("hello", "en");
        Node typed = NodeFactory.createLiteralDT("42",
                TypeMapper.getInstance().getSafeTypeByName("http://www.w3.org/2001/XMLSchema#integer"));
        Node uri = NodeFactory.createURI("http://ex.org/a");
        Node bnode = NodeFactory.createBlankNode("b1");
        assertEquals(lang, roundTrip(lang));
        assertEquals(typed, roundTrip(typed));
        assertEquals(uri, roundTrip(uri));
        assertEquals(bnode, roundTrip(bnode));
    }

    @Test
    void streamingDictionaryWriterEncodesBaseDirectionLiterals() throws Exception {
        Node ltr = NodeFactory.createLiteralDirLang("hello", "en", TextDirection.LTR);
        Node plain = NodeFactory.createLiteralLang("hello", "en");
        // NodeComparator order within the language block: (lang, lex, direction),
        // absent < ltr - so the plain term encodes first.
        try (StreamingDictionaryWriter w = new StreamingDictionaryWriter(
                dir.resolve("dirs"), "literals", 2, stringStats(),
                Set.of(Types.STRING, Types.INTEGER, Types.LONG, Types.FLOAT, Types.DOUBLE),
                new TreeSet<>(Set.of(RDF.dirLangString.getURI(), RDF.langString.getURI())),
                new TreeSet<>(Set.of("en")), true)) {
            w.encode(List.of(plain, ltr).iterator());
        }
    }
}
