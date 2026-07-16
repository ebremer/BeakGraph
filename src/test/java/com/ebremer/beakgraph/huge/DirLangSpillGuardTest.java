package com.ebremer.beakgraph.huge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.apache.jena.vocabulary.RDF;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The disk pipeline's guards against RDF 1.2 base-direction literals
 * (rdf:dirLangString), driven directly - the ingest guards normally fire
 * first, so these layers are unreachable end-to-end by design:
 * <ul>
 *   <li>{@link NodeCodec}: before its guard, "x"@en--ltr serialized as
 *       {@code T_LITERAL_LANG (lex, lang)} and deserialized as "x"@en - two
 *       distinct terms collapsed inside the external sort, falsifying the
 *       codec's "term identity is preserved" contract.</li>
 *   <li>{@link StreamingDictionaryWriter}: same collapse at encode time
 *       (mirror of MultiTypeDictionaryWriter's guard).</li>
 * </ul>
 */
class DirLangSpillGuardTest {

    private static final String GUARD_MESSAGE = "base direction cannot be stored";

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
        s.longestStringLength = 5;
        s.shortestStringLength = 5;
        return s;
    }

    @Test
    void nodeCodecRejectsBaseDirectionLiteral() {
        Node bad = NodeFactory.createLiteralDirLang("hello", "en", "ltr");
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
            NodeCodec.writeNode(new DataOutputStream(new ByteArrayOutputStream()), bad));
        assertTrue(ex.getMessage().contains(GUARD_MESSAGE), ex.getMessage());
    }

    @Test
    void nodeCodecRoundTripsOrdinaryTermsExactly() throws IOException {
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
    void streamingDictionaryWriterRejectsBaseDirectionLiteral() {
        Node bad = NodeFactory.createLiteralDirLang("hello", "en", "ltr");
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> {
            try (StreamingDictionaryWriter w = new StreamingDictionaryWriter(
                    dir.resolve("reject"), "literals", 1, stringStats(),
                    Set.of(Types.STRING, Types.INTEGER, Types.LONG, Types.FLOAT, Types.DOUBLE),
                    new TreeSet<>(Set.of(RDF.dirLangString.getURI())),
                    new TreeSet<>(Set.of("en")))) {
                w.encode(List.of(bad).iterator());
            }
        });
        assertTrue(ex.getMessage().contains(GUARD_MESSAGE), ex.getMessage());
    }

    @Test
    void streamingDictionaryWriterEncodesPlainLangTag() throws Exception {
        Node good = NodeFactory.createLiteralLang("hello", "en");
        try (StreamingDictionaryWriter w = new StreamingDictionaryWriter(
                dir.resolve("control"), "literals", 1, stringStats(),
                Set.of(Types.STRING, Types.INTEGER, Types.LONG, Types.FLOAT, Types.DOUBLE),
                new TreeSet<>(Set.of(RDF.langString.getURI())),
                new TreeSet<>(Set.of("en")))) {
            w.encode(List.of(good).iterator());
        }
    }
}
