package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.fuseki.RelativeIRIResolver;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * BG-394: a document-relative DATATYPE IRIREF ({@code "7"^^<scoreType>}) is
 * resolved against the sentinel base by the parser like any IRI, but only
 * IRI terms were relativized back, so the sentinel
 * {@code http://beakgraph.invalid/...} leaked into the typedLiterals
 * dictionary and every result. Datatypes are now stored relative and
 * resolved / matched at query time like IRI terms.
 */
class RelativeDatatypeIriTest {

    private static final String TTL = """
        @prefix ex: <http://ex.org/> .
        <> ex:score "7"^^<scoreType> .
        <> ex:other "8"^^<sub/otherType> .
        <> ex:plain "9" .
        """;
    private static final String BASE = "http://host/dir/doc.h5";

    @TempDir
    static Path dir;

    static Stream<WriterEngines.Engine> engines() {
        return WriterEngines.all();
    }

    private static Node literal(String lex, String datatype) {
        return NodeFactory.createLiteralDT(lex, TypeMapper.getInstance().getSafeTypeByName(datatype));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("engines")
    void relativeDatatypesAreStoredRelativeAndServedResolved(WriterEngines.Engine engine) throws Exception {
        engine.assumeAvailable();
        Path ttl = dir.resolve(engine.name() + ".ttl");
        Files.writeString(ttl, TTL);
        File h5 = dir.resolve(engine.name() + ".h5").toFile();
        engine.buildStore(ttl.toFile(), h5);
        try (BeakGraph bg = BG.getBeakGraph(h5)) {
            // No sentinel anywhere in the store.
            try (QueryExecution qe = QueryExecution.dataset(bg.getDataset())
                    .query(QueryFactory.create("SELECT ?s ?p ?o WHERE { ?s ?p ?o }")).build()) {
                ResultSet rs = qe.execSelect();
                int rows = 0;
                while (rs.hasNext()) {
                    var row = rs.next();
                    for (String v : new String[]{"s", "p", "o"}) {
                        Node n = row.get(v).asNode();
                        String text = n.isLiteral() ? n.getLiteralLexicalForm() + "^^" + n.getLiteralDatatypeURI() : n.toString();
                        assertFalse(text.contains("beakgraph.invalid"), "sentinel leaked: " + text);
                    }
                    rows++;
                }
                assertEquals(3, rows);
            }
            // Stored form: the datatype exactly as the document spelled it.
            assertEquals("scoreType", datatypeOf(bg, "http://ex.org/score", null));
            assertEquals("sub/otherType", datatypeOf(bg, "http://ex.org/other", null));
            // Served form: resolved against the URL the store is served from.
            RelativeIRIResolver resolver = new RelativeIRIResolver(BASE);
            assertEquals("http://host/dir/scoreType", datatypeOf(bg, "http://ex.org/score", resolver));
            assertEquals("http://host/dir/sub/otherType", datatypeOf(bg, "http://ex.org/other", resolver));
            // Query input: the absolute datatype maps back onto the stored relative one.
            Predicate<Node> stored = n -> bg.find(Node.ANY, Node.ANY, n).hasNext();
            Node absolute = literal("7", "http://host/dir/scoreType");
            assertFalse(stored.test(absolute), "the premise: only the relative form is in the dictionary");
            Node mapped = resolver.absoluteToStorage(stored).apply(absolute);
            assertEquals(literal("7", "scoreType"), mapped);
            assertTrue(stored.test(mapped));
            // ... and a literal typed with an unrelated absolute datatype is left alone.
            Node foreign = literal("7", "http://elsewhere.org/scoreType");
            assertEquals(foreign, resolver.absoluteToStorage(stored).apply(foreign));
        }
    }

    private static String datatypeOf(BeakGraph bg, String predicate, RelativeIRIResolver resolver) {
        try (QueryExecution qe = QueryExecution.dataset(bg.getDataset())
                .query(QueryFactory.create("SELECT ?o WHERE { ?s <" + predicate + "> ?o }")).build()) {
            ResultSet rs = resolver == null ? qe.execSelect() : resolver.resolve(qe.execSelect());
            assertTrue(rs.hasNext());
            return rs.next().get("o").asNode().getLiteralDatatypeURI();
        }
    }
}
