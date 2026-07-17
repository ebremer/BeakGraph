package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.TextDirection;
import org.apache.jena.query.Dataset;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * RDF 1.2 base-direction literals (rdf:dirLangString) round-trip term-exactly
 * through every writer engine (format v4: the langDirs column beside
 * langs/langTags). This REPLACES the Phase 0 containment test
 * (RDF12ContainmentTest), per that test's stated lifecycle: rejection guards
 * out, term-exact round-trip in.
 *
 * <p>The fixture is the exact trio from the original corruption finding -
 * three distinct terms sharing one lexical form, which versions ≤ 0.17.0
 * silently collapsed onto plain "hello"@en (3 terms in, 1 term out).
 */
class DirLangRoundTripTest {

    private static final String TTL = """
        @prefix : <http://ex.org/> .
        :s :p "hello"@en--ltr .
        :s :q "hello"@en--rtl .
        :s :r "hello"@en .
        :s :t "shalom"@he--rtl .
        """;

    @TempDir
    static Path dir;

    static Stream<WriterEngines.Engine> engines() {
        return WriterEngines.all();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("engines")
    void baseDirectionLiteralsRoundTripTermExactly(WriterEngines.Engine engine) throws Exception {
        engine.assumeAvailable();
        Path src = dir.resolve(engine.name() + ".ttl");
        Files.writeString(src, TTL);
        File dest = dir.resolve(engine.name() + ".h5").toFile();
        engine.buildStore(src.toFile(), dest);

        try (BeakGraph bg = BG.getBeakGraph(dest)) {
            Dataset ds = bg.getDataset();

            Set<String> stored = new TreeSet<>();
            ds.getDefaultModel().listStatements().forEachRemaining(st -> {
                Node n = st.getObject().asNode();
                stored.add(n.getLiteralLexicalForm() + "@" + n.getLiteralLanguage()
                        + "--" + n.getLiteralBaseDirection());
            });
            assertEquals(new TreeSet<>(Set.of(
                    "hello@en--ltr",
                    "hello@en--rtl",
                    "hello@en--null",
                    "shalom@he--rtl")), stored,
                    "the three same-lexical-form terms must stay distinct, with directions intact");

            // Term lookup, not just enumeration: locate() must resolve the exact
            // dirLangString term through the dictionary binary search.
            Node ltr = NodeFactory.createLiteralDirLang("hello", "en", TextDirection.LTR);
            Node rtl = NodeFactory.createLiteralDirLang("hello", "en", TextDirection.RTL);
            Node plain = NodeFactory.createLiteralLang("hello", "en");
            var m = ds.getDefaultModel();
            var g = m.getGraph();
            assertTrue(g.contains(Node.ANY,
                    NodeFactory.createURI("http://ex.org/p"), ltr), "locate(hello@en--ltr)");
            assertTrue(g.contains(Node.ANY,
                    NodeFactory.createURI("http://ex.org/q"), rtl), "locate(hello@en--rtl)");
            assertTrue(g.contains(Node.ANY,
                    NodeFactory.createURI("http://ex.org/r"), plain), "locate(hello@en)");
            // And the cross-checks: the WRONG variant must not match.
            assertTrue(!g.contains(Node.ANY,
                    NodeFactory.createURI("http://ex.org/p"), rtl), "p must not match the rtl term");
            assertTrue(!g.contains(Node.ANY,
                    NodeFactory.createURI("http://ex.org/r"), ltr), "r must not match the ltr term");
        }
    }
}
