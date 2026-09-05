package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.apache.jena.graph.Node;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.util.IsoMatcher;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Term-fidelity harness: parse -> write -> read -> assert
 * DATASET ISOMORPHISM against the source, for every writer engine. Isomorphism
 * is the RDF-correct form of "term set equality": it catches a store that
 * silently rewrites terms (the dirLangString corruption built "successfully"
 * with the right triple COUNT and the wrong terms) while still allowing the
 * blank-node relabeling BeakGraph's rank-derived labels legitimately perform.
 *
 * <p>The fixture deliberately covers every term shape the format stores:
 * IRIs, blank nodes (shared across graphs - label bijection must be
 * dataset-wide), plain / lang-tagged / typed literals, every by-value numeric
 * datatype (canonical spellings only - non-canonical spellings are a
 * documented deviation), the term-exact strings path (unbounded integers,
 * decimals, dates, booleans), value-equal-but-term-distinct pairs, unicode
 * with surrogate pairs, a string past the zstd compression threshold,
 * composite (cdt:) literals, and base-direction literals (rdf:dirLangString,
 * format v4 - including the same-lexical-form ltr/rtl/absent trio). Named
 * graphs use IRI and blank-node names.
 *
 * <p><b>Phase 3 extends THIS fixture</b> (triple terms) - one place, six
 * engines covered, instead of touching five parity tests.
 */
class TermFidelityTest {

    private static final String FIXTURE = """
        @prefix :    <http://ex.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        @prefix cdt: <http://w3id.org/awslabs/neptune/SPARQL-CDTs/> .

        :s :p :o .
        :s :p _:shared .
        _:shared :label "shared bnode" .
        :s :str "" .
        :s :str "plain string" .
        :s :long "this string is deliberately longer than the sixty-four byte zstd compression threshold used by the FCD writer" .
        :s :uni "caf\\u00E9 \\u00FCn\\u00EFcode \\U0001F426" .
        :s :uniIri <http://ex.org/\\u540D\\u524D/\\u0100> .
        :s :uniIri <http://ex.org/\\u00FF> .
        _:b\u00E9 :label "non-Latin-1 blank node label" .
        _:b\u0100 :label "label beyond 0xFF" .
        :s :lang "hello"@en .
        :s :lang "hello"@en-GB .
        :s :lang "bonjour"@fr .
        :s :dl "hello"@en--ltr .
        :s :dl "hello"@en--rtl .
        :s :dl "shalom"@he--rtl .
        :s :int "42"^^xsd:int .
        :s :int "-7"^^xsd:int .
        :s :long2 "123456789012"^^xsd:long .
        :s :float "2.5"^^xsd:float .
        :s :double "-3.25"^^xsd:double .
        :s :integer "123456789012345678901234567890"^^xsd:integer .
        :s :decimal "3.14"^^xsd:decimal .
        :s :bool "true"^^xsd:boolean .
        :s :date "2026-07-16"^^xsd:date .
        :s :dt "2026-07-16T12:00:00Z"^^xsd:dateTime .
        :s :dt "2026-07-16T12:00:00"^^xsd:dateTime .
        :s :vi "1"^^xsd:int .
        :s :vi "1"^^xsd:integer .
        :s :list "[1, 2, 3]"^^cdt:List .
        :s :list "[9]"^^cdt:List .
        :s :list "[10]"^^cdt:List .
        :s :map "{\\"k\\": 5}"^^cdt:Map .
        :s :tt <<( :a2 :b2 :c2 )>> .
        :s :tt2 <<( :a2 :b2 <<( :nx :ny "nested" )>> )>> .
        :s :tt3 <<( _:shared :inTerm "bnode co-refers inside and out" )>> .
        :s :tt4 <<( :a2 :b2 "hello"@en--ltr )>> .
        :s :tt5 <<( :a2 :b2 "hello"@en--rtl )>> .
        :s :tt6 <<( :onlyInsideS :onlyInsideP :onlyInsideO )>> .
        :s :tt7 <<( :a2 :b2 "42"^^xsd:int )>> .

        :g1 {
            :a :b :c .
            _:shared :in "g1" .
            :a :val "1"^^xsd:int .
            :a :ttg <<( :a2 :b2 :c2 )>> .
        }

        _:gname {
            :x :y "graph named by a blank node" .
        }
        """;

    @TempDir
    static Path dir;

    static Stream<WriterEngines.Engine> engines() {
        return WriterEngines.all();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("engines")
    void datasetSurvivesRoundTripIsomorphically(WriterEngines.Engine engine) throws Exception {
        engine.assumeAvailable();
        Path src = dir.resolve(engine.name() + ".trig");
        Files.writeString(src, FIXTURE);
        File dest = dir.resolve(engine.name() + ".h5").toFile();
        engine.buildStore(src.toFile(), dest);

        DatasetGraph expected = normalize(parseFixture());
        try (BeakGraph bg = BG.getBeakGraph(dest)) {
            DatasetGraph actual = normalize(bg.getDataset().asDatasetGraph());
            assertTrue(IsoMatcher.isomorphic(expected, actual),
                    () -> "round-trip is not isomorphic to the source\n" + diff(expected, actual));
        }
    }

    private static DatasetGraph parseFixture() {
        DatasetGraph dsg = DatasetGraphFactory.create();
        RDFParser.create().fromString(FIXTURE).lang(Lang.TRIG).parse(dsg);
        return dsg;
    }

    /**
     * Copy with two normalizations so both sides are compared fairly: BeakGraph's
     * derived metadata graphs (urn:x-beakgraph:*) are excluded, and both default
     * graph sentinels map onto Quad.defaultGraphIRI.
     */
    private static DatasetGraph normalize(DatasetGraph in) {
        DatasetGraph out = DatasetGraphFactory.create();
        Iterator<Quad> it = in.find(Node.ANY, Node.ANY, Node.ANY, Node.ANY);
        while (it.hasNext()) {
            Quad q = it.next();
            Node g = q.getGraph();
            if (g != null && g.isURI() && g.getURI().startsWith("urn:x-beakgraph:")) {
                continue;
            }
            out.add(new Quad(q.isDefaultGraph() ? Quad.defaultGraphIRI : g,
                    q.getSubject(), q.getPredicate(), q.getObject()));
        }
        return out;
    }

    private static String diff(DatasetGraph expected, DatasetGraph actual) {
        TreeSet<String> exp = quadStrings(expected);
        TreeSet<String> act = quadStrings(actual);
        StringBuilder sb = new StringBuilder();
        sb.append("expected ").append(exp.size()).append(" quads, actual ").append(act.size()).append('\n');
        exp.stream().filter(s -> !act.contains(s)).limit(12)
                .forEach(s -> sb.append("  only in source: ").append(s).append('\n'));
        act.stream().filter(s -> !exp.contains(s)).limit(12)
                .forEach(s -> sb.append("  only in store : ").append(s).append('\n'));
        sb.append("(blank-node label differences are expected; term differences are the bug)");
        return sb.toString();
    }

    private static TreeSet<String> quadStrings(DatasetGraph dsg) {
        TreeSet<String> out = new TreeSet<>();
        Iterator<Quad> it = dsg.find(Node.ANY, Node.ANY, Node.ANY, Node.ANY);
        while (it.hasNext()) {
            out.add(it.next().toString());
        }
        return out;
    }
}
