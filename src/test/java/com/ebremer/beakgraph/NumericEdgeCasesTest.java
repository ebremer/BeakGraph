package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ebremer.beakgraph.core.BeakGraph;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.RDFNode;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * BG-95: the numeric canonicalization edge cases had no coverage. An
 * xsd:int / xsd:long past its range is ill-typed and must be kept as the
 * exact original term (the strings route), never truncated onto its
 * wrap-around neighbour; INF, -INF, NaN and negative zero of xsd:float /
 * xsd:double must round-trip by value with their canonical lexical forms,
 * "-0" collapsing onto "-0.0" while "0.0" stays its own term. On every
 * engine.
 */
class NumericEdgeCasesTest {

    private static final String TTL = """
        @prefix ex:  <http://ex.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        ex:i1 ex:v "2147483648"^^xsd:int .
        ex:i2 ex:v "-2147483648"^^xsd:int .
        ex:l1 ex:v "9223372036854775808"^^xsd:long .
        ex:l2 ex:v "-9223372036854775808"^^xsd:long .
        ex:f1 ex:v "INF"^^xsd:float .
        ex:f2 ex:v "-INF"^^xsd:float .
        ex:f3 ex:v "NaN"^^xsd:float .
        ex:f4 ex:v "-0.0"^^xsd:float .
        ex:f5 ex:v "-0"^^xsd:float .
        ex:f6 ex:v "0.0"^^xsd:float .
        ex:d1 ex:v "INF"^^xsd:double .
        ex:d2 ex:v "-INF"^^xsd:double .
        ex:d3 ex:v "NaN"^^xsd:double .
        ex:d4 ex:v "-0.0"^^xsd:double .
        ex:d5 ex:v "-0"^^xsd:double .
        ex:d6 ex:v "0.0"^^xsd:double .
        """;
    private static final String PREFIX =
        "PREFIX ex: <http://ex.org/> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ";

    @TempDir
    static Path dir;

    static Stream<WriterEngines.Engine> engines() {
        return WriterEngines.all();
    }

    private static Set<String> subjects(Dataset ds, String objectTerm) {
        Set<String> uris = new HashSet<>();
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create(PREFIX + "SELECT ?s WHERE { ?s ex:v " + objectTerm + " }")).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                RDFNode n = rs.next().get("s");
                if (n != null && n.isURIResource()) uris.add(n.asResource().getURI().replace("http://ex.org/", ""));
            }
        }
        return uris;
    }

    private static String lexical(Dataset ds, String subject) {
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create(PREFIX + "SELECT ?v WHERE { ex:" + subject + " ex:v ?v }")).build()) {
            return qe.execSelect().next().getLiteral("v").getLexicalForm();
        }
    }

    private static Set<String> distinctLexicals(Dataset ds, String... subjects) {
        Set<String> out = new HashSet<>();
        for (String s : subjects) {
            out.add(lexical(ds, s));
        }
        return out;
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("engines")
    void outOfRangeAndSpecialValuesRoundTrip(WriterEngines.Engine engine) throws Exception {
        engine.assumeAvailable();
        File src = dir.resolve(engine.name() + ".ttl").toFile();
        Files.writeString(src.toPath(), TTL);
        File h5 = dir.resolve(engine.name() + ".h5").toFile();
        engine.buildStore(src, h5);
        try (BeakGraph bg = BG.getBeakGraph(h5)) {
            Dataset ds = bg.getDataset();

            // Out of range: ill-typed, kept as the exact term, never wrapped.
            assertEquals(Set.of("i1"), subjects(ds, "\"2147483648\"^^xsd:int"));
            assertEquals(Set.of("i2"), subjects(ds, "\"-2147483648\"^^xsd:int"));
            assertEquals(Set.of("l1"), subjects(ds, "\"9223372036854775808\"^^xsd:long"));
            assertEquals(Set.of("l2"), subjects(ds, "\"-9223372036854775808\"^^xsd:long"));
            assertEquals("2147483648", lexical(ds, "i1"));
            assertEquals("9223372036854775808", lexical(ds, "l1"));

            for (String dt : new String[]{"float", "double"}) {
                String p = dt.charAt(0) + "";
                assertEquals(Set.of(p + "1"), subjects(ds, "\"INF\"^^xsd:" + dt), dt + " INF");
                assertEquals(Set.of(p + "2"), subjects(ds, "\"-INF\"^^xsd:" + dt), dt + " -INF");
                assertEquals(Set.of(p + "3"), subjects(ds, "\"NaN\"^^xsd:" + dt), dt + " NaN");
                assertEquals(Set.of(p + "4", p + "5"), subjects(ds, "\"-0.0\"^^xsd:" + dt), dt + " -0.0 and -0 collapse");
                assertEquals(Set.of(p + "6"), subjects(ds, "\"0.0\"^^xsd:" + dt), dt + " 0.0 stays separate");
                assertEquals("INF", lexical(ds, p + "1"), dt);
                assertEquals("-INF", lexical(ds, p + "2"), dt);
                assertEquals("NaN", lexical(ds, p + "3"), dt);
                assertEquals("-0.0", lexical(ds, p + "5"), dt + ": -0 canonicalizes to -0.0");
                assertEquals(2, distinctLexicals(ds, p + "4", p + "5", p + "6").size(), dt + ": -0.0 and 0.0 are two terms");
            }
        }
    }
}
