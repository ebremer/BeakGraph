package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.vocabulary.XSD;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression tests for writer robustness against ill-typed literals: RDF
 * permits literals whose lexical form is not valid for their datatype
 * ("abc"^^xsd:int), and Jena's parsers accept them with a warning. The writer
 * used to call getLiteralValue() unconditionally and abort the entire build on
 * the resulting DatatypeFormatException. Such terms must instead be stored
 * term-exact via the lexical strings path.
 */
class MalformedLiteralRoundTripTest {

    private static final String TTL = """
        @prefix ex:  <http://ex.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        ex:a ex:v "abc"^^xsd:int .
        ex:b ex:v "not-a-date"^^xsd:date .
        ex:c ex:v 5 .
        ex:d ex:v "5"^^xsd:int .
        ex:e ex:v "9z9"^^xsd:long .
        """;

    private static final String PREFIX =
        "PREFIX ex: <http://ex.org/> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ";

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;

    @BeforeAll
    static void buildAndOpen() throws Exception {
        File ttl = dir.resolve("malformed.ttl").toFile();
        File h5 = dir.resolve("malformed.ttl.h5").toFile();
        Files.write(ttl.toPath(), TTL.getBytes(StandardCharsets.UTF_8));
        // Pre-fix this aborted with an IOException wrapping DatatypeFormatException.
        HDF5Writer.Builder()
                .setSource(ttl).setDestination(h5)
                .setSpatial(false).setFeatures(false)
                .build().write();
        bg = new BeakGraph(new HDF5Reader(h5));
        ds = bg.getDataset();
    }

    @AfterAll
    static void closeReader() {
        if (bg != null) bg.close();
    }

    private static Set<String> subjects(String objectTerm) {
        Set<String> uris = new HashSet<>();
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create(PREFIX + "SELECT ?s WHERE { ?s ex:v " + objectTerm + " }")).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                RDFNode n = rs.next().get("s");
                if (n != null && n.isURIResource()) uris.add(n.asResource().getURI());
            }
        }
        return uris;
    }

    private static Literal value(String subject) {
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create(PREFIX + "SELECT ?v WHERE { " + subject + " ex:v ?v }")).build()) {
            return qe.execSelect().next().getLiteral("v");
        }
    }

    @Test
    void illTypedLiteralsRoundTripTermExact() {
        Literal a = value("ex:a");
        assertEquals("abc", a.getLexicalForm());
        assertEquals(XSD.xint.getURI(), a.getDatatypeURI());

        Literal b = value("ex:b");
        assertEquals("not-a-date", b.getLexicalForm());
        assertEquals(XSD.date.getURI(), b.getDatatypeURI());

        Literal e = value("ex:e");
        assertEquals("9z9", e.getLexicalForm());
        assertEquals(XSD.xlong.getURI(), e.getDatatypeURI());
    }

    @Test
    void illTypedLiteralsAreFindableByTerm() {
        assertEquals(Set.of("http://ex.org/a"), subjects("\"abc\"^^xsd:int"));
        assertEquals(Set.of("http://ex.org/b"), subjects("\"not-a-date\"^^xsd:date"));
    }

    @Test
    void wellFormedNeighborsAreUnaffected() {
        assertEquals(Set.of("http://ex.org/c"), subjects("5"));
        assertEquals(Set.of("http://ex.org/d"), subjects("\"5\"^^xsd:int"));
        Literal d = value("ex:d");
        assertEquals("5", d.getLexicalForm());
        assertEquals(XSD.xint.getURI(), d.getDatatypeURI());
    }
}
