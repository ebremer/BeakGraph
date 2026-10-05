package com.ebremer.beakgraph;

import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.lib.Stats;
import com.ebremer.beakgraph.hdf5.DictionarySection;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.hdf5.writers.MultiTypeDictionaryWriter;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.vocabulary.XSD;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression tests for the five critical correctness fixes:
 * <ul>
 *   <li>1.1 - SELECT DISTINCT ?s / ?o must return only nodes that actually occur
 *       as subject / object, not the whole dictionary.</li>
 *   <li>1.2 - literals with "exotic" datatypes (here xsd:decimal, in a dataset
 *       with no plain-string literals) must round-trip rather than being dropped
 *       and corrupting the dictionary.</li>
 *   <li>1.3 - xsd:integer must survive value-and-type intact (not truncated to a
 *       32-bit xsd:int).</li>
 *   <li>1.4 - Graph.size() must report the real triple count, not 0.</li>
 *   <li>1.5 - value-equal but term-distinct literals ("1"^^xsd:int vs
 *       "1"^^xsd:integer) must stay distinct dictionary entries.</li>
 * </ul>
 */
class CriticalFixesRoundTripTest {

    // Pure-object URIs (o1,o2) and pure-subject URIs (s1,s2) let us tell the subject
    // and object id sets apart (fix 1.1). The xsd:integer / xsd:int / xsd:decimal
    // objects exercise fixes 1.2/1.3/1.5. (In the full pipeline the generated VOID
    // metadata also contributes an xsd:string literal, so this dataset alone does not
    // exercise the 1.2 "no strings buffer" corruption path; that is covered directly by
    // writerRefusesToDropStringStoredLiteral below.)
    private static final String TTL = """
        @prefix ex:  <http://ex.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        ex:s1     ex:p     ex:o1 .
        ex:s2     ex:p     ex:o2 .
        ex:big    ex:value "123456789012345"^^xsd:integer .
        ex:small  ex:value "42"^^xsd:int .
        ex:dec    ex:value "3.14"^^xsd:decimal .
        ex:onea   ex:value "1"^^xsd:int .
        ex:oneb   ex:value "1"^^xsd:integer .
        """;

    private static final int DEFAULT_GRAPH_TRIPLES = 7;

    private static final String PREFIX =
        "PREFIX ex: <http://ex.org/> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ";

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;

    @BeforeAll
    static void buildAndOpen() throws Exception {
        File ttl = dir.resolve("critical.ttl").toFile();
        File h5 = dir.resolve("critical.ttl.h5").toFile();
        Files.write(ttl.toPath(), TTL.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder()
                .setSource(ttl).setDestination(h5)
                .setSpatial(false).setFeatures(false)
                .build().write();
        bg = new BeakGraph(new HDF5Reader(h5));
        ds = bg.getDataset();
    }

    @AfterAll
    static void closeReader() {
        // Release the file: a leaked reader makes @TempDir cleanup flaky on Windows (BG-176).
        if (bg != null) bg.close();
    }

    private static Set<String> resourceURIs(String var, String query) {
        Set<String> uris = new HashSet<>();
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create(PREFIX + query)).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                RDFNode n = rs.next().get(var);
                if (n != null && n.isURIResource()) {
                    uris.add(n.asResource().getURI());
                }
            }
        }
        return uris;
    }

    private static int count(String whereBody) {
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create(PREFIX + "SELECT * WHERE { " + whereBody + " }")).build()) {
            ResultSet rs = qe.execSelect();
            int n = 0;
            while (rs.hasNext()) { rs.next(); n++; }
            return n;
        }
    }

    private static Literal singleObject(String subject) {
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create(PREFIX + "SELECT ?v WHERE { " + subject + " ex:value ?v }")).build()) {
            ResultSet rs = qe.execSelect();
            assertTrue(rs.hasNext(), "expected a value for " + subject);
            Literal lit = rs.next().getLiteral("v");
            assertFalse(rs.hasNext(), "expected exactly one value for " + subject);
            return lit;
        }
    }

    // --- 1.1 -------------------------------------------------------------

    @Test
    void distinctSubjectsExcludePureObjects() {
        Set<String> subjects = resourceURIs("s", "SELECT DISTINCT ?s WHERE { ?s ?p ?o }");
        // Real subjects are present...
        assertTrue(subjects.contains("http://ex.org/s1"), "s1 should be a subject");
        assertTrue(subjects.contains("http://ex.org/s2"), "s2 should be a subject");
        // ...but URIs that only ever appear as objects must NOT be reported.
        assertFalse(subjects.contains("http://ex.org/o1"), "o1 is never a subject");
        assertFalse(subjects.contains("http://ex.org/o2"), "o2 is never a subject");
    }

    @Test
    void distinctObjectsExcludePureSubjects() {
        Set<String> objects = resourceURIs("o", "SELECT DISTINCT ?o WHERE { ?s ?p ?o }");
        assertTrue(objects.contains("http://ex.org/o1"), "o1 should be an object");
        assertTrue(objects.contains("http://ex.org/o2"), "o2 should be an object");
        assertFalse(objects.contains("http://ex.org/s1"), "s1 is never an object");
        assertFalse(objects.contains("http://ex.org/s2"), "s2 is never an object");
    }

    // --- 1.2 -------------------------------------------------------------

    @Test
    void exoticDatatypeLiteralRoundTrips() {
        Literal v = singleObject("ex:dec");
        assertEquals("3.14", v.getLexicalForm());
        assertEquals(XSD.decimal.getURI(), v.getDatatypeURI());
    }

    @Test
    void writerRefusesToDropStringStoredLiteral() {
        // Reproduce the 1.2 corruption trigger directly: a string-stored literal
        // (xsd:decimal) with stats that report no strings, so the strings buffer is
        // not allocated. The old writer silently logged "dropped" and skipped the
        // node, leaving offsets/datatypes one entry short (dictionary corruption).
        // The fixed writer must fail loudly instead.
        Stats stats = new Stats(); // numStrings == 0
        Node decimal = NodeFactory.createLiteralDT("3.14", XSDDatatype.XSDdecimal);
        MultiTypeDictionaryWriter.Builder builder = new MultiTypeDictionaryWriter.Builder()
                .setName("literals")
                .setNodes(Set.of(decimal))
                .setStats(stats)
                .setDataTypes(Set.of(XSD.decimal.getURI()))
                .section(DictionarySection.LITERALS);
        assertThrows(IllegalStateException.class, builder::build);
    }

    // --- 1.3 -------------------------------------------------------------

    @Test
    void largeIntegerNotTruncatedOrRetyped() {
        Literal v = singleObject("ex:big");
        // Old behaviour truncated to a 32-bit int and changed the datatype to xsd:int.
        assertEquals("123456789012345", v.getLexicalForm());
        assertEquals(XSD.integer.getURI(), v.getDatatypeURI());
    }

    @Test
    void boundedIntStaysInt() {
        Literal v = singleObject("ex:small");
        assertEquals("42", v.getLexicalForm());
        assertEquals(XSD.xint.getURI(), v.getDatatypeURI());
    }

    // --- 1.5 -------------------------------------------------------------

    @Test
    void valueEqualButTermDistinctLiteralsDoNotCollide() {
        // "1"^^xsd:int and "1"^^xsd:integer compare equal by value but are distinct
        // RDF terms; each must match only its own subject.
        assertEquals(1, count("?s ex:value \"1\"^^xsd:int"));
        assertEquals(1, count("?s ex:value \"1\"^^xsd:integer"));
        assertEquals(Set.of("http://ex.org/onea"),
                resourceURIs("s", "SELECT ?s WHERE { ?s ex:value \"1\"^^xsd:int }"));
        assertEquals(Set.of("http://ex.org/oneb"),
                resourceURIs("s", "SELECT ?s WHERE { ?s ex:value \"1\"^^xsd:integer }"));
    }

    // --- 1.4 -------------------------------------------------------------

    @Test
    void graphSizeReportsRealTripleCount() {
        assertEquals(DEFAULT_GRAPH_TRIPLES, ds.getDefaultModel().size());
        assertFalse(ds.getDefaultModel().isEmpty(), "non-empty graph must not report empty");
    }
}
