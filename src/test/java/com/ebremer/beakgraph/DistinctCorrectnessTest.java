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
import org.apache.jena.rdf.model.RDFNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression tests for C2: SELECT DISTINCT over a single all-variable pattern
 * previously streamed the file-global dictionary id lists instead of executing
 * the query. Those lists span every named graph - including the always-written
 * VoID metadata graph - so default-graph queries over-reported terms that have
 * no default-graph triple, any FILTER around the pattern was silently dropped,
 * and DISTINCT ?g included the default graph. DISTINCT must answer exactly
 * what a normal execution answers.
 */
class DistinctCorrectnessTest {

    private static final String TTL = """
        @prefix ex: <http://ex.org/> .
        ex:s1 ex:p ex:o1 .
        ex:s2 ex:q ex:o2 .
        ex:s1 ex:q "lit" .
        """;

    private static final String PREFIX = "PREFIX ex: <http://ex.org/> ";

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;

    @BeforeAll
    static void buildAndOpen() throws Exception {
        File ttl = dir.resolve("distinct.ttl").toFile();
        File h5 = dir.resolve("distinct.ttl.h5").toFile();
        Files.write(ttl.toPath(), TTL.getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setVoidMode(com.ebremer.beakgraph.core.VoidMode.EXACT)
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

    /** URIs as-is; literals as their lexical form. */
    private static Set<String> select(String var, String query) {
        Set<String> values = new HashSet<>();
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create(PREFIX + query)).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                RDFNode n = rs.next().get(var);
                if (n == null) continue;
                values.add(n.isLiteral() ? n.asLiteral().getLexicalForm() : n.asResource().getURI());
            }
        }
        return values;
    }

    @Test
    void distinctPredicatesAreDefaultGraphOnly() {
        // The VoID metadata graph contributes rdf:type, void:*, sd:* predicates -
        // none of which occur in any default-graph triple.
        assertEquals(Set.of("http://ex.org/p", "http://ex.org/q"),
            select("p", "SELECT DISTINCT ?p WHERE { ?s ?p ?o }"));
    }

    @Test
    void distinctSubjectsAreDefaultGraphOnly() {
        assertEquals(Set.of("http://ex.org/s1", "http://ex.org/s2"),
            select("s", "SELECT DISTINCT ?s WHERE { ?s ?p ?o }"));
    }

    @Test
    void distinctObjectsAreDefaultGraphOnly() {
        assertEquals(Set.of("http://ex.org/o1", "http://ex.org/o2", "lit"),
            select("o", "SELECT DISTINCT ?o WHERE { ?s ?p ?o }"));
    }

    @Test
    void distinctRespectsFilters() {
        assertEquals(Set.of("http://ex.org/q"),
            select("p", "SELECT DISTINCT ?p WHERE { ?s ?p ?o FILTER(?p != ex:p) }"));
        assertEquals(Set.of("http://ex.org/s1"),
            select("s", "SELECT DISTINCT ?s WHERE { ?s ?p ?o FILTER(?p = ex:p) }"));
    }

    @Test
    void distinctGraphsExcludeDefaultGraph() {
        // SPARQL: GRAPH ?g ranges over named graphs only.
        assertEquals(Set.of(Params.VOIDSTRING),
            select("g", "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } }"));
    }
}
