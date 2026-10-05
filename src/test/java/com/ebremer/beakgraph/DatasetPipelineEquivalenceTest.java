package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * BG-344: every SPARQL test used to run through {@code QueryExecution.dataset(ds)}.
 * The Model path ({@code QueryExecutionFactory.create(q, model)} over the
 * default or a named model) and the dynamic-dataset path ({@code FROM} /
 * {@code FROM NAMED}, which Jena answers through a view built from
 * BGDatasetGraph.getGraph / containsGraph) were unverified. Both are pinned
 * here against the dataset path AND against Jena's in-memory dataset loaded
 * from the same TriG, over a store with a default graph and two named graphs
 * that share triples, an rdfs:member triple, a triple-term object and a
 * wktLiteral.
 */
class DatasetPipelineEquivalenceTest {

    private static final String PREFIX = "PREFIX ex: <http://ex.org/> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
            + "PREFIX geo: <http://www.opengis.net/ont/geosparql#> PREFIX geof: <http://www.opengis.net/def/function/geosparql/> ";
    private static final String TRIG = """
        @prefix ex: <http://ex.org/> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix geo: <http://www.opengis.net/ont/geosparql#> .
        ex:bag rdfs:member ex:item .
        ex:s ex:p ex:o .
        ex:s ex:shared "both" .
        ex:s ex:says <<( ex:a ex:b ex:c )>> .
        ex:s geo:asWKT "POLYGON((0 0,10 0,10 10,0 10,0 0))"^^geo:wktLiteral .
        ex:g1 {
            ex:s ex:shared "both" .
            ex:s ex:only "g1" .
            ex:t ex:says <<( ex:a ex:b ex:c )>> .
            ex:t geo:asWKT "POINT(5 5)"^^geo:wktLiteral .
        }
        ex:g2 {
            ex:s ex:shared "both" .
            ex:s ex:only "g2" .
            ex:bag rdfs:member ex:item2 .
        }
        """;

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;
    static Dataset reference;

    @BeforeAll
    static void build() throws Exception {
        Path src = dir.resolve("pipes.trig");
        Files.writeString(src, TRIG);
        File h5 = dir.resolve("pipes.h5").toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        bg = BG.getBeakGraph(h5);
        ds = bg.getDataset();
        DatasetGraph dsg = DatasetGraphFactory.create();
        RDFParser.create().source(src.toUri().toString()).lang(Lang.TRIG).parse(dsg);
        reference = DatasetFactory.wrap(dsg);
    }

    @AfterAll
    static void close() {
        if (bg != null) bg.close();
    }

    private static List<String> rows(ResultSet rs) {
        List<String> out = new ArrayList<>();
        while (rs.hasNext()) {
            QuerySolution qs = rs.next();
            StringBuilder sb = new StringBuilder();
            for (String v : rs.getResultVars()) sb.append(v).append('=').append(qs.get(v)).append(' ');
            out.add(sb.toString());
        }
        Collections.sort(out);
        return out;
    }

    private static List<String> viaDataset(Dataset d, String query) {
        try (QueryExecution qe = QueryExecution.dataset(d).query(QueryFactory.create(PREFIX + query)).build()) {
            return rows(qe.execSelect());
        }
    }

    private static List<String> viaModel(Model m, String query) {
        try (QueryExecution qe = QueryExecutionFactory.create(QueryFactory.create(PREFIX + query), m)) {
            return rows(qe.execSelect());
        }
    }

    private static boolean ask(Dataset d, String query) {
        try (QueryExecution qe = QueryExecution.dataset(d).query(QueryFactory.create(PREFIX + query)).build()) {
            return qe.execAsk();
        }
    }

    // --- (a) Model path == dataset path == Jena -----------------------------

    @Test
    void defaultModelAnswersLikeTheDatasetPath() {
        String[] queries = {
            "SELECT ?s ?p ?o WHERE { ?s ?p ?o }",
            "SELECT ?x WHERE { ex:bag rdfs:member ?x }",
            "SELECT ?c WHERE { ex:s ex:says <<( ex:a ex:b ?c )>> }",
            "SELECT ?o WHERE { ?s ex:shared ?o }",
            "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }",
            "SELECT DISTINCT ?p WHERE { ?s ?p ?o }",
            "SELECT ?s WHERE { ?s geo:asWKT ?w FILTER(geof:sfIntersects(?w, \"POINT(1 1)\"^^geo:wktLiteral)) }",
            "SELECT ?s WHERE { ?s geo:asWKT ?w FILTER(geof:sfIntersects(?w, \"POINT(50 50)\"^^geo:wktLiteral)) }",
        };
        for (String q : queries) {
            List<String> expected = viaDataset(reference, q);
            assertEquals(expected, viaDataset(ds, q), "dataset path: " + q);
            assertEquals(expected, viaModel(ds.getDefaultModel(), q), "default-model path: " + q);
        }
        assertFalse(viaDataset(ds, "SELECT ?x WHERE { ex:bag rdfs:member ?x }").isEmpty(), "rdfs:member is a stored predicate here");
        try (QueryExecution a = QueryExecution.dataset(ds).query(QueryFactory.create(PREFIX + "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }")).build();
             QueryExecution b = QueryExecutionFactory.create(QueryFactory.create(PREFIX + "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"), ds.getDefaultModel())) {
            Model viaDs = a.execConstruct();
            Model viaModel = b.execConstruct();
            assertTrue(viaDs.isIsomorphicWith(viaModel));
            assertTrue(viaDs.isIsomorphicWith(reference.getDefaultModel()));
        }
    }

    @Test
    void namedModelAnswersLikeGraphPatterns() {
        for (String g : new String[]{"http://ex.org/g1", "http://ex.org/g2"}) {
            for (String body : new String[]{"?s ?p ?o", "ex:s ex:only ?o", "?b rdfs:member ?m", "?t ex:says <<( ex:a ex:b ?c )>>"}) {
                String graphQuery = "SELECT * WHERE { GRAPH <" + g + "> { " + body + " } }";
                String plain = "SELECT * WHERE { " + body + " }";
                List<String> expected = viaDataset(reference, graphQuery);
                assertEquals(expected, viaDataset(ds, graphQuery), graphQuery);
                assertEquals(expected, viaModel(ds.getNamedModel(g), plain), "named model " + g + ": " + plain);
            }
            assertTrue(ds.getNamedModel(g).isIsomorphicWith(reference.getNamedModel(g)), g);
        }
    }

    // --- (b) FROM / FROM NAMED --------------------------------------------

    @Test
    void fromAndFromNamedMatchGraphPatternsAndJena() {
        String[] queries = {
            "SELECT ?s ?p ?o FROM <http://ex.org/g1> WHERE { ?s ?p ?o }",
            "SELECT ?s ?p ?o FROM NAMED <http://ex.org/g1> WHERE { GRAPH ?g { ?s ?p ?o } }",
            "SELECT ?g ?s ?p ?o FROM NAMED <http://ex.org/g1> FROM NAMED <http://ex.org/g2> WHERE { GRAPH ?g { ?s ?p ?o } }",
            "SELECT ?s ?p ?o FROM <http://ex.org/g1> FROM <http://ex.org/g2> WHERE { ?s ?p ?o }",
            "SELECT (COUNT(*) AS ?n) FROM <http://ex.org/g1> FROM <http://ex.org/g2> WHERE { ?s ?p ?o }",
            "SELECT ?s ?p ?o FROM <http://ex.org/absent> WHERE { ?s ?p ?o }",
            "SELECT ?s ?p ?o FROM NAMED <http://ex.org/absent> WHERE { GRAPH ?g { ?s ?p ?o } }",
            "SELECT ?s ?p ?o FROM <http://ex.org/g1> WHERE { GRAPH ?g { ?s ?p ?o } }",
            "SELECT ?s ?p ?o FROM <http://ex.org/g2> FROM NAMED <http://ex.org/g1> WHERE { ?s ?p ?o . GRAPH ?g { ?s ?p ?o } }",
            "SELECT ?x FROM <http://ex.org/g2> WHERE { ex:bag rdfs:member ?x }",
            "SELECT ?c FROM <http://ex.org/g1> WHERE { ?t ex:says <<( ex:a ex:b ?c )>> }",
            "SELECT ?s FROM <http://ex.org/g1> WHERE { ?s geo:asWKT ?w FILTER(geof:sfIntersects(?w, \"POINT(5 5)\"^^geo:wktLiteral)) }",
        };
        for (String q : queries) {
            assertEquals(viaDataset(reference, q), viaDataset(ds, q), q);
        }
        // Pinned facts, so the equivalences above are not vacuous.
        assertEquals(viaDataset(ds, "SELECT ?s ?p ?o WHERE { GRAPH <http://ex.org/g1> { ?s ?p ?o } }"),
                viaDataset(ds, "SELECT ?s ?p ?o FROM <http://ex.org/g1> WHERE { ?s ?p ?o }"), "GRAPH <g> == FROM <g>");
        assertEquals(viaDataset(ds, "SELECT ?s ?p ?o WHERE { GRAPH <http://ex.org/g1> { ?s ?p ?o } }"),
                viaDataset(ds, "SELECT ?s ?p ?o FROM NAMED <http://ex.org/g1> WHERE { GRAPH ?g { ?s ?p ?o } }"), "GRAPH <g> == FROM NAMED <g> + GRAPH ?g");
        assertEquals(List.of("n=\"6\"^^xsd:integer "),
                viaDataset(ds, "SELECT (COUNT(*) AS ?n) FROM <http://ex.org/g1> FROM <http://ex.org/g2> WHERE { ?s ?p ?o }"),
                "FROM <g1> FROM <g2> is a set union: the shared triple counts once (4 + 3 - 1)");
        assertEquals(List.of(), viaDataset(ds, "SELECT ?s ?p ?o FROM <http://ex.org/absent> WHERE { ?s ?p ?o }"), "an absent FROM graph is empty, not an error");
        assertEquals(List.of(), viaDataset(ds, "SELECT ?s ?p ?o FROM NAMED <http://ex.org/absent> WHERE { GRAPH ?g { ?s ?p ?o } }"));
        assertFalse(ask(ds, "ASK FROM <http://ex.org/g1> { ex:s ex:p ex:o }"), "the default graph is not part of a FROM dataset");
        assertTrue(ask(ds, "ASK FROM <http://ex.org/g1> { ex:s ex:only \"g1\" }"));
    }
}
