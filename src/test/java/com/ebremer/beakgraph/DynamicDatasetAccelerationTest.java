package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BGDatasetGraph;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.QueryEngineBG;
import com.ebremer.beakgraph.hdf5.jena.AggregateCountFastPath;
import com.ebremer.beakgraph.hdf5.jena.DistinctTermFastPath;
import com.ebremer.beakgraph.hdf5.jena.PatternMatchBG;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.sparql.core.DatasetDescription;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.DynamicDatasets;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.graph.GraphZero;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * BG-336: a dataset clause ({@code FROM} / {@code FROM NAMED}, or the SPARQL
 * protocol's {@code default-graph-uri}) used to hand the query to Jena's
 * generic engine over a {@code GraphUnionRead}: correct, but no id-level
 * joins, no pushdown, no fast paths, and a HashSet of every materialised
 * triple for de-duplication. {@link QueryEngineBG} now builds the dynamic
 * dataset from BeakGraph views, so the accelerated paths fire - pinned here
 * by the hit counters - while the answers stay Jena's.
 */
class DynamicDatasetAccelerationTest {

    private static final String PREFIX = "PREFIX ex: <http://ex.org/> ";
    private static final String G1 = "http://ex.org/g1";
    private static final String G2 = "http://ex.org/g2";
    private static final String TRIG = """
        @prefix ex: <http://ex.org/> .
        ex:s ex:p ex:o .
        ex:s ex:shared "both" .
        ex:g1 {
            ex:s ex:shared "both" .
            ex:s ex:only "g1" .
            ex:s ex:p ex:o1 .
            ex:t ex:p ex:o1 .
        }
        ex:g2 {
            ex:s ex:shared "both" .
            ex:s ex:only "g2" .
            ex:u ex:q ex:o2 .
        }
        """;

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;
    static Dataset reference;

    @BeforeAll
    static void build() throws Exception {
        Path src = dir.resolve("dyn.trig");
        Files.writeString(src, TRIG);
        File h5 = dir.resolve("dyn.h5").toFile();
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

    private static List<String> rows(Dataset d, Query query) {
        List<String> out = new ArrayList<>();
        try (QueryExecution qe = QueryExecution.dataset(d).query(query).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                QuerySolution qs = rs.next();
                StringBuilder sb = new StringBuilder();
                for (String v : rs.getResultVars()) sb.append(v).append('=').append(qs.get(v)).append(' ');
                out.add(sb.toString());
            }
        }
        Collections.sort(out);
        return out;
    }

    private static List<String> rows(Dataset d, String query) {
        return rows(d, QueryFactory.create(PREFIX + query));
    }

    /** Runs {@code query} on the store, asserting parity with Jena and returning the growth of {@code counter}. */
    private static long hits(java.util.concurrent.atomic.AtomicLong counter, String query) {
        long before = counter.get();
        assertEquals(rows(reference, query), rows(ds, query), query);
        return counter.get() - before;
    }

    @Test
    void singleFromKeepsTheIdLevelEngine() {
        assertTrue(hits(PatternMatchBG.HITS, "SELECT ?o ?o2 FROM <" + G1 + "> WHERE { ?s ex:only ?o . ?s ex:shared ?o2 }") > 0,
                "a 2-pattern BGP under FROM <g> must run on PatternMatchBG");
        assertTrue(hits(AggregateCountFastPath.HITS, "SELECT (COUNT(*) AS ?n) FROM <" + G1 + "> WHERE { ?s ?p ?o }") > 0,
                "COUNT(*) under FROM <g> must be answered from the index");
        assertTrue(hits(DistinctTermFastPath.HITS, "SELECT DISTINCT ?p FROM <" + G1 + "> WHERE { ?s ?p ?o }") > 0,
                "DISTINCT ?p under FROM <g> must stream the GPOS predicate level");
        assertEquals(List.of("n=\"4\"^^xsd:integer "), rows(ds, "SELECT (COUNT(*) AS ?n) FROM <" + G1 + "> WHERE { ?s ?p ?o }"));
        assertTrue(hits(PatternMatchBG.HITS, "SELECT ?s ?p ?o FROM <urn:x-arq:DefaultGraph> WHERE { ?s ?p ?o }") > 0);
        assertTrue(hits(PatternMatchBG.HITS, "SELECT ?s ?p ?o FROM <urn:x-arq:UnionGraph> WHERE { ?s ?p ?o }") > 0);
        assertTrue(hits(PatternMatchBG.HITS, "SELECT ?s ?p ?o FROM <http://ex.org/absent> WHERE { ?s ?p ?o }") > 0);
    }

    @Test
    void severalFromGraphsAreASetUnionOnTheIdLevelEngine() {
        String q = "SELECT ?s ?p ?o FROM <" + G1 + "> FROM <" + G2 + "> WHERE { ?s ?p ?o }";
        assertTrue(hits(PatternMatchBG.HITS, q) > 0, "FROM <g1> FROM <g2> must run on a graph-set view, not GraphUnionRead");
        assertEquals(List.of("n=\"6\"^^xsd:integer "),
                rows(ds, "SELECT (COUNT(*) AS ?n) FROM <" + G1 + "> FROM <" + G2 + "> WHERE { ?s ?p ?o }"),
                "the shared triple counts once (4 + 3 - 1)");
        assertTrue(hits(PatternMatchBG.HITS, "SELECT ?s ?o FROM <" + G1 + "> FROM <" + G2 + "> WHERE { ?s ex:only ?o . ?s ex:shared ?b }") > 0);
        assertTrue(hits(PatternMatchBG.HITS, "SELECT ?s ?p ?o FROM <urn:x-arq:DefaultGraph> FROM <" + G2 + "> WHERE { ?s ?p ?o }") > 0);
        assertEquals(List.of("n=\"4\"^^xsd:integer "),
                rows(ds, "SELECT (COUNT(*) AS ?n) FROM <urn:x-arq:DefaultGraph> FROM <" + G2 + "> WHERE { ?s ?p ?o }"),
                "default (2) + g2 (3) - shared (1)");
        // No single-graph fast path may answer a set view.
        long distinct = DistinctTermFastPath.HITS.get();
        long count = AggregateCountFastPath.HITS.get();
        assertEquals(rows(reference, "SELECT DISTINCT ?p FROM <" + G1 + "> FROM <" + G2 + "> WHERE { ?s ?p ?o }"),
                rows(ds, "SELECT DISTINCT ?p FROM <" + G1 + "> FROM <" + G2 + "> WHERE { ?s ?p ?o }"));
        assertEquals(distinct, DistinctTermFastPath.HITS.get(), "DISTINCT ?p over a graph set is not a single index level");
        assertEquals(count, AggregateCountFastPath.HITS.get());
    }

    @Test
    void fromNamedGraphsStayAccelerated() {
        assertTrue(hits(PatternMatchBG.HITS, "SELECT ?g ?s ?o FROM NAMED <" + G1 + "> FROM NAMED <" + G2 + "> WHERE { GRAPH ?g { ?s ex:only ?o . ?s ex:shared ?b } }") > 0);
        assertEquals(List.of(), rows(ds, "SELECT ?s ?p ?o FROM NAMED <" + G1 + "> WHERE { ?s ?p ?o }"),
                "FROM NAMED only: the default graph is empty");
        assertEquals(List.of(), rows(ds, "SELECT ?s ?p ?o FROM NAMED <http://ex.org/absent> WHERE { GRAPH ?g { ?s ?p ?o } }"));
        assertTrue(hits(PatternMatchBG.HITS, "SELECT ?s ?p ?o FROM <" + G2 + "> FROM NAMED <" + G1 + "> WHERE { ?s ?p ?o . GRAPH ?g { ?s ?p ?o } }") > 0);
    }

    @Test
    void protocolDatasetTakesTheSamePath() {
        // The SPARQL protocol's default-graph-uri lands on Query.addGraphURI - the
        // same DatasetDescription the FROM clause builds.
        Query q = QueryFactory.create(PREFIX + "SELECT ?s ?o WHERE { ?s ex:only ?o . ?s ex:shared ?b }");
        q.addGraphURI(G1);
        q.addNamedGraphURI(G2);
        long before = PatternMatchBG.HITS.get();
        assertEquals(rows(reference, q), rows(ds, q));
        assertEquals(List.of("s=http://ex.org/s o=g1 "), rows(ds, q));
        assertTrue(PatternMatchBG.HITS.get() > before);
    }

    @Test
    void theDynamicDatasetIsBuiltFromBeakGraphViews() {
        BGDatasetGraph dsg = (BGDatasetGraph) ds.asDatasetGraph();
        DatasetDescription one = DatasetDescription.create(List.of(G1), List.of(G2));
        DatasetGraph dyn = QueryEngineBG.dynamicDataset(dsg, one, false);
        assertTrue(dyn instanceof DynamicDatasets.DynamicDatasetGraph, "the marker type Jena's engine and Fuseki inspect");
        Graph dft = dyn.getDefaultGraph();
        assertTrue(dft instanceof BeakGraph, "single FROM: the graph's own view");
        assertEquals(NodeFactory.createURI(G1), ((BeakGraph) dft).getNamedGraph());
        assertFalse(((BeakGraph) dft).isGraphSetView());
        assertTrue(dyn.getGraph(NodeFactory.createURI(G2)) instanceof BeakGraph, "FROM NAMED: a named view");
        assertFalse(dyn.containsGraph(NodeFactory.createURI(G1)), "a FROM graph is not a named graph of the dynamic dataset");
        assertEquals(List.of(NodeFactory.createURI(G2)), iterate(dyn.listGraphNodes()));

        DatasetGraph two = QueryEngineBG.dynamicDataset(dsg, DatasetDescription.create(List.of(G1, G2), List.of()), false);
        BeakGraph set = (BeakGraph) two.getDefaultGraph();
        assertTrue(set.isGraphSetView());
        assertEquals(BeakGraph.GRAPH_SET, set.getNamedGraph());
        assertEquals(2, set.getMemberGraphs().size());
        assertEquals(6, set.size(), "Graph.size() of the set view counts the de-duplicated union");
        assertEquals(1, set.find(Node.ANY, NodeFactory.createURI("http://ex.org/shared"), Node.ANY).toList().size());

        DatasetGraph none = QueryEngineBG.dynamicDataset(dsg, DatasetDescription.create(List.of(), List.of(G1)), false);
        assertTrue(none.getDefaultGraph() instanceof GraphZero, "FROM NAMED only: an empty default graph");

        DatasetGraph union = QueryEngineBG.dynamicDataset(dsg, DatasetDescription.create(List.of(Quad.unionGraph.getURI()), List.of()), false);
        assertEquals(Quad.unionGraph, ((BeakGraph) union.getDefaultGraph()).getNamedGraph());

        DatasetGraph unionPlusDefault = QueryEngineBG.dynamicDataset(dsg,
                DatasetDescription.create(List.of(Quad.unionGraph.getURI(), Quad.defaultGraphIRI.getURI()), List.of()), false);
        BeakGraph all = (BeakGraph) unionPlusDefault.getDefaultGraph();
        assertTrue(all.isGraphSetView());
        assertEquals(7, all.size(), "default (2) + g1 (4) + g2 (3) - the shared triple present in all three (2)");
    }

    @Test
    void modelPathWithFromFallsBackToJenaAndStillAgrees() {
        // A Model over the default graph is a DatasetGraphOne, not a BGDatasetGraph:
        // FROM there is meaningless (no named graphs) and takes Jena's construction.
        String q = PREFIX + "SELECT ?s ?p ?o FROM <" + G1 + "> WHERE { ?s ?p ?o }";
        try (QueryExecution a = QueryExecutionFactory.create(QueryFactory.create(q), ds.getDefaultModel());
             QueryExecution b = QueryExecutionFactory.create(QueryFactory.create(q), reference.getDefaultModel())) {
            List<String> x = new ArrayList<>();
            List<String> y = new ArrayList<>();
            a.execSelect().forEachRemaining(s -> x.add(s.toString()));
            b.execSelect().forEachRemaining(s -> y.add(s.toString()));
            Collections.sort(x);
            Collections.sort(y);
            assertEquals(y, x);
        }
    }

    private static <T> List<T> iterate(java.util.Iterator<T> it) {
        List<T> out = new ArrayList<>();
        it.forEachRemaining(out::add);
        return out;
    }
}
