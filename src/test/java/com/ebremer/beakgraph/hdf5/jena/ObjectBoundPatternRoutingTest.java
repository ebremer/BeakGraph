package com.ebremer.beakgraph.hdf5.jena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.BG;
import com.ebremer.beakgraph.core.BeakGraph;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * BG-335: {@code ?s ?p <o>} used to be answered by a full GSPO scan of the
 * graph with an object clamp (there is no object-first index). It is now one
 * GPOS probe per predicate of the graph. Correctness against Jena for every
 * shape that reaches the object-first route - concrete and VALUES-bound
 * objects, literals, blank nodes, triple terms, named and union graphs,
 * property paths seeded from the object - and the routing itself pinned by
 * the iterator counters.
 */
class ObjectBoundPatternRoutingTest {

    private static final String PREFIX = "PREFIX ex: <http://ex.org/> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ";
    private static final String TRIG = """
        @prefix ex: <http://ex.org/> .
        ex:a ex:p ex:o1 ; ex:q ex:o1 ; ex:r ex:o2 ; ex:name "shared" ; ex:n 5 .
        ex:b ex:p ex:o1 ; ex:name "shared" ; ex:n 7 .
        ex:c ex:q ex:o2 ; ex:name "other" ; ex:says <<( ex:a ex:p ex:o1 )>> .
        ex:d ex:r ex:o1 ; ex:says <<( ex:a ex:p ex:o1 )>> .
        ex:o1 ex:p ex:o3 .
        ex:o3 ex:p ex:o4 .
        ex:x ex:knows _:k .
        ex:y ex:likes _:k .
        _:k ex:name "anon" .
        ex:self ex:self ex:self .
        ex:g1 {
            ex:a ex:p ex:o1 .
            ex:e ex:q ex:o1 .
            ex:e ex:p ex:o5 .
            ex:o5 ex:p ex:o1 .
        }
        ex:g2 {
            ex:f ex:r ex:o1 .
            ex:a ex:p ex:o1 .
        }
        """;

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;
    static Dataset reference;

    @BeforeAll
    static void build() throws Exception {
        Path src = dir.resolve("objfirst.trig");
        Files.writeString(src, TRIG);
        File h5 = dir.resolve("objfirst.h5").toFile();
        com.ebremer.beakgraph.hdf5.writers.HDF5Writer.Builder().setSource(src.toFile()).setDestination(h5)
                .setSpatial(false).setFeatures(false).build().write();
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

    private static List<String> rows(Dataset d, String query) {
        List<String> out = new ArrayList<>();
        try (QueryExecution qe = QueryExecution.dataset(d).query(QueryFactory.create(PREFIX + query)).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                QuerySolution qs = rs.next();
                StringBuilder sb = new StringBuilder();
                for (String v : rs.getResultVars()) {
                    var n = qs.get(v);
                    sb.append(v).append('=').append(n == null ? "" : n.isAnon() ? "_:bnode" : n.toString()).append(' ');
                }
                out.add(sb.toString());
            }
        }
        Collections.sort(out);
        return out;
    }

    private static void assertParity(String query) {
        List<String> expected = rows(reference, query);
        assertEquals(expected, rows(ds, query), query);
    }

    @Test
    void objectOnlyBoundPatternsMatchJena() {
        String[] queries = {
            "SELECT ?s ?p WHERE { ?s ?p ex:o1 }",
            "SELECT ?s ?p WHERE { ?s ?p ex:o2 }",
            "SELECT ?s ?p WHERE { ?s ?p \"shared\" }",
            "SELECT ?s ?p WHERE { ?s ?p 5 }",
            "SELECT ?s ?p WHERE { ?s ?p <<( ex:a ex:p ex:o1 )>> }",
            "SELECT ?s ?p WHERE { ?s ?p ex:absent }",
            "SELECT ?s ?p WHERE { ?s ?p ex:self }",
            "SELECT ?s WHERE { ?s ?s ex:self }",
            "SELECT ?s ?p ?t WHERE { ?t ex:knows ?k . ?s ?p ?k }",
            "SELECT ?s ?p ?o WHERE { VALUES ?o { ex:o1 ex:o2 ex:nowhere } ?s ?p ?o }",
            "SELECT ?s ?p ?o WHERE { VALUES ?o { \"shared\" \"anon\" } ?s ?p ?o }",
            "SELECT ?s ?p WHERE { GRAPH ex:g1 { ?s ?p ex:o1 } }",
            "SELECT ?g ?s ?p WHERE { GRAPH ?g { ?s ?p ex:o1 } }",
            "SELECT ?s ?p WHERE { GRAPH <urn:x-arq:UnionGraph> { ?s ?p ex:o1 } }",
            "SELECT ?s ?p WHERE { ?s ?p ex:o1 FILTER(?s != ex:a) }",
            "SELECT ?s ?p WHERE { ?s ?p ex:o1 FILTER(STR(?s) > \"http://ex.org/b\") }",
            "SELECT ?s ?p WHERE { ex:a ?p ex:o1 }",
            "SELECT ?s ?p ?o WHERE { ?s ex:says ?o . ?x ?p ?o }",
        };
        for (String q : queries) {
            assertParity(q);
        }
        assertEquals(List.of("s=http://ex.org/a p=http://ex.org/p ", "s=http://ex.org/a p=http://ex.org/q ",
                "s=http://ex.org/b p=http://ex.org/p ", "s=http://ex.org/d p=http://ex.org/r "),
                rows(ds, "SELECT ?s ?p WHERE { ?s ?p ex:o1 }"), "fixture sanity");
    }

    @Test
    void objectOnlyBoundPatternProbesInsteadOfScanning() {
        long objectFirst = BGIteratorObjectFirst.HITS.get();
        long scans = BGIteratorSPO_All.HITS.get();
        rows(ds, "SELECT ?s ?p WHERE { ?s ?p ex:o1 }");
        assertEquals(objectFirst + 1, BGIteratorObjectFirst.HITS.get(), "?s ?p <o> routes to the per-predicate GPOS probes");
        assertEquals(scans, BGIteratorSPO_All.HITS.get(), "?s ?p <o> must not construct a graph scan");

        // The same through Graph.find(ANY, ANY, o) - what PathLib issues.
        objectFirst = BGIteratorObjectFirst.HITS.get();
        scans = BGIteratorSPO_All.HITS.get();
        List<Triple> found = new ArrayList<>();
        ExtendedIterator<Triple> it = bg.find(Node.ANY, Node.ANY, NodeFactory.createURI("http://ex.org/o1"));
        try {
            it.forEachRemaining(found::add);
        } finally {
            it.close();
        }
        assertEquals(4, found.size());
        assertEquals(objectFirst + 1, BGIteratorObjectFirst.HITS.get());
        assertEquals(scans, BGIteratorSPO_All.HITS.get());

        // A subject sharing the predicate's variable name keeps the scan route
        // (the pre-bound predicate id must not be read as a subject).
        objectFirst = BGIteratorObjectFirst.HITS.get();
        rows(ds, "SELECT ?s WHERE { ?s ?s ex:self }");
        assertEquals(objectFirst, BGIteratorObjectFirst.HITS.get(), "?x ?x <o> is not eligible");

        // Unbound object, unbound predicate: still the scan.
        objectFirst = BGIteratorObjectFirst.HITS.get();
        scans = BGIteratorSPO_All.HITS.get();
        rows(ds, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }");
        assertEquals(objectFirst, BGIteratorObjectFirst.HITS.get());
        assertTrue(BGIteratorSPO_All.HITS.get() > scans);
    }

    @Test
    void propertyPathsSeededFromTheObjectMatchJena() {
        String[] queries = {
            "SELECT ?s WHERE { ?s ex:p+ ex:o4 }",
            "SELECT ?s WHERE { ?s ex:p* ex:o4 }",
            "SELECT ?s WHERE { ?s ex:p+ ex:o1 }",
            "SELECT ?s WHERE { ?s ^ex:p ex:a }",
            "SELECT ?s WHERE { ?s !(ex:q|ex:r) ex:o1 }",
            "SELECT ?s WHERE { ?s ^!(ex:q) ex:o1 }",
            "SELECT ?s WHERE { ?s (ex:p|ex:q)+ ex:o3 }",
            "SELECT ?s WHERE { ?s ex:p/ex:p ex:o4 }",
            "SELECT ?s ?o WHERE { ?s ex:p+ ?o }",
            "SELECT ?s WHERE { GRAPH ex:g1 { ?s ex:p+ ex:o1 } }",
            "SELECT ?g ?s WHERE { GRAPH ?g { ?s ex:p+ ex:o1 } }",
            "SELECT ?s WHERE { ?s (ex:knows|ex:likes)/ex:name \"anon\" }",
            "SELECT ?s WHERE { ?s ex:says/ex:p ?o }",
        };
        for (String q : queries) {
            assertParity(q);
        }
        assertFalse(rows(ds, "SELECT ?s WHERE { ?s ex:p+ ex:o4 }").isEmpty(), "fixture sanity: a, b, o1, o3 reach o4");
        assertEquals(List.of("s=http://ex.org/a ", "s=http://ex.org/b ", "s=http://ex.org/o1 ", "s=http://ex.org/o3 "),
                rows(ds, "SELECT ?s WHERE { ?s ex:p+ ex:o4 }"));
    }

    @Test
    void namedGraphViewAnswersObjectFirstToo() {
        Node g1 = NodeFactory.createURI("http://ex.org/g1");
        BeakGraph view = (BeakGraph) ds.asDatasetGraph().getGraph(g1);
        List<Triple> found = new ArrayList<>();
        ExtendedIterator<Triple> it = view.find(Node.ANY, Node.ANY, NodeFactory.createURI("http://ex.org/o1"));
        try {
            it.forEachRemaining(found::add);
        } finally {
            it.close();
        }
        assertEquals(3, found.size(), "a p o1, e q o1, o5 p o1 in g1");
        assertTrue(found.stream().allMatch(t -> t.getObject().getURI().equals("http://ex.org/o1")));
        assertEquals(3, ds.asDatasetGraph().stream(g1, Node.ANY, Node.ANY, NodeFactory.createURI("http://ex.org/o1")).count());
        assertEquals(Quad.defaultGraphIRI, bg.getNamedGraph());
    }
}
