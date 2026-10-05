package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.jena.AggregateCountFastPath;
import com.ebremer.beakgraph.hdf5.jena.DistinctTermFastPath;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * BG-183: the quad-level companion of {@link RoundTripPatternTest}. The
 * graph position selects between BGIteratorMaster's GSPO/GPOS dispatch, the
 * union-graph view, the default-graph sentinels and the "entity that is not
 * a graph" guard, and it was only ever spot-checked. Here every (g, s, p, o)
 * in {each real value, ANY, one absent value} - plus the two default-graph
 * sentinels, the union graph and a subject used as a graph name - is compared
 * against Jena's in-memory dataset loaded from the same TriG, through both
 * {@code find} and {@code findNG}. A second fixture with no default-graph
 * triples pins the fast paths on an empty default graph.
 */
class RoundTripQuadPatternTest {

    private static final String NS = "http://ex.org/";
    private static final String TRIG = """
        @prefix ex: <http://ex.org/> .
        ex:s0 ex:p0 ex:o0 .
        ex:s0 ex:p1 "lit1" .
        ex:s1 ex:p0 ex:o1 .
        ex:s1 ex:p1 "lit0" .
        ex:s2 ex:p2 ex:s0 .
        ex:g1 {
            ex:s0 ex:p0 ex:o0 .
            ex:s1 ex:p1 "lit1" .
            ex:s3 ex:p2 ex:o1 .
        }
        ex:g2 {
            ex:s0 ex:p0 ex:o1 .
            ex:s2 ex:p1 "lit1" .
            ex:s2 ex:p0 ex:o0 .
        }
        ex:g3 {
            ex:s1 ex:p0 ex:o0 .
            ex:s1 ex:p2 ex:g1 .
        }
        """;

    @TempDir
    static Path dir;
    static DatasetGraph truth;
    static BeakGraph bg;
    static DatasetGraph dsg;

    @BeforeAll
    static void build() throws Exception {
        Path src = dir.resolve("quads.trig");
        Files.writeString(src, TRIG);
        File h5 = dir.resolve("quads.h5").toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        truth = DatasetGraphFactory.create();
        RDFParser.create().source(src.toUri().toString()).lang(Lang.TRIG).parse(truth);
        bg = BG.getBeakGraph(h5);
        dsg = bg.getDataset().asDatasetGraph();
    }

    @AfterAll
    static void close() {
        if (bg != null) bg.close();
    }

    /** Quads as strings with both default-graph sentinels folded onto one and internal graphs dropped. */
    private static Set<String> normalize(Iterator<Quad> it, boolean triplesOnly) {
        Set<String> out = new TreeSet<>();
        while (it.hasNext()) {
            Quad q = it.next();
            Node g = q.getGraph();
            if (g != null && g.isURI() && g.getURI().startsWith("urn:x-beakgraph:")) continue;
            if (triplesOnly) {
                out.add(q.asTriple().toString());
            } else {
                Node gn = (g == null || Quad.isDefaultGraph(g)) ? Quad.defaultGraphIRI : g;
                out.add(new Quad(gn, q.asTriple()).toString());
            }
        }
        return out;
    }

    private static List<Node> values(int position) {
        Set<Node> seen = new HashSet<>();
        List<Node> out = new ArrayList<>();
        Iterator<Quad> it = truth.find(Node.ANY, Node.ANY, Node.ANY, Node.ANY);
        while (it.hasNext()) {
            Quad q = it.next();
            Node n = switch (position) {
                case 0 -> q.getGraph();
                case 1 -> q.getSubject();
                case 2 -> q.getPredicate();
                default -> q.getObject();
            };
            if (n != null && seen.add(n)) out.add(n);
        }
        return out;
    }

    @Test
    void everyQuadPatternShapeMatchesInMemoryDataset() {
        List<Node> graphs = values(0);
        graphs.removeIf(Quad::isDefaultGraph);
        graphs.add(Node.ANY);
        graphs.add(Quad.defaultGraphIRI);
        graphs.add(Quad.defaultGraphNodeGenerated);
        graphs.add(Quad.unionGraph);
        graphs.add(NodeFactory.createURI(NS + "s0"));        // an entity that is not a graph (padding-block guard)
        graphs.add(NodeFactory.createURI(NS + "absentG"));
        List<Node> subs = values(1);
        subs.add(Node.ANY);
        subs.add(NodeFactory.createURI(NS + "absentS"));
        List<Node> preds = values(2);
        preds.add(Node.ANY);
        preds.add(NodeFactory.createURI(NS + "absentP"));
        List<Node> objs = values(3);
        objs.add(Node.ANY);
        objs.add(NodeFactory.createURI(NS + "absentO"));

        assertTrue(subs.contains(NodeFactory.createURI(NS + "g1")) || objs.contains(NodeFactory.createURI(NS + "g1")),
                "fixture: a graph name must also occur as a term");

        int checked = 0;
        for (Node g : graphs) {
            // The union graph's quads carry whichever graph node an implementation
            // chooses (Jena: the union sentinel); compare the distinct TRIPLES there.
            boolean triplesOnly = Quad.isUnionGraph(g);
            for (Node s : subs) {
                for (Node p : preds) {
                    for (Node o : objs) {
                        String at = "{ " + g + " , " + s + " , " + p + " , " + o + " }";
                        assertEquals(normalize(truth.find(g, s, p, o), triplesOnly), normalize(dsg.find(g, s, p, o), triplesOnly), "find " + at);
                        assertEquals(normalize(truth.findNG(g, s, p, o), triplesOnly), normalize(dsg.findNG(g, s, p, o), triplesOnly), "findNG " + at);
                        checked++;
                    }
                }
            }
        }
        assertEquals(graphs.size() * subs.size() * preds.size() * objs.size(), checked);
        // Not vacuous: the fixture's overlaps produce multi-row answers.
        assertTrue(normalize(dsg.find(Node.ANY, NodeFactory.createURI(NS + "s0"), NodeFactory.createURI(NS + "p0"), Node.ANY), false).size() >= 3);
    }

    private static long count(Dataset ds, String query) {
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(query)).build()) {
            ResultSet rs = qe.execSelect();
            return rs.next().getLiteral(rs.getResultVars().get(0)).getLong();
        }
    }

    private static int rows(Dataset ds, String query) {
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(query)).build()) {
            ResultSet rs = qe.execSelect();
            int n = 0;
            while (rs.hasNext()) { rs.next(); n++; }
            return n;
        }
    }

    @Test
    void namedGraphOnlyStoreHasAnEmptyDefaultGraphOnEveryFastPath() throws Exception {
        Path src = dir.resolve("named-only.trig");
        Files.writeString(src, """
            @prefix ex: <http://ex.org/> .
            ex:g1 { ex:a ex:p ex:b . ex:a ex:q "x" . }
            ex:g2 { ex:c ex:p ex:b . }
            """);
        File h5 = dir.resolve("named-only.h5").toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        try (BeakGraph only = BG.getBeakGraph(h5)) {
            Dataset ds = only.getDataset();
            long before = AggregateCountFastPath.HITS.get();
            assertEquals(0, count(ds, "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }"));
            assertEquals(0, count(ds, "SELECT (COUNT(DISTINCT ?s) AS ?n) WHERE { ?s ?p ?o }"));
            assertEquals(2, AggregateCountFastPath.HITS.get() - before, "the empty default graph is answered by the index fast path");
            before = DistinctTermFastPath.HITS.get();
            assertEquals(0, rows(ds, "SELECT DISTINCT ?s WHERE { ?s ?p ?o }"));
            assertEquals(0, rows(ds, "SELECT DISTINCT ?p WHERE { ?s ?p ?o }"));
            assertEquals(2, DistinctTermFastPath.HITS.get() - before);
            assertEquals(0, ds.getDefaultModel().size());
            assertEquals(1, rows(ds, "SELECT ?s WHERE { GRAPH <http://ex.org/g1> { ?s <http://ex.org/p> <http://ex.org/b> } }"));
            assertEquals(3, rows(ds, "SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }"));
            assertEquals(2, count(ds, "SELECT (COUNT(DISTINCT ?g) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } }"));
            assertEquals(3, normalize(only.getDataset().asDatasetGraph().find(Quad.unionGraph, Node.ANY, Node.ANY, Node.ANY), true).size());
        }
    }
}
