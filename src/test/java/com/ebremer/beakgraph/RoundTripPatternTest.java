package com.ebremer.beakgraph;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Generative write -> read round-trip. A model with deliberate subject/predicate/object overlaps and
 * mixed object types (shared URIs, shared string literals, a long FCD-compressed literal, a typed
 * integer) is written to HDF5, then every S/P/O pattern shape is queried and compared against the
 * in-memory graph. Each position ranges over {every real value, ANY, one absent value}, so the
 * cross-product exercises all eight bound/unbound shapes plus single-, multi-, and empty-result cases.
 */
class RoundTripPatternTest {

    private static final String NS = "http://ex.org/";

    @TempDir static Path dir;
    static Graph truth;
    static BeakGraph bg;

    @BeforeAll
    static void build() throws Exception {
        Model m = ModelFactory.createDefaultModel();
        for (int s = 0; s < 4; s++) {
            Resource subj = m.createResource(NS + "s" + s);
            for (int p = 0; p < 3; p++) {
                Property pred = m.createProperty(NS + "p" + p);
                if ((s + p) % 2 == 0) {
                    subj.addProperty(pred, m.createResource(NS + "o" + ((s + p) % 3))); // shared URI objects
                } else {
                    subj.addProperty(pred, m.createLiteral("lit" + p));                 // shared string literals
                }
            }
        }
        // Extra triples: a second object for (s2,p0) (multi-result), mixed/edge object types.
        m.getResource(NS + "s2").addProperty(m.createProperty(NS + "p0"), m.createResource(NS + "o1"));
        m.getResource(NS + "s0").addLiteral(m.createProperty(NS + "num"), 42);                // typed numeric
        m.getResource(NS + "s1").addProperty(m.createProperty(NS + "long"), "x".repeat(120)); // FCD-compressed

        File ttl = dir.resolve("rt.ttl").toFile();
        File h5 = dir.resolve("rt.ttl.h5").toFile();
        try (OutputStream out = Files.newOutputStream(ttl.toPath())) {
            RDFDataMgr.write(out, m, Lang.TURTLE);
        }
        HDF5Writer.Builder().setSource(ttl).setDestination(h5).setSpatial(false).setFeatures(false).build().write();

        truth = m.getGraph();
        bg = new BeakGraph(new HDF5Reader(h5), h5.toURI());
    }

    @AfterAll
    static void close() throws Exception {
        if (bg != null) bg.close();
    }

    private static Set<Triple> set(ExtendedIterator<Triple> it) {
        Set<Triple> s = new HashSet<>();
        try { it.forEachRemaining(s::add); } finally { it.close(); }
        return s;
    }

    @Test
    void everyPatternShapeMatchesInMemoryGraph() {
        Node ANY = Node.ANY;
        List<Node> subs = new ArrayList<>();
        List<Node> preds = new ArrayList<>();
        List<Node> objs = new ArrayList<>();
        truth.find(ANY, ANY, ANY).forEachRemaining(t -> {
            if (!subs.contains(t.getSubject())) subs.add(t.getSubject());
            if (!preds.contains(t.getPredicate())) preds.add(t.getPredicate());
            if (!objs.contains(t.getObject())) objs.add(t.getObject());
        });
        // every position also ranges over ANY (unbound) and a value that is absent from the store
        subs.add(ANY);  subs.add(NodeFactory.createURI(NS + "absentS"));
        preds.add(ANY); preds.add(NodeFactory.createURI(NS + "absentP"));
        objs.add(ANY);  objs.add(NodeFactory.createURI(NS + "absentO"));

        // sanity: the data actually has the overlaps the cross-product relies on
        assertTrue(set(truth.find(NodeFactory.createURI(NS + "s2"), NodeFactory.createURI(NS + "p0"), ANY)).size() >= 2,
            "expected (s2,p0) to have multiple objects");

        int checked = 0;
        for (Node s : subs) {
            for (Node p : preds) {
                for (Node o : objs) {
                    Set<Triple> expected = set(truth.find(s, p, o));
                    Set<Triple> actual = set(bg.find(s, p, o));
                    assertEquals(expected, actual, "pattern { " + s + " , " + p + " , " + o + " }");
                    checked++;
                }
            }
        }
        assertEquals(subs.size() * preds.size() * objs.size(), checked);
    }
}
