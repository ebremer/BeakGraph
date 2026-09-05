package com.ebremer.beakgraph.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.hdf5.jena.BGReader;
import com.ebremer.beakgraph.hdf5.jena.BindingNodeId;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.LongStream;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.shared.AddDeniedException;
import org.apache.jena.shared.ClosedException;
import org.apache.jena.shared.DeleteDeniedException;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The public Graph / Dataset API contracts of BeakGraph: a closed graph says
 * so and refuses reads (BG-5); writes are denied the Jena way (BG-15); a
 * named-graph view's dataset agrees with itself (BG-14); an unresolvable id
 * is dropped, never handed to Quad.create (BG-235); and the document base
 * the constructor accepts is honoured by the query engine, the dataset graph
 * and find() (BG-275, BG-396).
 */
class BeakGraphApiContractTest {

    private static final String PREFIX = "PREFIX ex: <http://ex.org/> PREFIX geo: <http://www.opengis.net/ont/geosparql#> ";
    private static final String TRIG = """
        @prefix ex: <http://ex.org/> .
        ex:d1 ex:p ex:o1 . ex:d2 ex:p ex:o2 . ex:d3 ex:p ex:o3 . ex:d4 ex:p ex:o4 . ex:d5 ex:p ex:o5 .
        ex:g { ex:n1 ex:p ex:o1 . ex:n2 ex:p ex:o2 . ex:n3 ex:p ex:o3 . }
        ex:h { ex:h1 ex:p ex:o1 . }
        """;
    private static final String RELATIVE = """
        @prefix geo:  <http://www.opengis.net/ont/geosparql#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix sdo:  <https://schema.org/> .
        <> a geo:FeatureCollection ; rdfs:label "doc" .
        <image.png> a sdo:ImageObject .
        <http://snomed.info/id/123> a sdo:Thing .
        """;

    @TempDir
    static Path dir;
    static File quads;
    static File relative;

    @BeforeAll
    static void build() throws Exception {
        quads = build("quads.trig", TRIG);
        relative = build("doc.ttl", RELATIVE);
    }

    private static File build(String name, String text) throws Exception {
        Path src = dir.resolve(name);
        Files.writeString(src, text);
        File h5 = dir.resolve(name + ".h5").toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        return h5;
    }

    private static List<String> rows(Dataset ds, String query) {
        List<String> out = new ArrayList<>();
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(PREFIX + query)).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                QuerySolution qs = rs.next();
                StringBuilder sb = new StringBuilder();
                for (String v : rs.getResultVars()) sb.append(v).append('=').append(qs.get(v)).append(' ');
                out.add(sb.toString());
            }
        }
        out.sort(null);
        return out;
    }

    private static int count(Iterator<?> it) {
        int n = 0;
        while (it.hasNext()) { it.next(); n++; }
        return n;
    }

    // --- BG-5 ------------------------------------------------------------

    @Test
    void aClosedGraphSaysSoAndRefusesReads() throws Exception {
        BeakGraph bg = new BeakGraph(new HDF5Reader(quads));
        assertFalse(bg.isClosed());
        assertEquals(5, count(bg.find()));
        bg.close();
        assertTrue(bg.isClosed(), "GraphBase's closed flag is set");
        assertThrows(ClosedException.class, () -> bg.find(Triple.ANY));
        assertThrows(ClosedException.class, bg::find);
        bg.close(); // idempotent

        BeakGraph other = new BeakGraph(new HDF5Reader(quads));
        Dataset ds = other.getDataset();
        assertEquals(5, rows(ds, "SELECT ?s WHERE { ?s ex:p ?o }").size());
        ds.close();
        assertTrue(other.isClosed());
        RuntimeException e = assertThrows(RuntimeException.class, () -> rows(ds, "SELECT ?s WHERE { ?s ex:p ?o }"));
        Throwable root = e;
        while (root.getCause() != null) root = root.getCause();
        assertTrue(root instanceof ClosedException || String.valueOf(root.getMessage()).contains("closed"),
                "a query over a closed dataset fails on the closed reader, not deep inside jHDF: " + root);
    }

    // --- BG-15 -----------------------------------------------------------

    @Test
    void writesAreDeniedTheJenaWayInBothForms() throws Exception {
        try (BeakGraph bg = new BeakGraph(new HDF5Reader(quads))) {
            Node s = NodeFactory.createURI("http://ex.org/x");
            Triple t = Triple.create(s, s, s);
            assertThrows(AddDeniedException.class, () -> bg.add(t));
            assertThrows(AddDeniedException.class, () -> bg.add(s, s, s));
            assertThrows(DeleteDeniedException.class, () -> bg.delete(t));
            assertThrows(DeleteDeniedException.class, () -> bg.delete(s, s, s));
            assertEquals(5, bg.size(), "nothing changed");
        }
    }

    // --- BG-14 -----------------------------------------------------------

    @Test
    void aViewsDatasetAgreesWithItselfAboutItsDefaultGraph() throws Exception {
        try (BeakGraph bg = new BeakGraph(new HDF5Reader(quads))) {
            Node g = NodeFactory.createURI("http://ex.org/g");
            BeakGraph view = (BeakGraph) bg.getDataset().asDatasetGraph().getGraph(g);
            assertEquals(3, view.size());
            Dataset d2 = view.getDataset();
            DatasetGraph dsg = d2.asDatasetGraph();
            assertEquals(3, d2.getDefaultModel().size());
            assertEquals(3, count(dsg.find(Quad.defaultGraphIRI, Node.ANY, Node.ANY, Node.ANY)),
                    "find(defaultGraph) answers the view's graph, not the stored default graph");
            assertEquals(3, rows(d2, "SELECT ?s WHERE { ?s ex:p ?o }").size());
            List<String> any = new ArrayList<>();
            dsg.find(Node.ANY, Node.ANY, Node.ANY, Node.ANY).forEachRemaining(q -> any.add(q.getGraph().toString()));
            assertEquals(3, any.stream().filter(x -> x.equals(Quad.defaultGraphIRI.toString())).count(), any.toString());
            assertFalse(any.contains("http://ex.org/g"), "the view's own graph is the default graph, not a named one: " + any);
            assertEquals(1, any.stream().filter(x -> x.equals("http://ex.org/h")).count(), "other named graphs stay named");
            List<Node> named = new ArrayList<>();
            dsg.listGraphNodes().forEachRemaining(named::add);
            assertFalse(named.contains(g));
            d2.close();
            assertTrue(view.isClosed());
            assertTrue(bg.getReader().isOpen(), "closing a view's dataset never closes the shared reader");
            assertEquals(5, bg.size());
        }
    }

    // --- BG-235 ----------------------------------------------------------

    /** A reader whose node table cannot resolve one id: the contract every NodeTable is allowed to have. */
    private static final class PoisonedReader implements BGReader {
        private final BGReader delegate;
        private final Node poisoned;

        PoisonedReader(BGReader delegate, Node poisoned) {
            this.delegate = delegate;
            this.poisoned = poisoned;
        }

        @Override public GSPODictionary getDictionary() { return delegate.getDictionary(); }
        @Override public NodeTable getNodeTable() {
            NodeTable real = delegate.getNodeTable();
            return new NodeTable() {
                @Override public long getNodeIdForNode(Node n) { return real.getNodeIdForNode(n); }
                @Override public Node getNodeForNodeId(long nodeId) {
                    Node n = real.getNodeForNodeId(nodeId);
                    return (n != null && n.equals(poisoned)) ? null : n;
                }
                @Override public void close() {}
            };
        }
        @Override public Iterator<BindingNodeId> read(Node ng, BindingNodeId b, Triple t, ExprList f, NodeTable nt) { return delegate.read(ng, b, t, f, nt); }
        @Override public Iterator<BindingNodeId> readGraphs(Collection<Node> g, BindingNodeId b, Triple t, ExprList f, NodeTable nt) { return delegate.readGraphs(g, b, t, f, nt); }
        @Override public Iterator<BindingNodeId> read(long gid, BindingNodeId b, Triple t, ExprList f, NodeTable nt) { return delegate.read(gid, b, t, f, nt); }
        @Override public LongStream graphIds() { return delegate.graphIds(); }
        @Override public ExtendedIterator<Triple> graphBaseFind(Node graph, Triple tp) { return delegate.graphBaseFind(graph, tp); }
        @Override public Iterator<Node> listGraphNodes() { return delegate.listGraphNodes(); }
        @Override public boolean containsGraph(Node graphNode) { return delegate.containsGraph(graphNode); }
        @Override public URI getURI() { return delegate.getURI(); }
        @Override public boolean isOpen() { return delegate.isOpen(); }
        @Override public long getFormatVersion() { return delegate.getFormatVersion(); }
        @Override public long countTriples(Node graph) { return delegate.countTriples(graph); }
        @Override public void close() throws Exception { delegate.close(); }
    }

    @Test
    void anUnresolvableIdIsDroppedNotHandedToQuadCreate() throws Exception {
        try (HDF5Reader real = new HDF5Reader(quads)) {
            try (BeakGraph bg = new BeakGraph(new PoisonedReader(real, NodeFactory.createURI("http://ex.org/o1")))) {
                DatasetGraph dsg = bg.getDataset().asDatasetGraph();
                List<Quad> all = new ArrayList<>();
                dsg.find(Node.ANY, Node.ANY, Node.ANY, Node.ANY).forEachRemaining(all::add);
                assertEquals(9 - 3, all.size(), "the three rows naming ex:o1 are dropped, the rest survive: " + all);
                assertTrue(all.stream().noneMatch(q -> q.getObject().getURI().equals("http://ex.org/o1")));
                List<Quad> named = new ArrayList<>();
                dsg.findNG(Node.ANY, Node.ANY, Node.ANY, Node.ANY).forEachRemaining(named::add);
                assertEquals(4 - 2, named.size());
            }
        }
    }

    // --- BG-275 / BG-396 -------------------------------------------------

    @Test
    void theBaseIsHonouredByTheEngineTheDatasetGraphAndFind() throws Exception {
        try (BeakGraph plain = new BeakGraph(new HDF5Reader(relative))) {
            assertNull(plain.getBase(), "a local file has no base unless given one");
            assertEquals(List.of("s= "), rows(plain.getDataset(), "SELECT ?s WHERE { ?s a geo:FeatureCollection }"),
                    "without a base the stored relative term is served as is");
        }
        URI base = URI.create("http://example.org/data/doc.ttl.h5");
        try (BeakGraph bg = new BeakGraph(new HDF5Reader(relative), relative.toURI(), base)) {
            assertEquals(base, bg.getBase());
            Dataset ds = bg.getDataset();
            assertEquals(List.of("s=" + base + " "), rows(ds, "SELECT ?s WHERE { ?s a geo:FeatureCollection }"),
                    "results are resolved against the base");
            assertEquals(2, rows(ds, "SELECT ?p ?o WHERE { <" + base + "> ?p ?o }").size(),
                    "the document named by its served URL reaches the stored <> term");
            assertEquals(1, rows(ds, "SELECT ?s WHERE { <http://example.org/data/image.png> a <https://schema.org/ImageObject> }").size());
            assertEquals(1, rows(ds, "SELECT ?t WHERE { <http://snomed.info/id/123> a ?t }").size(), "absolute terms are untouched");
            // the quad API and the graph API agree
            DatasetGraph dsg = ds.asDatasetGraph();
            List<Quad> all = new ArrayList<>();
            dsg.find(Node.ANY, Node.ANY, Node.ANY, Node.ANY).forEachRemaining(all::add);
            assertTrue(all.stream().allMatch(q -> !q.getSubject().isURI() || q.getSubject().getURI().contains(":")),
                    "no relative IRI leaves find(): " + all);
            assertEquals(2, count(dsg.find(Node.ANY, NodeFactory.createURI(base.toString()), Node.ANY, Node.ANY)));
            assertEquals(2, count(bg.find(Triple.create(NodeFactory.createURI(base.toString()), Node.ANY, Node.ANY))));
            assertEquals(1, count(bg.find(Triple.create(NodeFactory.createURI("http://example.org/data/image.png"), Node.ANY, Node.ANY))));
        }
        assertEquals(URI.create("https://h/x.h5"), BeakGraph.defaultBase(URI.create("https://h/x.h5")));
        assertNull(BeakGraph.defaultBase(relative.toURI()));
    }
}
