package com.ebremer.beakgraph.core.fuseki;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.function.Predicate;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ResultSetStream;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.graph.NodeTransform;
import org.apache.jena.sys.JenaSystem;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link RelativeIRIResolver} - the bidirectional transform between
 * stored relative IRIs and absolute IRIs resolved against the serving URL.
 */
class RelativeIRIResolverTest {

    private static final String BASE =
            "http://localhost:8888/HalcyonStorage/utah/hdf5/doc.ttl.h5";
    private static final String SIBLING =
            "http://localhost:8888/HalcyonStorage/utah/hdf5/image.png";

    @BeforeAll
    static void initJena() {
        JenaSystem.init();
    }

    @Test
    void storageToAbsoluteResolvesEmptyReferenceToBase() {
        NodeTransform t = new RelativeIRIResolver(BASE).storageToAbsolute();
        assertEquals(BASE, t.apply(NodeFactory.createURI("")).getURI());
    }

    @Test
    void storageToAbsoluteResolvesSiblingReference() {
        NodeTransform t = new RelativeIRIResolver(BASE).storageToAbsolute();
        assertEquals(SIBLING, t.apply(NodeFactory.createURI("image.png")).getURI());
    }

    @Test
    void storageToAbsoluteLeavesAbsoluteIrisUnchanged() {
        NodeTransform t = new RelativeIRIResolver(BASE).storageToAbsolute();
        Node snomed = NodeFactory.createURI("http://snomed.info/id/123");
        assertSame(snomed, t.apply(snomed));
    }

    /** A dictionary that holds only relative-form terms (no scheme) - the shape a BG store has. */
    private static final Predicate<Node> RELATIVE_FORMS_STORED = n -> !n.getURI().contains(":");

    @Test
    void absoluteToStorageRelativizesTheDocumentItself() {
        NodeTransform t = new RelativeIRIResolver(BASE).absoluteToStorage(RELATIVE_FORMS_STORED);
        assertEquals("", t.apply(NodeFactory.createURI(BASE)).getURI());
    }

    @Test
    void absoluteToStorageRelativizesASibling() {
        NodeTransform t = new RelativeIRIResolver(BASE).absoluteToStorage(RELATIVE_FORMS_STORED);
        assertEquals("image.png", t.apply(NodeFactory.createURI(SIBLING)).getURI());
    }

    @Test
    void absoluteToStorageLeavesUnrelatedIrisUnchanged() {
        NodeTransform t = new RelativeIRIResolver(BASE).absoluteToStorage(RELATIVE_FORMS_STORED);
        Node snomed = NodeFactory.createURI("http://snomed.info/id/123");
        assertSame(snomed, t.apply(snomed));
    }

    @Test
    void absoluteStoredIriIsNotRewrittenIntoAGuaranteedMiss() {
        // Same host, different subtree: relativize yields the path-absolute form
        // "/other/thing", which the writer never stores. The rewrite must not
        // fire - the dictionary holds the ABSOLUTE term and the query must keep
        // naming it (the old unconditional rewrite silently returned 0 rows).
        String abs = "http://localhost:8888/other/thing";
        NodeTransform t = new RelativeIRIResolver(BASE)
                .absoluteToStorage(n -> n.getURI().equals(abs));
        Node node = NodeFactory.createURI(abs);
        assertSame(node, t.apply(node));
    }

    @Test
    void nothingStoredMeansNoRewriteForAnyForm() {
        Node up = NodeFactory.createURI("http://localhost:8888/HalcyonStorage/utah/other.png");
        NodeTransform t = new RelativeIRIResolver(BASE).absoluteToStorage(n -> false);
        assertSame(up, t.apply(up));
    }

    @Test
    void parentAndPathAbsoluteFormsAreRewrittenWhenStored() {
        // BG-391: the writer stores <../other.png>, <../../x> and </LICENSE>
        // in exactly those forms; a query naming the served IRI must reach them.
        String[][] cases = {
            {"http://localhost:8888/HalcyonStorage/utah/other.png", "../other.png"},
            {"http://localhost:8888/HalcyonStorage/x", "../../x"},
            {"http://localhost:8888/LICENSE", "/LICENSE"},
            {"http://localhost:8888/HalcyonStorage/utah/hdf5/sub/y.png", "sub/y.png"},
        };
        for (String[] c : cases) {
            NodeTransform t = new RelativeIRIResolver(BASE).absoluteToStorage(n -> n.getURI().equals(c[1]));
            assertEquals(c[1], t.apply(NodeFactory.createURI(c[0])).getURI(), c[0]);
            // and the stored form serves back as the IRI the query named
            assertEquals(c[0], new RelativeIRIResolver(BASE).storageToAbsolute()
                    .apply(NodeFactory.createURI(c[1])).getURI(), c[1]);
        }
    }

    @Test
    void bothFormsStoredKeepsTheExactTermTheQueryNamed() {
        NodeTransform t = new RelativeIRIResolver(BASE).absoluteToStorage(n -> true);
        Node sibling = NodeFactory.createURI(SIBLING);
        assertSame(sibling, t.apply(sibling));
    }

    @Test
    void inputThenOutputRoundTripsToTheOriginalIri() {
        RelativeIRIResolver r = new RelativeIRIResolver(BASE);
        NodeTransform in = r.absoluteToStorage(RELATIVE_FORMS_STORED);
        NodeTransform out = r.storageToAbsolute();
        for (String iri : new String[] { BASE, SIBLING }) {
            Node original = NodeFactory.createURI(iri);
            assertEquals(iri, out.apply(in.apply(original)).getURI());
        }
    }

    @Test
    void nullBaseMakesResolverInactiveAndTransformsIdentity() {
        RelativeIRIResolver r = new RelativeIRIResolver(null);
        assertFalse(r.isActive());
        Node rel = NodeFactory.createURI("image.png");
        assertSame(rel, r.storageToAbsolute().apply(rel));
        assertSame(rel, r.absoluteToStorage(n -> true).apply(rel));
    }

    @Test
    void resolveModelResolvesRelativeIrisInTriples() {
        Model in = ModelFactory.createDefaultModel();
        in.add(in.createResource(""), RDF.type,
                in.createResource("https://schema.org/Dataset"));
        in.add(in.createResource("image.png"), RDF.type,
                in.createResource("https://schema.org/ImageObject"));

        Model out = new RelativeIRIResolver(BASE).resolve(in);

        assertTrue(out.containsResource(out.createResource(BASE)));
        assertTrue(out.containsResource(out.createResource(SIBLING)));
    }

    @Test
    void resolveResultSetResolvesBoundRelativeIris() {
        Var s = Var.alloc("s");
        Binding b = Binding.builder().add(s, NodeFactory.createURI("")).build();
        ResultSet rs = ResultSetStream.create(List.of(s), List.of(b).iterator());

        ResultSet resolved = new RelativeIRIResolver(BASE).resolve(rs);

        assertTrue(resolved.hasNext());
        assertEquals(BASE, resolved.next().get("s").asNode().getURI());
    }
}
