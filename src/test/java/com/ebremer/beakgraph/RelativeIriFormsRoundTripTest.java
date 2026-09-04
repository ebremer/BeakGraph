package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.fuseki.RelativeIRIResolver;
import com.ebremer.beakgraph.core.lib.RelativeIris;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.sparql.graph.NodeTransform;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression tests for BG-391: the relative-IRI sentinel base had a single
 * path segment, so RFC 3986 resolution discarded {@code ..} above the root
 * and a path-absolute reference became a child of {@code /}. {@code <../t.png>},
 * {@code </LICENSE>} and {@code <t.png>} were all stored as the child form and,
 * served from any URL below the server root, resolved into the document's own
 * directory; a query naming the correct served IRI found nothing. A sibling
 * literally named like the sentinel's leaf collapsed onto {@code <>}. The base
 * is now deep, with an unauthorable leaf, and relativization is textual with
 * the canonical {@code ../} form for any number of levels.
 */
class RelativeIriFormsRoundTripTest {

    /** Source reference -> expected stored form. */
    private static final Map<String, String> FORMS = new LinkedHashMap<>();
    static {
        FORMS.put("<>", "");
        FORMS.put("<#f>", "#f");
        FORMS.put("<?q=1>", "?q=1");
        FORMS.put("<sib.png>", "sib.png");
        FORMS.put("<sub/dir/x.png>", "sub/dir/x.png");
        FORMS.put("<document>", "document");
        FORMS.put("<../t.png>", "../t.png");
        FORMS.put("<../../up2.png>", "../../up2.png");
        FORMS.put("<../a/../b.png>", "../b.png");
        FORMS.put("</LICENSE>", "/LICENSE");
        FORMS.put("</a/b?x=1>", "/a/b?x=1");
        FORMS.put("<./y.png>", "y.png");
    }
    private static final String SERVED = "http://host/HalcyonStorage/utah/hdf5/doc.h5";

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;

    @BeforeAll
    static void buildAndOpen() throws Exception {
        StringBuilder ttl = new StringBuilder("@prefix ex: <http://ex.org/> .\n");
        int i = 0;
        for (String ref : FORMS.keySet()) {
            ttl.append("<> ex:ref").append(i++).append(' ').append(ref).append(" .\n");
        }
        File src = dir.resolve("doc.ttl").toFile();
        File h5 = dir.resolve("doc.ttl.h5").toFile();
        Files.writeString(src.toPath(), ttl, StandardCharsets.UTF_8);
        HDF5Writer.Builder().setSource(src).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        bg = new BeakGraph(new HDF5Reader(h5));
        ds = bg.getDataset();
    }

    @AfterAll
    static void close() {
        if (bg != null) bg.close();
    }

    private static Node object(int i) {
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create("SELECT ?o WHERE { ?s <http://ex.org/ref" + i + "> ?o }")).build()) {
            ResultSet rs = qe.execSelect();
            Node o = rs.next().get("o").asNode();
            assertEquals(false, rs.hasNext());
            return o;
        }
    }

    private static boolean stored(Node n) {
        return bg.getReader().getDictionary().getObjects().locate(n) >= 1
            || bg.getReader().getDictionary().getSubjects().locate(n) >= 1;
    }

    @Test
    void everyReferenceFormIsStoredAsWritten() {
        int i = 0;
        for (Map.Entry<String, String> e : FORMS.entrySet()) {
            assertEquals(e.getValue(), object(i++).getURI(), "stored form of " + e.getKey());
        }
    }

    @Test
    void storedFormsServeAsTheRightUrlAndAreReachableByIt() {
        // The served IRI is what RFC 3986 gives for the ORIGINAL reference
        // against the serving URL; the round trip through storage must agree.
        org.apache.jena.irix.IRIx served = org.apache.jena.irix.IRIx.create(SERVED);
        RelativeIRIResolver r = new RelativeIRIResolver(SERVED);
        NodeTransform out = r.storageToAbsolute();
        NodeTransform in = r.absoluteToStorage(RelativeIriFormsRoundTripTest::stored);
        for (Map.Entry<String, String> e : FORMS.entrySet()) {
            String ref = e.getKey().substring(1, e.getKey().length() - 1);
            String expected = served.resolve(ref).str();
            Node storedNode = NodeFactory.createURI(e.getValue());
            assertEquals(expected, out.apply(storedNode).getURI(), "serving " + e.getKey());
            assertEquals(e.getValue(), in.apply(NodeFactory.createURI(expected)).getURI(),
                    "query naming " + expected + " must reach the stored term");
        }
        assertEquals("http://host/HalcyonStorage/utah/thumbs/t.png", served.resolve("../thumbs/t.png").str());
    }

    @Test
    void queryByServedIriFindsTheRow() {
        // End to end through the resolver: a SELECT naming the served IRI of a
        // parent-directory reference must match (the old code returned 0 rows).
        RelativeIRIResolver r = new RelativeIRIResolver(SERVED);
        NodeTransform in = r.absoluteToStorage(RelativeIriFormsRoundTripTest::stored);
        Node named = in.apply(NodeFactory.createURI("http://host/HalcyonStorage/utah/t.png"));
        assertEquals("../t.png", named.getURI());
        // Substituted as a node: the SPARQL parser would resolve a relative
        // <../t.png> against the query's own base.
        try (QueryExecution qe = QueryExecution.dataset(ds)
                .query(QueryFactory.create("SELECT ?p WHERE { ?s ?p ?o }"))
                .substitution("o", org.apache.jena.rdf.model.ModelFactory.createDefaultModel().asRDFNode(named)).build()) {
            ResultSet rs = qe.execSelect();
            assertEquals("http://ex.org/ref6", rs.next().get("p").asNode().getURI());
        }
    }

    @Test
    void siblingNamedLikeTheOldSentinelLeafIsNotTheDocument() {
        // <document> used to relativize to "" and merge with <>.
        assertEquals("document", object(5).getURI());
        assertEquals("", object(0).getURI());
    }

    @Test
    void textualRelativizerCoversEveryLevel() {
        String base = "http://h/a/b/c/doc";
        assertEquals("", RelativeIris.relativize(base, "http://h/a/b/c/doc"));
        assertEquals("#f", RelativeIris.relativize(base, "http://h/a/b/c/doc#f"));
        assertEquals("?q", RelativeIris.relativize(base, "http://h/a/b/c/doc?q"));
        assertEquals("?q#f", RelativeIris.relativize(base, "http://h/a/b/c/doc?q#f"));
        assertEquals("x", RelativeIris.relativize(base, "http://h/a/b/c/x"));
        assertEquals("x/y", RelativeIris.relativize(base, "http://h/a/b/c/x/y"));
        assertEquals("./", RelativeIris.relativize(base, "http://h/a/b/c/"));
        assertEquals("../x", RelativeIris.relativize(base, "http://h/a/b/x"));
        assertEquals("../", RelativeIris.relativize(base, "http://h/a/b/"));
        assertEquals("../../x", RelativeIris.relativize(base, "http://h/a/x"));
        assertEquals("../../../x", RelativeIris.relativize(base, "http://h/x"));
        assertEquals("../../../", RelativeIris.relativize(base, "http://h/"));
        assertEquals("../../../x?q#f", RelativeIris.relativize(base, "http://h/x?q#f"));
        assertEquals("../c", RelativeIris.relativize(base, "http://h/a/b/c"));   // the directory named as a file
        assertEquals("./a:b", RelativeIris.relativize(base, "http://h/a/b/c/a:b"));
        assertNull(RelativeIris.relativize(base, "http://other/a/b/c/doc"));
        assertNull(RelativeIris.relativize(base, "urn:x"));
        assertEquals("/a/x", RelativeIris.pathAbsoluteForm(base, "http://h/a/x"));
        assertNull(RelativeIris.pathAbsoluteForm(base, "https://h/a/x"));

        // Directly under the sentinel root: a source's </x>, or a reference
        // collapsed beyond the sentinel's depth - stored path-absolute.
        assertEquals("/up.png", RelativeIris.toStorageForm(RelativeIris.SENTINEL_PREFIX + "up.png"));
        assertEquals("/", RelativeIris.toStorageForm(RelativeIris.SENTINEL_PREFIX));
        assertEquals("../".repeat(RelativeIris.SENTINEL_DEPTH - 1) + "x",
                RelativeIris.toStorageForm(RelativeIris.SENTINEL_PREFIX + "d/x"));
        assertEquals("", RelativeIris.toStorageForm(RelativeIris.SENTINEL_BASE));
        assertEquals("sib.png", RelativeIris.toStorageForm(RelativeIris.SENTINEL_DIR + "sib.png"));
        assertEquals("../t.png", RelativeIris.toStorageForm(RelativeIris.SENTINEL_PREFIX + "d/".repeat(RelativeIris.SENTINEL_DEPTH - 1) + "t.png"));
        assertNull(RelativeIris.toStorageForm("http://ex.org/x"));
    }
}
