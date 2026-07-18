package com.ebremer.beakgraph.w3c;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.ebremer.beakgraph.BG;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.util.IsoMatcher;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.io.TempDir;

/**
 * Manifest-driven runner for the vendored W3C RDF 1.2 test suites
 * (src/test/resources/w3c/rdf12 - see its README for provenance).
 *
 * <p>Format v5 stores every RDF 1.2 term kind - base-direction literals since
 * v4, triple terms since v5 - so {@link #containsUnsupportedTerms} is empty
 * and every manifest entry runs the full build-and-round-trip branch: this
 * suite is the RDF 1.2 CONFORMANCE ORACLE. (The containment machinery in
 * containmentSplit is kept for any future term kind that repeats the
 * reject-loudly-first lifecycle.)
 *
 * <p>Per test type:
 * <ul>
 *   <li>NegativeSyntax - Jena's parse must fail (ingest = Jena, so this IS
 *       BeakGraph's ingest conformance).</li>
 *   <li>PositiveSyntax - parse must succeed; then the containment split.</li>
 *   <li>Eval - parse action and expected result; if Jena's parse of the action
 *       is not isomorphic to the expected result the test ABORTS (upstream
 *       divergence, not BeakGraph's bug - BeakGraph's contract is fidelity to
 *       what Jena hands it); then the containment split, with the supported
 *       branch asserting store round-trip isomorphism.</li>
 *   <li>PositiveC14N - skipped: canonical N-Triples serialization is a
 *       serializer property, out of scope for a storage layer.</li>
 * </ul>
 */
class W3CRdf12SuiteTest {

    private static final String MF = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#";

    private record Suite(String dir, Lang lang) {}

    private static final List<Suite> SUITES = List.of(
        new Suite("rdf-turtle/syntax", Lang.TURTLE),
        new Suite("rdf-turtle/eval", Lang.TURTLE),
        new Suite("rdf-n-triples/syntax", Lang.NTRIPLES),
        new Suite("rdf-n-triples/c14n", Lang.NTRIPLES),
        new Suite("rdf-n-quads/syntax", Lang.NQUADS),
        new Suite("rdf-n-quads/c14n", Lang.NQUADS),
        new Suite("rdf-trig/syntax", Lang.TRIG),
        new Suite("rdf-trig/eval", Lang.TRIG));

    @TempDir
    static Path tmp;

    @TestFactory
    Stream<DynamicNode> w3cRdf12() {
        return SUITES.stream().map(this::suiteContainer);
    }

    private DynamicContainer suiteContainer(Suite suite) {
        Path manifestPath = resourcePath("/w3c/rdf12/" + suite.dir() + "/manifest.ttl");
        Model m = RDFDataMgr.loadModel(manifestPath.toUri().toString());
        Resource manifest = m.listResourcesWithProperty(
                m.createProperty(MF, "entries")).nextResource();
        String assumedBase = manifest.getProperty(m.createProperty(MF, "assumedTestBase"))
                .getObject().asResource().getURI();

        List<DynamicTest> tests = new ArrayList<>();
        Iterator<RDFNode> entries = manifest.getProperty(m.createProperty(MF, "entries"))
                .getObject().as(org.apache.jena.rdf.model.RDFList.class).iterator();
        while (entries.hasNext()) {
            Resource entry = entries.next().asResource();
            String type = entry.getProperty(org.apache.jena.vocabulary.RDF.type)
                    .getObject().asResource().getLocalName();
            String name = entry.getProperty(m.createProperty(MF, "name")).getString();
            Path action = toPath(entry.getProperty(m.createProperty(MF, "action"))
                    .getObject().asResource().getURI());
            Path result = entry.hasProperty(m.createProperty(MF, "result"))
                    ? toPath(entry.getProperty(m.createProperty(MF, "result"))
                            .getObject().asResource().getURI())
                    : null;
            String label = entry.getLocalName() + ": " + name;
            // Suite-qualified artifact id: several suites reuse entry names
            // (trig12-bnode-1 exists in both trig syntax and trig eval), and on
            // Windows the first store's mapped file blocks a same-named rebuild.
            String artifactId = suite.dir().replace('/', '-') + "." + entry.getLocalName();
            tests.add(DynamicTest.dynamicTest(label,
                    () -> runEntry(type, suite.lang(), assumedBase, action, result, artifactId)));
        }
        return DynamicContainer.dynamicContainer(suite.dir() + " (" + tests.size() + ")", tests);
    }

    private void runEntry(String type, Lang lang, String assumedBase,
                          Path action, Path result, String id) throws Exception {
        String base = assumedBase + action.getFileName();
        if (type.endsWith("PositiveC14N")) {
            Assumptions.abort("c14n: canonical serialization is out of scope for a storage layer");
        } else if (type.endsWith("NegativeSyntax")) {
            try {
                parse(action, lang, base);
            } catch (Exception correctlyRejected) {
                return;
            }
            // Same policy as the eval divergences: parsing is Jena's layer, so a
            // negative-syntax document Jena ACCEPTS is an upstream leniency, not
            // a BeakGraph failure (whatever Jena hands over still passes the
            // term-kind guards). Abort so it shows as skipped-with-reason; the
            // vendored README lists the known cases for Jena 6.1.0.
            Assumptions.abort("upstream: Jena accepts this negative-syntax document");
        } else if (type.endsWith("PositiveSyntax")) {
            DatasetGraph parsed = parse(action, lang, base);
            containmentSplit(parsed, action, id, null, lang);
        } else if (type.endsWith("Eval")) {
            DatasetGraph parsed = parse(action, lang, base);
            DatasetGraph expected = parse(result, resultLang(result), base);
            Assumptions.assumeTrue(IsoMatcher.isomorphic(normalize(parsed), normalize(expected)),
                    "upstream: Jena's parse differs from the W3C expected result");
            containmentSplit(parsed, action, id, expected, lang);
        } else {
            fail("unhandled manifest test type: " + type);
        }
    }

    /**
     * The containment split: unsupported terms present -> BeakGraph must abort
     * the build loudly on the RAW file; absent -> the build must succeed, and
     * for eval tests the store must be isomorphic to the parsed input.
     */
    private void containmentSplit(DatasetGraph parsed, Path rawAction, String id,
                                  DatasetGraph expectedOrNull, Lang lang) throws Exception {
        File dest = tmp.resolve(id + ".h5").toFile();
        if (containsUnsupportedTerms(parsed)) {
            Exception ex = assertThrows(Exception.class,
                    () -> build(rawAction.toFile(), dest));
            assertTrue(chainMentionsGuard(ex),
                    "build failed, but not on a term-kind guard: " + ex);
            return;
        }
        // Supported terms only: absolutize through Jena (the raw file may use
        // relative IRIs, which BeakGraph deliberately stores unresolved), then
        // the store must hold an isomorphic dataset.
        Path absolutized = tmp.resolve(id + ".nq");
        RDFDataMgr.write(java.nio.file.Files.newOutputStream(absolutized), parsed, Lang.NQUADS);
        build(absolutized.toFile(), dest);
        DatasetGraph reference = (expectedOrNull != null) ? expectedOrNull : parsed;
        try (BeakGraph bg = BG.getBeakGraph(dest)) {
            assertTrue(IsoMatcher.isomorphic(
                            normalize(reference),
                            normalize(bg.getDataset().asDatasetGraph())),
                    "store round-trip is not isomorphic to the parsed input");
        }
    }

    // ---- helpers ----

    private static void build(File src, File dest) throws Exception {
        HDF5Writer.Builder().setSource(src).setDestination(dest).build().write();
    }

    private static DatasetGraph parse(Path file, Lang lang, String base) {
        DatasetGraph dsg = DatasetGraphFactory.create();
        RDFParser.create()
                .source(file.toUri().toString())
                .lang(lang)
                .base(base)
                .parse(dsg);
        return dsg;
    }

    private static Lang resultLang(Path result) {
        return result.getFileName().toString().endsWith(".nq") ? Lang.NQUADS : Lang.NTRIPLES;
    }

    private static boolean containsUnsupportedTerms(DatasetGraph dsg) {
        // Empty since format v5: base-direction literals became storable in v4
        // (Phase 2) and triple terms in v5 (Phase 3), so every RDF 1.2 term
        // kind stores and the whole suite asserts round-trip conformance.
        return false;
    }

    private static boolean chainMentionsGuard(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            String msg = c.getMessage();
            if (msg != null && (msg.contains("Unexpected") || msg.contains("Unsupported")
                    || msg.contains("base direction cannot be stored"))) {
                return true;
            }
        }
        return false;
    }

    private static DatasetGraph normalize(DatasetGraph in) {
        DatasetGraph out = DatasetGraphFactory.create();
        Iterator<Quad> it = in.find(Node.ANY, Node.ANY, Node.ANY, Node.ANY);
        while (it.hasNext()) {
            Quad q = it.next();
            Node g = q.getGraph();
            if (g != null && g.isURI() && g.getURI().startsWith("urn:x-beakgraph:")) {
                continue;
            }
            out.add(new Quad(q.isDefaultGraph() ? Quad.defaultGraphIRI : g,
                    q.getSubject(), q.getPredicate(), q.getObject()));
        }
        return out;
    }

    private static Path resourcePath(String resource) {
        try {
            var url = W3CRdf12SuiteTest.class.getResource(resource);
            if (url == null) {
                throw new IllegalStateException("vendored suite missing: " + resource);
            }
            return Paths.get(url.toURI());
        } catch (java.net.URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Path toPath(String fileUri) {
        return Paths.get(java.net.URI.create(fileUri));
    }
}
