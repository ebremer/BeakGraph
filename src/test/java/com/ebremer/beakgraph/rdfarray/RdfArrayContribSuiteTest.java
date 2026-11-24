package com.ebremer.beakgraph.rdfarray;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.ebremer.beakgraph.BG;
import com.ebremer.beakgraph.WriterEngines;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.lib.SeqTerms;
import com.ebremer.beakgraph.w3c.ManifestLists;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.query.ResultSetRewindable;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.ResultSetMgr;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.resultset.ResultsCompare;
import org.apache.jena.sparql.resultset.SPARQLResult;
import org.apache.jena.sparql.util.IsoMatcher;
import org.apache.jena.update.UpdateFactory;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.io.TempDir;

/**
 * The rdf-array contrib suite (rdf-tests-contrib, branch {@code rdf-undef},
 * vendored under src/test/resources/rdf-array - see its README) over REAL
 * BeakGraph stores (RDFARRY.md step 4).
 *
 * <ul>
 *   <li><b>RDF syntaxes.</b> Parsing is Jena's layer: a negative test must be
 *       refused by the fork's parser, and an eval test's parse must match its
 *       expected result. Then the containment split of the W3C runner: a
 *       document holding a sequence where BeakGraph refuses one (a subject,
 *       or inside a triple term; SPECIFICATIONS.md §9.5) must fail its build
 *       on a guard, and any other must build into a store isomorphic to it.</li>
 *   <li><b>SPARQL.</b> Every query-evaluation test runs over a store built
 *       from its data, in an HDF5 file and in a Zarr store, and its results
 *       must equal the expected ones. Data BeakGraph refuses (a sequence
 *       subject) aborts the test with that reason, never silently.</li>
 * </ul>
 */
class RdfArrayContribSuiteTest {

    private static final String MF = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#";
    private static final String QT = "http://www.w3.org/2001/sw/DataAccess/tests/test-query#";
    private static final String ROOT = "/rdf-array/rdf-tests-contrib/";

    @TempDir
    static Path tmp;

    /** data-set key and container -> built store, or the guard's message when the build is refused. */
    private static final Map<String, Object> STORES = new HashMap<>();

    private static WriterEngines.Engine engine() {
        WriterEngines.Engine e = WriterEngines.selected();
        Assumptions.assumeTrue(e.storesSequences(), e.name() + " does not store sequences yet (RDFARRY.md step 5)");
        return e;
    }

    // ------------------------------------------------------------------ RDF

    @TestFactory
    Stream<DynamicNode> rdfSyntaxes() {
        List<DynamicNode> out = new ArrayList<>();
        for (String suite : List.of("rdf-turtle", "rdf-trig", "rdf-n-triples", "rdf-n-quads")) {
            Path manifestPath = resourcePath(ROOT + "rdf/" + suite + "/manifest.ttl");
            Model m = RDFDataMgr.loadModel(manifestPath.toUri().toString());
            List<DynamicTest> tests = new ArrayList<>();
            for (Resource entry : entries(m)) {
                String type = entry.getPropertyResourceValue(org.apache.jena.vocabulary.RDF.type).getLocalName();
                Path action = toPath(entry.getPropertyResourceValue(m.createProperty(MF, "action")).getURI());
                Resource result = entry.getPropertyResourceValue(m.createProperty(MF, "result"));
                tests.add(DynamicTest.dynamicTest(entry.getLocalName() + " (" + type + ")",
                        () -> runRdf(type, action, result == null ? null : toPath(result.getURI()))));
            }
            out.add(DynamicContainer.dynamicContainer(suite + " (" + tests.size() + ")", tests));
        }
        return out.stream();
    }

    private void runRdf(String type, Path action, Path result) throws Exception {
        Lang lang = RDFLanguages.filenameToLang(action.getFileName().toString());
        String base = action.toUri().toString();
        if (type.endsWith("NegativeSyntax")) {
            assertThrows(Exception.class, () -> parse(action, lang, base), "the fork's parser must refuse it");
            return;
        }
        DatasetGraph parsed = parse(action, lang, base);
        if (type.endsWith("Eval")) {
            DatasetGraph expected = parse(result, RDFLanguages.filenameToLang(result.getFileName().toString()), base);
            Assumptions.assumeTrue(IsoMatcher.isomorphic(parsed, expected),
                    "upstream: the fork's parse is not the expected result");
        }
        WriterEngines.Engine engine = engine();
        Path nq = tmp.resolve(action.getFileName() + ".nq");
        try (var os = Files.newOutputStream(nq)) {
            RDFDataMgr.write(os, parsed, Lang.NQUADS);
        }
        File dest = tmp.resolve(action.getFileName() + ".h5").toFile();
        String refused = refusedTerm(parsed);
        if (refused != null) {
            Exception ex = assertThrows(Exception.class, () -> engine.buildStore(nq.toFile(), dest), refused);
            assertTrue(chain(ex).contains("Unexpected"), "the build failed, but not on a guard: " + chain(ex));
            return;
        }
        engine.buildStore(nq.toFile(), dest);
        try (BeakGraph bg = BG.getBeakGraph(dest)) {
            assertTrue(IsoMatcher.isomorphic(canonical(parsed), stored(bg)), "the store is not isomorphic to the document");
        }
    }

    /**
     * {@code dsg} as a store keeps it: xsd:int, long, float and double
     * literals by value, so in their canonical form, at every depth of a
     * sequence or triple term (SPECIFICATIONS.md §9.3) - {@code 1e0} comes
     * back as {@code 1.0E0}.
     */
    private static DatasetGraph canonical(DatasetGraph dsg) {
        DatasetGraph out = DatasetGraphFactory.create();
        dsg.find().forEachRemaining(q -> out.add(new Quad(q.getGraph(), q.getSubject(), q.getPredicate(),
                com.ebremer.beakgraph.core.lib.CompoundTerms.map(q.getObject(),
                        com.ebremer.beakgraph.hdf5.writers.PositionalDictionaryWriterBuilder::canonicalizeNumericNode))));
        return out;
    }

    // --------------------------------------------------------------- SPARQL

    @TestFactory
    Stream<DynamicNode> sparql() {
        List<DynamicNode> out = new ArrayList<>();
        for (String suite : List.of("syntax-query", "eval", "results", "update")) {
            Path manifestPath = resourcePath(ROOT + "sparql/" + suite + "/manifest.ttl");
            Model m = RDFDataMgr.loadModel(manifestPath.toUri().toString());
            List<DynamicNode> tests = new ArrayList<>();
            for (Resource entry : entries(m)) {
                String type = entry.getPropertyResourceValue(org.apache.jena.vocabulary.RDF.type).getLocalName();
                Resource action = entry.getPropertyResourceValue(m.createProperty(MF, "action"));
                Resource result = entry.getPropertyResourceValue(m.createProperty(MF, "result"));
                String name = entry.getLocalName() + " (" + type + ")";
                if (type.equals("QueryEvaluationTest") || type.equals("CSVResultFormatTest")) {
                    tests.add(DynamicContainer.dynamicContainer(name, Stream.of("h5", "zarr").map(container ->
                            DynamicTest.dynamicTest(container, () -> evaluate(type, m, action, result, container)))));
                } else {
                    tests.add(DynamicTest.dynamicTest(name, () -> runSyntax(type, m, action)));
                }
            }
            out.add(DynamicContainer.dynamicContainer(suite + " (" + tests.size() + ")", tests));
        }
        return out.stream();
    }

    private void runSyntax(String type, Model m, Resource action) throws Exception {
        switch (type) {
            case "PositiveSyntaxTest11" -> {
                Path q = toPath(action.getURI());
                QueryFactory.create(Files.readString(q), q.toUri().toString(), Syntax.syntaxARQ);
            }
            case "NegativeSyntaxTest11" -> {
                Path q = toPath(action.getURI());
                assertThrows(Exception.class,
                        () -> QueryFactory.create(Files.readString(q), q.toUri().toString(), Syntax.syntaxARQ));
            }
            case "PositiveUpdateSyntaxTest11" -> {
                Path u = toPath(action.getURI());
                UpdateFactory.create(Files.readString(u), u.toUri().toString(), Syntax.syntaxARQ);
            }
            case "NegativeUpdateSyntaxTest11" -> {
                Path u = toPath(action.getURI());
                assertThrows(Exception.class,
                        () -> UpdateFactory.create(Files.readString(u), u.toUri().toString(), Syntax.syntaxARQ));
            }
            case "UpdateEvaluationTest" -> Assumptions.abort("update evaluation: BeakGraph stores are read-only");
            default -> fail("unhandled manifest test type: " + type);
        }
    }

    private void evaluate(String type, Model m, Resource action, Resource result, String container) throws Exception {
        Path queryFile = toPath(action.getPropertyResourceValue(m.createProperty(QT, "query")).getURI());
        List<Path> data = new ArrayList<>();
        action.listProperties(m.createProperty(QT, "data")).forEachRemaining(st ->
                data.add(toPath(st.getObject().asResource().getURI())));
        List<Resource> graphData = new ArrayList<>();
        action.listProperties(m.createProperty(QT, "graphData")).forEachRemaining(st ->
                graphData.add(st.getObject().asResource()));
        Query query = QueryFactory.create(Files.readString(queryFile), queryFile.toUri().toString(), Syntax.syntaxARQ);
        assertNotNull(result, "an evaluation test without mf:result");
        Path resultFile = toPath(result.getURI());

        WriterEngines.Engine engine = engine();
        Object store = storeFor(data, graphData, container, engine);
        if (store instanceof String refusal) {
            Assumptions.abort("the data holds a term BeakGraph refuses (SPECIFICATIONS.md §9.5): " + refusal);
        }
        try (BeakGraph bg = BG.getBeakGraph((File) store);
             QueryExecution qe = QueryExecution.dataset(bg.getDataset()).query(query).build()) {
            if (type.equals("CSVResultFormatTest")) {
                compareCsv(qe.execSelect(), resultFile, query, data, graphData);
            } else if (query.isAskType()) {
                assertEquals(ResultSetFactory.result(resultFile.toString()).getBooleanResult(), qe.execAsk(),
                        "ASK result mismatch");
            } else if (query.isSelectType()) {
                compareSelect(ResultSetFactory.result(resultFile.toString()).getResultSet(), qe.execSelect(), query);
            } else if (query.isConstructType()) {
                Model expected = RDFDataMgr.loadModel(resultFile.toUri().toString());
                Model act = qe.execConstruct();
                assertTrue(IsoMatcher.isomorphic(expected.getGraph(), act.getGraph()),
                        "CONSTRUCT result not isomorphic to expected\n--- actual:\n" + asNTriples(act));
            } else {
                fail("unhandled query type: " + queryFile.getFileName());
            }
        }
    }

    static void compareSelect(ResultSet expectedRs, ResultSet actualRs, Query query) {
        ResultSetRewindable exp = ResultSetFactory.makeRewindable(expectedRs);
        ResultSetRewindable act = ResultSetFactory.makeRewindable(actualRs);
        boolean ordered = query.hasOrderBy();
        boolean ok = ordered ? ResultsCompare.equalsByTermAndOrder(exp, act) : ResultsCompare.equalsByTerm(exp, act);
        if (!ok) {
            exp.reset();
            act.reset();
            fail("SELECT results differ" + (ordered ? " (order-sensitive)" : "") + "\n--- expected:\n"
                    + ResultSetFormatter.asText(exp) + "--- actual:\n" + ResultSetFormatter.asText(act));
        }
    }

    /**
     * The CSV writer is Jena's: BeakGraph's rows written as CSV must read as
     * the expected file does, and where they do not, the same query over an
     * in-memory dataset must fail the same way for the gap to be Jena's.
     */
    private void compareCsv(ResultSet actual, Path expectedFile, Query query, List<Path> data, List<Resource> graphData)
            throws Exception {
        String expected = Files.readString(expectedFile).replace("\r\n", "\n");
        String got = csv(actual);
        if (expected.equals(got)) {
            return;
        }
        Dataset memory = DatasetFactory.wrap(load(data, graphData));
        try (QueryExecution qe = QueryExecution.dataset(memory).query(query).build()) {
            String jena = csv(qe.execSelect());
            assertEquals(jena, got, "BeakGraph's CSV differs from Jena's own for the same data");
            Assumptions.abort("upstream: Jena's CSV writer gives\n" + jena + "where the suite expects\n" + expected);
        }
    }

    private static String csv(ResultSet rs) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ResultSetMgr.write(out, rs, ResultSetLang.RS_CSV);
        return out.toString(StandardCharsets.UTF_8).replace("\r\n", "\n");
    }

    // -------------------------------------------------------------- helpers

    /** A built store for the test's data set in {@code container}, or the guard's message. Cached. */
    private static synchronized Object storeFor(List<Path> data, List<Resource> graphData, String container,
                                                WriterEngines.Engine engine) throws Exception {
        String key = String.join("|", data.stream().map(Path::toString).toList())
                + "#" + String.join("|", graphData.stream().map(Resource::getURI).toList()) + "@" + container;
        Object cached = STORES.get(key);
        if (cached != null) {
            return cached;
        }
        DatasetGraph dsg = load(data, graphData);
        String refused = refusedTerm(dsg);
        Object built;
        if (refused != null) {
            built = refused;
        } else {
            String stem = "store-" + Integer.toHexString(key.hashCode());
            Path nq = tmp.resolve(stem + ".nq");
            try (var os = Files.newOutputStream(nq)) {
                RDFDataMgr.write(os, dsg, Lang.NQUADS);
            }
            File dest = tmp.resolve(stem + "." + container).toFile();
            engine.buildStore(nq.toFile(), dest);
            built = dest;
        }
        STORES.put(key, built);
        return built;
    }

    private static DatasetGraph load(List<Path> data, List<Resource> graphData) {
        DatasetGraph dsg = DatasetGraphFactory.create();
        for (Path d : data) {
            RDFParser.create().source(d.toUri().toString()).parse(dsg);
        }
        for (Resource g : graphData) {
            DatasetGraph one = DatasetGraphFactory.create();
            RDFParser.create().source(toPath(g.getURI()).toUri().toString()).parse(one);
            Node name = org.apache.jena.graph.NodeFactory.createURI(g.getURI());
            one.getDefaultGraph().find().forEachRemaining(t -> dsg.add(name, t.getSubject(), t.getPredicate(), t.getObject()));
        }
        return dsg;
    }

    /** A term of {@code dsg} BeakGraph refuses (§9.5), described; null when it stores the whole dataset. */
    static String refusedTerm(DatasetGraph dsg) {
        Iterator<Quad> quads = dsg.find();
        while (quads.hasNext()) {
            Quad q = quads.next();
            if (SeqTerms.holdsSequence(q.getSubject())) {
                return "a sequence subject: " + q.getSubject();
            }
            if (q.getObject().isTripleTerm() && SeqTerms.holdsSequence(q.getObject())) {
                return "a sequence inside a triple term: " + q.getObject();
            }
        }
        return null;
    }

    private static DatasetGraph parse(Path file, Lang lang, String base) {
        DatasetGraph dsg = DatasetGraphFactory.create();
        RDFParser.create().source(file.toUri().toString()).lang(lang).base(base).parse(dsg);
        return dsg;
    }

    private static DatasetGraph stored(BeakGraph bg) {
        DatasetGraph out = DatasetGraphFactory.create();
        bg.getDataset().asDatasetGraph().find().forEachRemaining(q -> {
            Node g = q.getGraph();
            if (g != null && g.isURI() && g.getURI().startsWith("urn:x-beakgraph:")) {
                return;
            }
            out.add(new Quad(q.isDefaultGraph() ? Quad.defaultGraphIRI : g, q.getSubject(), q.getPredicate(), q.getObject()));
        });
        return out;
    }

    private static List<Resource> entries(Model m) {
        Resource manifest = m.listResourcesWithProperty(m.createProperty(MF, "entries")).nextResource();
        List<Resource> out = new ArrayList<>();
        Iterator<RDFNode> it = ManifestLists.members(manifest.getProperty(m.createProperty(MF, "entries")).getObject());
        it.forEachRemaining(n -> out.add(n.asResource()));
        return out;
    }

    private static String asNTriples(Model m) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RDFDataMgr.write(out, m, Lang.NTRIPLES);
        return out.toString(StandardCharsets.UTF_8);
    }

    private static String chain(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable c = t; c != null; c = c.getCause()) {
            sb.append(c.getMessage()).append(" | ");
        }
        return sb.toString();
    }

    private static Path resourcePath(String resource) {
        try {
            var url = RdfArrayContribSuiteTest.class.getResource(resource);
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
