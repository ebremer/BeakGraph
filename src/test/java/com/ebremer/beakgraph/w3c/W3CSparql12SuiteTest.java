package com.ebremer.beakgraph.w3c;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import com.ebremer.beakgraph.BG;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.WriterEngines;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.query.ResultSetRewindable;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.resultset.ResultsCompare;
import org.apache.jena.sparql.resultset.SPARQLResult;
import org.apache.jena.update.UpdateFactory;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.io.TempDir;

/**
 * Manifest-driven runner for the vendored W3C SPARQL 1.2 test suites
 * (src/test/resources/w3c/sparql12 - see its README for provenance and the
 * abort policies). Query-evaluation entries execute over REAL BeakGraph
 * stores built from each test's data files (stores cached per data set);
 * syntax entries are grammar checks; update evaluation aborts (read-only
 * store), update syntax still parses.
 */
class W3CSparql12SuiteTest {

    private static final String MF = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#";
    private static final String QT = "http://www.w3.org/2001/sw/DataAccess/tests/test-query#";

    @TempDir
    static Path tmp;

    /** data-set key -> built store. */
    private static final Map<String, File> STORES = new HashMap<>();

    @TestFactory
    Stream<DynamicNode> w3cSparql12() {
        Path root = resourcePath("/w3c/sparql12/manifest.ttl");
        Model top = RDFDataMgr.loadModel(root.toUri().toString());
        Resource manifest = top.listResourcesWithProperty(
                top.createProperty(MF, "include")).nextResource();
        List<DynamicNode> containers = new ArrayList<>();
        Iterator<RDFNode> includes = manifest.getProperty(top.createProperty(MF, "include"))
                .getObject().as(org.apache.jena.rdf.model.RDFList.class).iterator();
        while (includes.hasNext()) {
            containers.add(subManifest(toPath(includes.next().asResource().getURI())));
        }
        return containers.stream();
    }

    private DynamicContainer subManifest(Path manifestPath) {
        Model m = RDFDataMgr.loadModel(manifestPath.toUri().toString());
        Resource manifest = m.listResourcesWithProperty(
                m.createProperty(MF, "entries")).nextResource();
        List<DynamicTest> tests = new ArrayList<>();
        Iterator<RDFNode> entries = manifest.getProperty(m.createProperty(MF, "entries"))
                .getObject().as(org.apache.jena.rdf.model.RDFList.class).iterator();
        while (entries.hasNext()) {
            Resource entry = entries.next().asResource();
            String type = entry.getProperty(org.apache.jena.vocabulary.RDF.type)
                    .getObject().asResource().getLocalName();
            String name = entry.hasProperty(m.createProperty(MF, "name"))
                    ? entry.getProperty(m.createProperty(MF, "name")).getString()
                    : entry.getLocalName();
            Resource action = entry.getProperty(m.createProperty(MF, "action"))
                    .getObject().asResource();
            Resource result = entry.hasProperty(m.createProperty(MF, "result"))
                    ? entry.getProperty(m.createProperty(MF, "result")).getObject().asResource()
                    : null;
            tests.add(DynamicTest.dynamicTest(entry.getLocalName() + ": " + name,
                    () -> runEntry(type, m, action, result)));
        }
        String label = manifestPath.getParent().getFileName() + " (" + tests.size() + ")";
        return DynamicContainer.dynamicContainer(label, tests);
    }

    private void runEntry(String type, Model m, Resource action, Resource result) throws Exception {
        switch (type) {
            case "PositiveSyntaxTest" -> positiveQuerySyntax(queryFileOf(m, action));
            case "NegativeSyntaxTest" -> negativeQuerySyntax(queryFileOf(m, action));
            case "PositiveUpdateSyntaxTest" -> positiveUpdateSyntax(queryFileOf(m, action));
            case "NegativeUpdateSyntaxTest" -> negativeUpdateSyntax(queryFileOf(m, action));
            case "QueryEvaluationTest" -> evaluate(m, action, result);
            case "UpdateEvaluationTest" ->
                    Assumptions.abort("update evaluation: BeakGraph stores are read-only");
            default -> fail("unhandled manifest test type: " + type);
        }
    }

    /** Syntax-test actions are the query file directly; eval actions nest qt:query. */
    private Path queryFileOf(Model m, Resource action) {
        if (action.hasProperty(m.createProperty(QT, "query"))) {
            return toPath(action.getProperty(m.createProperty(QT, "query"))
                    .getObject().asResource().getURI());
        }
        return toPath(action.getURI());
    }

    private void positiveQuerySyntax(Path queryFile) throws Exception {
        String text = Files.readString(queryFile);
        String base = queryFile.toUri().toString();
        try {
            QueryFactory.create(text, base, Syntax.syntaxARQ);
            return;
        } catch (RuntimeException arqRejects) {
            try {
                QueryFactory.create(text, base, Syntax.syntaxSPARQL_12);
            } catch (RuntimeException bothReject) {
                // Parsing is Jena's layer: a W3C-positive query BOTH parsers
                // reject is an upstream gap (e.g. the SPARQL 1.2 GROUP BY
                // scoping relaxation), mirroring the RDF suite's policy of
                // aborting on Jena divergences rather than failing.
                Assumptions.abort("upstream: Jena rejects this positive-syntax query: " + bothReject.getMessage());
            }
            // See the vendored README: the endpoint deliberately runs syntaxARQ
            // (syntaxSPARQL_12 would delete the CDT UNFOLD/FOLD surface).
            Assumptions.abort("parses under syntaxSPARQL_12 only; BeakGraph's endpoint deliberately runs syntaxARQ ");
        }
    }

    private void negativeQuerySyntax(Path queryFile) throws Exception {
        String text = Files.readString(queryFile);
        try {
            QueryFactory.create(text, queryFile.toUri().toString(), Syntax.syntaxSPARQL_12);
        } catch (RuntimeException correctlyRejected) {
            return;
        }
        Assumptions.abort("upstream: Jena's syntaxSPARQL_12 parser accepts this negative-syntax query");
    }

    private void positiveUpdateSyntax(Path updateFile) throws Exception {
        // Grammar conformance only - the store never executes updates.
        UpdateFactory.create(Files.readString(updateFile), updateFile.toUri().toString(),
                Syntax.syntaxSPARQL_12);
    }

    private void negativeUpdateSyntax(Path updateFile) throws Exception {
        String text = Files.readString(updateFile);
        try {
            UpdateFactory.create(text, updateFile.toUri().toString(), Syntax.syntaxSPARQL_12);
        } catch (RuntimeException correctlyRejected) {
            return;
        }
        Assumptions.abort("upstream: Jena's syntaxSPARQL_12 parser accepts this negative-syntax update");
    }

    private void evaluate(Model m, Resource action, Resource result) throws Exception {
        Path queryFile = toPath(action.getProperty(m.createProperty(QT, "query"))
                .getObject().asResource().getURI());
        List<Path> data = new ArrayList<>();
        action.listProperties(m.createProperty(QT, "data")).forEachRemaining(st ->
                data.add(toPath(st.getObject().asResource().getURI())));
        List<Resource> graphData = new ArrayList<>();
        action.listProperties(m.createProperty(QT, "graphData")).forEachRemaining(st ->
                graphData.add(st.getObject().asResource()));

        String queryText = Files.readString(queryFile);
        Query query;
        try {
            query = QueryFactory.create(queryText, queryFile.toUri().toString(), Syntax.syntaxARQ);
        } catch (RuntimeException arqRejects) {
            QueryFactory.create(queryText, queryFile.toUri().toString(), Syntax.syntaxSPARQL_12);
            Assumptions.abort("parses under syntaxSPARQL_12 only; BeakGraph's endpoint deliberately runs syntaxARQ ");
            return;
        }

        assertNotNull(result, "QueryEvaluationTest without mf:result: " + action.getURI());
        File store = storeFor(data, graphData, m);
        SPARQLResult expected = ResultSetFactory.result(toPath(result.getURI()).toString());
        try (BeakGraph bg = BG.getBeakGraph(store);
             QueryExecution qe = QueryExecution.dataset(bg.getDataset()).query(query).build()) {
            if (query.isAskType()) {
                assertEquals(expected.getBooleanResult(), qe.execAsk(), "ASK result mismatch");
            } else if (query.isSelectType()) {
                ResultSetRewindable act = ResultSetFactory.makeRewindable(qe.execSelect());
                ResultSetRewindable exp = ResultSetFactory.makeRewindable(expected.getResultSet());
                boolean ordered = query.hasOrderBy();
                boolean ok = ordered
                        ? ResultsCompare.equalsByTermAndOrder(exp, act)
                        : ResultsCompare.equalsByTerm(exp, act);
                if (!ok) {
                    exp.reset();
                    act.reset();
                    ok = ordered
                            ? ResultsCompare.equalsByValueAndOrder(exp, act)
                            : ResultsCompare.equalsByValue(exp, act);
                }
                if (!ok) {
                    exp.reset();
                    act.reset();
                    fail("SELECT results differ" + (ordered ? " (order-sensitive)" : "") + "\n--- expected:\n"
                            + org.apache.jena.query.ResultSetFormatter.asText(exp)
                            + "--- actual:\n"
                            + org.apache.jena.query.ResultSetFormatter.asText(act));
                }
            } else if (query.isConstructType()) {
                Model act = qe.execConstruct();
                // IsoMatcher, not Model.isIsomorphicWith: the legacy GraphMatcher
                // treats triple terms as ground terms, so blank nodes INSIDE them
                // (which these suites use, co-referring across statements) never
                // join the bijection and truly-isomorphic results compare false.
                assertTrue(org.apache.jena.sparql.util.IsoMatcher.isomorphic(
                                expected.getModel().getGraph(), act.getGraph()),
                        "CONSTRUCT result not isomorphic to expected");
            } else {
                fail("unhandled query type: " + queryFile.getFileName());
            }
        }
    }

    /**
     * A BeakGraph store for the test's dataset: qt:data files parse into one
     * dataset (TriG/N-Quads carry their named graphs), qt:graphData files into
     * the named graph of their resource URI (the DAWG convention); the
     * assembled dataset serializes to N-Quads and builds through the standard
     * writer. Cached per distinct data set.
     */
    private static synchronized File storeFor(List<Path> data, List<Resource> graphData, Model m)
            throws Exception {
        String key = String.join("|", data.stream().map(Path::toString).toList())
                + "#" + String.join("|", graphData.stream().map(Resource::getURI).toList());
        File cached = STORES.get(key);
        if (cached != null) {
            return cached;
        }
        DatasetGraph dsg = DatasetGraphFactory.create();
        for (Path d : data) {
            RDFParser.create().source(d.toUri().toString()).parse(dsg);
        }
        for (Resource g : graphData) {
            DatasetGraph one = DatasetGraphFactory.create();
            RDFParser.create().source(toPath(g.getURI()).toUri().toString()).parse(one);
            org.apache.jena.graph.Node name = org.apache.jena.graph.NodeFactory.createURI(g.getURI());
            one.getDefaultGraph().find().forEachRemaining(t ->
                    dsg.add(name, t.getSubject(), t.getPredicate(), t.getObject()));
        }
        Path nq = tmp.resolve("data-" + Math.abs(key.hashCode()) + ".nq");
        try (var os = Files.newOutputStream(nq)) {
            RDFDataMgr.write(os, dsg, Lang.NQUADS);
        }
        File dest = tmp.resolve("store-" + Math.abs(key.hashCode()) + ".h5").toFile();
        // The engine named by -Dbeakgraph.test.engine (method 0 by default).
        WriterEngines.Engine engine = WriterEngines.selected();
        engine.assumeAvailable();
        engine.buildStore(nq.toFile(), dest);
        STORES.put(key, dest);
        return dest;
    }

    private static Path resourcePath(String resource) {
        try {
            var url = W3CSparql12SuiteTest.class.getResource(resource);
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
