package com.ebremer.beakgraph.cdt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.ebremer.beakgraph.BG;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.lib.CdtTerms;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.query.ResultSetRewindable;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.resultset.ResultsCompare;
import org.apache.jena.sparql.resultset.SPARQLResult;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.io.TempDir;

/**
 * Manifest-driven runner for the vendored SPARQL-CDTs test suite
 * (src/test/resources/sparql-cdts - see its README for provenance): 658
 * mf:QueryEvaluationTest entries, each executed over a REAL BeakGraph store
 * built from the test's data file (stores cached per distinct data file).
 *
 * <p>Two policy carve-outs, both asserted rather than ignored:
 * <ul>
 *   <li>Tests whose DATA contains blank nodes inside composite literals assert
 *       BeakGraph's documented reject-at-ingest guard instead of evaluating -
 *       BeakGraph regenerates blank-node labels from dictionary rank, so the
 *       spec's import co-reference (its section 5.2) cannot be preserved.</li>
 *   <li>Queries using SERVICE abort: they need a remote endpoint.</li>
 * </ul>
 */
class SparqlCdtSuiteTest {

    private static final String MF = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#";
    private static final String QT = "http://www.w3.org/2001/sw/DataAccess/tests/test-query#";

    @TempDir
    static Path tmp;

    /** data file -> built store; empty Optional = rejected by the CDT blank-node policy. */
    private static final Map<String, Optional<File>> STORES = new HashMap<>();

    @TestFactory
    Stream<DynamicNode> sparqlCdts() {
        Path root = resourcePath("/sparql-cdts/tests/manifest-all.ttl");
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
            String name = entry.getProperty(m.createProperty(MF, "name")).getString();
            Resource action = entry.getProperty(m.createProperty(MF, "action"))
                    .getObject().asResource();
            Path query = toPath(action.getProperty(m.createProperty(QT, "query"))
                    .getObject().asResource().getURI());
            // A test's dataset is either one-or-more qt:data files, or (the
            // bnodes export tests) a qt:constructDataFile: a CONSTRUCT query
            // whose serialization, in the given format, becomes the data.
            List<Path> data = new ArrayList<>();
            action.listProperties(m.createProperty(QT, "data")).forEachRemaining(st ->
                    data.add(toPath(st.getObject().asResource().getURI())));
            Resource constructData = action.hasProperty(m.createProperty(QT, "constructDataFile"))
                    ? action.getProperty(m.createProperty(QT, "constructDataFile")).getObject().asResource()
                    : null;
            Path constructQuery = (constructData != null)
                    ? toPath(constructData.getProperty(m.createProperty(QT, "query"))
                            .getObject().asResource().getURI())
                    : null;
            String constructFormat = (constructData != null)
                    ? constructData.getProperty(m.createProperty(QT, "format")).getString()
                    : null;
            Path result = toPath(entry.getProperty(m.createProperty(MF, "result"))
                    .getObject().asResource().getURI());
            tests.add(DynamicTest.dynamicTest(name,
                    () -> runEntry(query, data, constructQuery, constructFormat, result)));
        }
        String label = manifestPath.getParent().getFileName() + " (" + tests.size() + ")";
        return DynamicContainer.dynamicContainer(label, tests);
    }

    private void runEntry(Path queryFile, List<Path> dataFiles, Path constructQuery,
                          String constructFormat, Path resultFile) throws Exception {
        String queryText = Files.readString(queryFile);
        if (queryText.matches("(?s).*\\bSERVICE\\b.*")) {
            org.junit.jupiter.api.Assumptions.abort("requires a remote SPARQL endpoint (SERVICE)");
        }
        if (constructQuery != null) {
            // Materialize the constructed dataset exactly as the manifest asks
            // (run the CONSTRUCT, serialize in the requested format), then treat
            // it as ordinary data - if it carries blank nodes inside composite
            // literals (these tests exist to check exactly that export scoping),
            // it lands in the policy-rejection branch below like any other data.
            dataFiles = List.of(materializeConstructedData(constructQuery, constructFormat));
        }

        Optional<File> store = storeFor(dataFiles);
        if (store.isEmpty()) {
            // Data carries a blank node inside a composite literal: BeakGraph's
            // documented policy is loud rejection at ingest (the co-reference the
            // spec requires cannot survive rank-derived relabeling). The build
            // failure IS the expected behavior; assert it fired on the guard.
            List<Path> rejected = dataFiles;
            Exception ex = assertThrows(Exception.class, () -> build(rejected));
            assertTrue(chainMentions(ex, "blank node inside cdt: composite literal"),
                    "build failed, but not on the CDT blank-node guard: " + ex);
            return;
        }

        Query query = QueryFactory.create(queryText, queryFile.toUri().toString());
        SPARQLResult expected = ResultSetFactory.result(resultFile.toString());
        try (BeakGraph bg = BG.getBeakGraph(store.get());
             QueryExecution qe = QueryExecution.dataset(bg.getDataset()).query(query).build()) {
            if (query.isAskType()) {
                assertEquals(expected.getBooleanResult(), qe.execAsk(),
                        "ASK result mismatch");
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
                assertTrue(act.isIsomorphicWith(expected.getModel()),
                        "CONSTRUCT result not isomorphic to expected");
            } else {
                fail("unhandled query type in test suite: " + queryFile.getFileName());
            }
        }
    }

    private static synchronized Optional<File> storeFor(List<Path> dataFiles) throws Exception {
        String key = dataFiles.isEmpty() ? "<none>"
                : String.join("|", dataFiles.stream().map(Path::toString).toList());
        Optional<File> cached = STORES.get(key);
        if (cached != null) {
            return cached;
        }
        Optional<File> value;
        if (dataFiles.isEmpty()) {
            Path empty = tmp.resolve("no-data.ttl");
            if (!Files.exists(empty)) {
                Files.writeString(empty, "");
            }
            value = Optional.of(build(List.of(empty)));
        } else if (dataFiles.stream().anyMatch(SparqlCdtSuiteTest::dataContainsCdtBlankNode)) {
            value = Optional.empty();
        } else {
            value = Optional.of(build(dataFiles));
        }
        STORES.put(key, value);
        return value;
    }

    private static boolean dataContainsCdtBlankNode(Path dataFile) {
        Model m = RDFDataMgr.loadModel(dataFile.toUri().toString());
        var it = m.listStatements();
        while (it.hasNext()) {
            if (CdtTerms.containsBlankNode(it.nextStatement().getObject().asNode())) {
                return true;
            }
        }
        return false;
    }

    private static File build(List<Path> srcs) throws Exception {
        String stem = srcs.get(0).getFileName().toString().replaceAll("[^A-Za-z0-9.-]", "_");
        File dest = tmp.resolve(stem + "-" + Math.abs(String.join("|",
                srcs.stream().map(Path::toString).toList()).hashCode()) + ".h5").toFile();
        HDF5Writer.Builder b = HDF5Writer.Builder().setDestination(dest);
        if (srcs.size() == 1) {
            b.setSource(srcs.get(0).toFile());
        } else {
            // Multi-file tests check cross-document blank-node scoping; the merge
            // path keeps blank nodes distinct per source document, as they require.
            b.setSources(srcs.stream().map(Path::toFile).toList());
        }
        b.build().write();
        return dest;
    }

    private Path materializeConstructedData(Path constructQuery, String format) throws Exception {
        Query q = QueryFactory.create(Files.readString(constructQuery),
                constructQuery.toUri().toString());
        Model constructed;
        try (QueryExecution qe = QueryExecution.dataset(
                org.apache.jena.query.DatasetFactory.create()).query(q).build()) {
            constructed = qe.execConstruct();
        }
        boolean turtle = format.contains("turtle");
        Path out = tmp.resolve("constructed-" + Math.abs(constructQuery.hashCode())
                + (turtle ? ".ttl" : ".nt"));
        try (var os = Files.newOutputStream(out)) {
            RDFDataMgr.write(os, constructed,
                    turtle ? org.apache.jena.riot.Lang.TURTLE : org.apache.jena.riot.Lang.NTRIPLES);
        }
        return out;
    }

    private static boolean chainMentions(Throwable t, String marker) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c.getMessage() != null && c.getMessage().contains(marker)) {
                return true;
            }
        }
        return false;
    }

    private static Path resourcePath(String resource) {
        try {
            var url = SparqlCdtSuiteTest.class.getResource(resource);
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
