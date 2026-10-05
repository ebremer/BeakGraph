package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.cmdline.BeakGraphCLI;
import com.ebremer.beakgraph.cmdline.Parameters;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.VoidMode;
import com.ebremer.beakgraph.hdf5.jena.AggregateCountFastPath;
import com.ebremer.beakgraph.hdf5.jena.DistinctTermFastPath;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import com.ebremer.beakgraph.pool.BeakGraphPoolFactory;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.commons.pool2.PooledObject;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.DatasetGraph;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * BG-348: a store built from an empty source (no dictionary sections, index
 * groups whose component buffers may be zero-entry or absent) reaches every
 * read entry point through guard chains that CdtHardeningTest's four
 * SELECT/ASK shapes never pinned: the COUNT and DISTINCT fast paths
 * (IndexCounts / DistinctTermFastPath on a -1 graph id), Graph.size, the
 * dataset's graph list and containsGraph, the pool's validation probe, and
 * -export. Both VoID modes are covered: NONE (no graphs at all) and EXACT
 * (a VoID graph describing nothing).
 */
class EmptyStoreEntryPointsTest {

    @TempDir
    static Path dir;

    static Stream<VoidMode> modes() {
        return Stream.of(VoidMode.NONE, VoidMode.EXACT);
    }

    private static File build(VoidMode mode) throws Exception {
        Path src = dir.resolve("empty-" + mode + ".ttl");
        Files.writeString(src, "");
        File dest = dir.resolve("empty-" + mode + ".h5").toFile();
        if (!dest.exists()) {
            HDF5Writer.Builder().setSource(src.toFile()).setDestination(dest).setVoidMode(mode).build().write();
        }
        return dest;
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

    @ParameterizedTest(name = "{0}")
    @MethodSource("modes")
    void countAndDistinctFastPathsAnswerZeroFromTheIndex(VoidMode mode) throws Exception {
        try (BeakGraph bg = BG.getBeakGraph(build(mode))) {
            Dataset ds = bg.getDataset();
            for (String body : new String[]{"?s ?p ?o", "GRAPH <http://ex.org/nope> { ?s ?p ?o }"}) {
                for (String agg : new String[]{"COUNT(*)", "COUNT(DISTINCT ?s)", "COUNT(DISTINCT ?p)"}) {
                    String q = "SELECT (" + agg + " AS ?n) WHERE { " + body + " }";
                    long before = AggregateCountFastPath.HITS.get();
                    assertEquals(0, count(ds, q), q);
                    assertEquals(1, AggregateCountFastPath.HITS.get() - before,
                            "the index-side fast path must answer (not a scan fallback): " + q);
                }
                // COUNT(DISTINCT ?o) has no index level (it scans on every store): the scan must answer 0 too.
                assertEquals(0, count(ds, "SELECT (COUNT(DISTINCT ?o) AS ?n) WHERE { " + body + " }"));
            }
            for (String var : new String[]{"?s", "?p"}) {
                String q = "SELECT DISTINCT " + var + " WHERE { ?s ?p ?o }";
                long before = DistinctTermFastPath.HITS.get();
                assertEquals(0, rows(ds, q), q);
                assertEquals(1, DistinctTermFastPath.HITS.get() - before, "DISTINCT fast path must answer: " + q);
            }
            assertEquals(0, rows(ds, "SELECT DISTINCT ?o WHERE { ?s ?p ?o }"), "objects have no index level: the scan answers");
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("modes")
    void sizesGraphListAndContainsGraph(VoidMode mode) throws Exception {
        try (BeakGraph bg = BG.getBeakGraph(build(mode))) {
            Dataset ds = bg.getDataset();
            DatasetGraph dsg = ds.asDatasetGraph();
            assertEquals(0, ds.getDefaultModel().size());
            assertEquals(0, ((BeakGraph) dsg.getDefaultGraph()).size(), "graphBaseSize -> IndexCounts.quads");
            assertTrue(dsg.getDefaultGraph().isEmpty());
            assertFalse(dsg.containsGraph(NodeFactory.createURI("http://ex.org/x")));
            List<Node> graphs = new ArrayList<>();
            dsg.listGraphNodes().forEachRemaining(graphs::add);
            Node voidGraph = NodeFactory.createURI(Params.VOIDSTRING);
            if (mode == VoidMode.NONE) {
                assertEquals(List.of(), graphs, "no graphs at all without VoID");
                assertFalse(dsg.containsGraph(voidGraph));
            } else {
                assertEquals(List.of(voidGraph), graphs, "EXACT statistics describe the empty store in the VoID graph");
                assertTrue(dsg.containsGraph(voidGraph));
                assertTrue(count(ds, "SELECT (COUNT(*) AS ?n) WHERE { GRAPH <" + Params.VOIDSTRING + "> { ?s ?p ?o } }") > 0);
            }
            assertEquals(0, rows(ds, "SELECT * WHERE { ?s ?p ?o }"));
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("modes")
    void poolValidationAcceptsAnEmptyStore(VoidMode mode) throws Exception {
        File h5 = build(mode);
        BeakGraphPoolFactory factory = new BeakGraphPoolFactory();
        BeakGraph bg = factory.create(h5.toURI());
        try {
            PooledObject<BeakGraph> pooled = factory.wrap(bg);
            // Covers the getNumberOfNodes() == 0 branch: nothing to probe, still valid.
            assertTrue(factory.validateObject(h5.toURI(), pooled), "an empty store is a valid pooled reader");
        } finally {
            bg.close();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("modes")
    void exportsProduceParseableEmptyFiles(VoidMode mode) throws Exception {
        File h5 = build(mode);
        Path out = Files.createDirectories(dir.resolve("export-" + mode));
        File copy = out.resolve("empty.h5").toFile();
        Files.copy(h5.toPath(), copy.toPath());
        for (String fmt : new String[]{"NT", "TTL", "NQ", "TRIG"}) {
            Parameters p = new Parameters();
            p.src = copy;
            p.export = fmt;
            BeakGraphCLI cli = new BeakGraphCLI(p);
            cli.export();
            assertEquals(0, cli.getFileCounter().getFailedConversionFileCount(), "export " + fmt + " must succeed");
            Path file = out.resolve("empty." + fmt.toLowerCase(java.util.Locale.ROOT));
            assertTrue(Files.exists(file), "export must write " + file);
            Dataset parsed = RDFDataMgr.loadDataset(file.toString());
            assertEquals(0, parsed.getDefaultModel().size(), fmt + " export must hold no triples");
            assertFalse(parsed.listNames().hasNext(), fmt + " export must hold no named graphs (VoID is internal)");
        }
    }
}
