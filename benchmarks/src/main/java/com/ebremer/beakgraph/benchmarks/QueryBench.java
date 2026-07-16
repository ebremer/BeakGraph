package com.ebremer.beakgraph.benchmarks;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.RDFNode;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * End-to-end SPARQL over a BeakGraph store - the numbers that ultimately matter.
 * Each query shape targets a distinct read-path mechanism:
 *
 * <ul>
 *   <li>{@code pointLookup} - bound S+P (GSPO iterator construction + one row).</li>
 *   <li>{@code starJoin} - three patterns on one subject (per-pattern iterator
 *       construction against the same binding).</li>
 *   <li>{@code predicateScan} - bound P only (GPOS nested object/subject scan,
 *       one row per subject).</li>
 *   <li>{@code rangeFilter} - FILTER range pushdown into the object id range.</li>
 *   <li>{@code chainJoin} - selective anchor then two joins: each intermediate
 *       binding constructs fresh iterators and re-resolves pattern terms, the
 *       per-binding cost identified in the read-path review.</li>
 *   <li>{@code graphVarScan} - GRAPH ?g: per-named-graph chaining.</li>
 *   <li>{@code fullScanFind} - Graph API wildcard scan (SPO_All) including
 *       Triple materialization through the node table.</li>
 * </ul>
 *
 * <p>Every query drains its ResultSet and touches each projected term, so lazy
 * bindings actually materialize - the timings include dictionary extraction.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgsAppend = {
        "--enable-native-access=ALL-UNNAMED",
        "--sun-misc-unsafe-memory-access=allow",
        "-Xmx4g"})
@State(Scope.Benchmark)
public class QueryBench {

    private static final String PREFIX = "PREFIX ex: <" + SyntheticStore.NS + ">\n";

    @Param({"50000"})
    public int subjects;

    private BeakGraph graph;
    private Dataset dataset;
    private Query pointLookup;
    private Query starJoin;
    private Query predicateScan;
    private Query rangeFilter;
    private Query chainJoin;
    private Query graphVarScan;
    private Query distinctSubjects;
    private Query distinctObjects;
    private Query fullScanSparql;

    @Setup
    public void setup() {
        graph = new BeakGraph(new HDF5Reader(SyntheticStore.get(subjects).toFile()));
        dataset = graph.getDataset();
        int mid = subjects / 2;
        pointLookup = QueryFactory.create(PREFIX
                + "SELECT ?o WHERE { ex:s" + mid + " ex:link ?o }");
        starJoin = QueryFactory.create(PREFIX
                + "SELECT ?v ?n ?t WHERE { ex:s" + mid + " ex:value ?v ; ex:name ?n ; ex:link ?t }");
        predicateScan = QueryFactory.create(PREFIX
                + "SELECT ?s ?v WHERE { ?s ex:value ?v }");
        rangeFilter = QueryFactory.create(PREFIX
                + "SELECT ?s ?v WHERE { ?s ex:value ?v FILTER(?v >= 995) }");
        chainJoin = QueryFactory.create(PREFIX
                + "SELECT ?a ?c WHERE { ?a ex:value 7 . ?a ex:link ?b . ?b ex:link ?c }");
        graphVarScan = QueryFactory.create(PREFIX
                + "SELECT ?g ?s WHERE { GRAPH ?g { ?s ex:tag ?t } }");
        // Index-answered (GSPO subject-level stream) vs scan+dedup fallback: the
        // same row count, so the pair isolates the DISTINCT fast path's effect.
        distinctSubjects = QueryFactory.create(PREFIX
                + "SELECT DISTINCT ?s WHERE { ?s ?p ?o }");
        distinctObjects = QueryFactory.create(PREFIX
                + "SELECT DISTINCT ?o WHERE { ?s ?p ?o }");
        // Engine-path full scan (unlike fullScanFind's Graph API): eligible for
        // the chunked parallel scan; A/B via -Dbeakgraph.scan.parallel.threshold.
        fullScanSparql = QueryFactory.create(PREFIX
                + "SELECT ?s ?p ?o WHERE { ?s ?p ?o }");
    }

    @TearDown
    public void tearDown() {
        graph.close();
    }

    /** Runs the query and touches every projected term so results fully materialize. */
    private long run(Query q) {
        long h = 0;
        try (QueryExecution qe = QueryExecution.dataset(dataset).query(q).build()) {
            ResultSet rs = qe.execSelect();
            List<String> vars = rs.getResultVars();
            while (rs.hasNext()) {
                QuerySolution row = rs.next();
                for (String v : vars) {
                    RDFNode n = row.get(v);
                    if (n != null) {
                        h += n.asNode().hashCode();
                    }
                }
            }
        }
        return h;
    }

    @Benchmark
    public long pointLookup() {
        return run(pointLookup);
    }

    @Benchmark
    public long starJoin() {
        return run(starJoin);
    }

    @Benchmark
    public long predicateScan() {
        return run(predicateScan);
    }

    @Benchmark
    public long rangeFilter() {
        return run(rangeFilter);
    }

    @Benchmark
    public long chainJoin() {
        return run(chainJoin);
    }

    @Benchmark
    public long graphVarScan() {
        return run(graphVarScan);
    }

    @Benchmark
    public long distinctSubjects() {
        return run(distinctSubjects);
    }

    @Benchmark
    public long distinctObjects() {
        return run(distinctObjects);
    }

    @Benchmark
    public long fullScanSparql() {
        return run(fullScanSparql);
    }

    @Benchmark
    public long fullScanFind() {
        long h = 0;
        var it = graph.find(Node.ANY, Node.ANY, Node.ANY);
        try {
            while (it.hasNext()) {
                h += it.next().hashCode();
            }
        } finally {
            it.close();
        }
        return h;
    }
}
