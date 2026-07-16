package com.ebremer.beakgraph.benchmarks;

import com.ebremer.beakgraph.BG;
import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.HTTPSeekableByteChannel;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.riot.out.NodeFmtLib;

/**
 * Paired remote-vs-local query benchmark: opens the SAME BeakGraph twice -
 * once from a local file, once over {@link HTTPSeekableByteChannel} (HTTP
 * range requests) - and runs an identical, seeded battery of random SPARQL
 * queries against both, comparing wall-clock per query type and verifying
 * that both stores return the same number of rows.
 *
 * <p>Deliberately a plain {@code main} program rather than a JMH benchmark:
 * the workload is paired (every query runs against both stores), depends on
 * live network behavior, and the interesting output includes correctness
 * cross-checks and HTTP transfer metrics (bytes fetched, range requests,
 * fraction of the file touched) that JMH's model has no place for. JMH
 * remains the right tool for the micro benches in this module.
 *
 * <p>The battery is built by harvesting real terms from the LOCAL store
 * (graph list plus bounded per-graph samples), so every random query is over
 * things that actually exist. The same seed produces the same query
 * sequence, and each instantiated query object is shared by both stores.
 *
 * <pre>
 * java -cp benchmarks.jar com.ebremer.beakgraph.benchmarks.RemoteVsLocalBench \
 *      [-pairs testpairs.tsv] [-local D:\data\store.h5 -remote https://example.org/store.h5] \
 *      [-tries 20] [-seed 42] [-warmup 2] [-samplegraphs 48] [-cachemb 128] \
 *      [-test graph-dump]
 * </pre>
 *
 * <p>Benchmark targets live in a git-ignored {@code testpairs.tsv} (looked up
 * in the working directory, then {@code benchmarks/}, or wherever {@code
 * -pairs} points): one tab-separated pair per line - the remote http(s) URI
 * and the local file path of the SAME BeakGraph, in either column order;
 * {@code #} starts a comment. Every pair in the file is benchmarked in turn.
 * {@code -local}/{@code -remote} bypass the file for a one-off pair.
 *
 * <p>{@code -test <name>} runs a single battery test by itself (the cold
 * worst-case shots are skipped); an unknown name lists what is available.
 *
 * @author Erich Bremer
 */
public final class RemoteVsLocalBench {

    /** One benchmark target: the same BeakGraph reachable both ways. */
    private record Pair(File local, URI remote) {}

    private record Sample(Node g, Node s, Node p, Node o) {}
    private record Star(Node g, Node s, Node p1, Node o1, Node p2) {}
    private record Numeric(Node g, Node p, double threshold) {}

    /** One query type: a name and a per-try query generator over the harvested pools. */
    private record Test(String name, String what, List<Query> queries) {}

    /** Timings and row counts for one store across the tries of one test. */
    private static final class Series {
        final List<Long> nanos = new ArrayList<>();
        long rows = 0;
    }

    public static void main(String[] args) throws Exception {
        String localPath = null;
        String remoteUrl = null;
        String pairsPath = null;
        int tries = 20;
        long seed = 42;
        int warmup = 2;
        int sampleGraphs = 48;
        int cacheMb = 128;
        String testFilter = null;
        for (int i = 0; i < args.length - 1; i += 2) {
            switch (args[i]) {
                case "-local" -> localPath = args[i + 1];
                case "-remote" -> remoteUrl = args[i + 1];
                case "-pairs" -> pairsPath = args[i + 1];
                case "-tries" -> tries = Integer.parseInt(args[i + 1]);
                case "-seed" -> seed = Long.parseLong(args[i + 1]);
                case "-warmup" -> warmup = Integer.parseInt(args[i + 1]);
                case "-samplegraphs" -> sampleGraphs = Integer.parseInt(args[i + 1]);
                case "-cachemb" -> cacheMb = Integer.parseInt(args[i + 1]);
                case "-test" -> testFilter = args[i + 1];
                default -> throw new IllegalArgumentException("Unknown option: " + args[i]);
            }
        }
        List<Pair> pairs;
        if (localPath != null || remoteUrl != null) {
            if (localPath == null || remoteUrl == null) {
                throw new IllegalArgumentException("-local and -remote must be given together");
            }
            pairs = List.of(new Pair(new File(localPath), URI.create(remoteUrl)));
        } else {
            pairs = loadPairs(pairsPath);
        }
        for (int i = 0; i < pairs.size(); i++) {
            if (i > 0) {
                System.out.println();
                System.out.println("=".repeat(118));
                System.out.println();
            }
            run(pairs.get(i).local(), pairs.get(i).remote(),
                    tries, seed, warmup, sampleGraphs, cacheMb, testFilter);
        }
    }

    /**
     * Loads benchmark targets from a tab-separated pairs file: one pair per
     * line, one field the remote {@code http(s)} URI and the other the local
     * file path (column order free - the http(s) field is recognized as the
     * remote). Blank lines and {@code #} comments are ignored. The file is
     * deliberately NOT in version control (see .gitignore): test targets are
     * deployment-specific, not source.
     */
    private static List<Pair> loadPairs(String explicit) throws java.io.IOException {
        List<java.nio.file.Path> candidates = (explicit != null)
                ? List.of(java.nio.file.Path.of(explicit))
                : List.of(java.nio.file.Path.of("testpairs.tsv"),
                          java.nio.file.Path.of("benchmarks", "testpairs.tsv"));
        java.nio.file.Path file = candidates.stream()
                .filter(java.nio.file.Files::isRegularFile).findFirst().orElse(null);
        if (file == null) {
            throw new IllegalArgumentException("No pairs file found (looked for " + candidates + "). "
                    + "Create testpairs.tsv with one tab-separated pair per line: "
                    + "<https://host/store.h5>\\t<local\\path\\store.h5> ('#' starts a comment), "
                    + "or pass -local/-remote (or -pairs <file>) explicitly.");
        }
        List<Pair> pairs = new ArrayList<>();
        List<String> lines = java.nio.file.Files.readAllLines(file);
        for (int n = 0; n < lines.size(); n++) {
            String line = lines.get(n).trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            String[] fields = line.split("\t");
            String remote = null;
            String local = null;
            for (String raw : fields) {
                String field = raw.trim();
                if (field.isEmpty()) {
                    continue;
                }
                if (field.regionMatches(true, 0, "http://", 0, 7)
                        || field.regionMatches(true, 0, "https://", 0, 8)) {
                    remote = field;
                } else {
                    local = field;
                }
            }
            if (remote == null || local == null) {
                throw new IllegalArgumentException(file + " line " + (n + 1)
                        + ": need one http(s) URI and one local path, tab-separated: " + line);
            }
            pairs.add(new Pair(new File(local), URI.create(remote)));
        }
        if (pairs.isEmpty()) {
            throw new IllegalArgumentException(file + " contains no test pairs");
        }
        System.out.println("targets: " + pairs.size() + " pair(s) from " + file);
        return pairs;
    }

    private static void run(File localFile, URI remoteUri, int tries, long seed, int warmup,
            int sampleGraphs, int cacheMb, String testFilter) throws Exception {
        if (!localFile.isFile()) {
            throw new IllegalArgumentException("Local file not found: " + localFile);
        }
        System.out.println("=== BeakGraph query benchmark: HTTP range requests vs local file ===");
        System.out.printf("local : %s (%,d bytes)%n", localFile, localFile.length());
        System.out.printf("remote: %s%n", remoteUri);
        System.out.printf("seed %d, %d tries/test, %d warmup, %d sample graphs, %d MiB block cache%s%n%n",
                seed, tries, warmup, sampleGraphs, cacheMb,
                (testFilter == null) ? "" : ", single test: " + testFilter);

        // ---- open both stores (timed: "time until queryable") -------------
        long t0 = System.nanoTime();
        BeakGraph local = BG.getBeakGraph(localFile);
        long localOpenNanos = System.nanoTime() - t0;
        Dataset localDS = local.getDataset();

        t0 = System.nanoTime();
        HTTPSeekableByteChannel channel = new HTTPSeekableByteChannel(remoteUri,
                128 * 1024, cacheMb * 8); // 128 KiB blocks -> 8 blocks per MiB
        BeakGraph remote = BG.getBeakGraph(channel);
        long remoteOpenNanos = System.nanoTime() - t0;
        Dataset remoteDS = remote.getDataset();

        long openBytes = channel.getBytesFetched();
        long openRequests = channel.getRangeRequestCount();
        System.out.printf("open: local %s | remote %s (%d range requests, %s fetched)%n%n",
                ms(localOpenNanos), ms(remoteOpenNanos), openRequests, mb(openBytes));
        if (channel.size() != localFile.length()) {
            System.out.printf("WARNING: sizes differ (local %,d, remote %,d) - not the same file?%n",
                    localFile.length(), channel.size());
        }

        try {
            // ---- harvest real terms from a THROWAWAY local reader ---------
            // Materializing terms warms a reader's node cache with exactly the
            // terms the battery will then query. The measured stores must both
            // start cold, so the harvest gets its own instance.
            Random rnd = new Random(seed);
            Pools pools;
            BeakGraph harvestBG = BG.getBeakGraph(localFile);
            try {
                pools = harvest(harvestBG.getDataset(), rnd, sampleGraphs);
            } finally {
                harvestBG.close();
            }
            System.out.printf("harvest: %,d graphs; pools: quads=%d anchors=%d (selective=%d) "
                    + "stars=%d numerics=%d (subjects are %s)%n%n",
                    pools.graphCount, pools.samples.size(),
                    pools.samples.stream().filter(s -> anchorable(s.o())).count(),
                    pools.selectiveAnchors.size(), pools.stars.size(), pools.numerics.size(),
                    pools.iriSubjects ? "IRIs" : "blank nodes");

            // ---- build the battery: identical Query objects for both stores
            List<Test> battery = buildBattery(pools, rnd, tries);
            if (testFilter != null) {
                String available = String.join(", ", battery.stream().map(Test::name).toList());
                String wanted = testFilter;
                battery.removeIf(t -> !t.name().equals(wanted));
                if (battery.isEmpty()) {
                    System.out.println("Unknown or unavailable test '" + testFilter
                            + "'. Available on this store: " + available);
                    System.exit(1);
                }
            }

            // ---- single-shot pathologies, measured COLD (before any warmup)
            // 1. Anchoring on a custom-datatype (WKT) literal: dictionary
            //    location of such a literal is the worst remote access pattern.
            // 2. A variable-predicate fanout from a bound subject executes as a
            //    full-graph scan, so even a 1-match anchor reads the store.
            // Both are real query shapes; they get one measured shot each so
            // they inform without ambushing the battery's statistics.
            // Skipped under -test: a single-test run should measure just that test.
            if (testFilter == null) {
                pools.samples.stream()
                        .filter(s -> s.g() != null && s.o().isLiteral()
                                && s.o().getLiteralLexicalForm().length() >= 200)
                        .findFirst()
                        .ifPresent(s -> singleShot(
                                "worst case 1: anchor on a " + s.o().getLiteralLexicalForm().length()
                                        + "-char custom-datatype literal (cold)",
                                "SELECT ?p2 ?o2 WHERE { GRAPH " + term(s.g()) + " { ?x " + term(s.p())
                                        + " " + term(s.o()) + " . ?x ?p2 ?o2 } }",
                                localDS, remoteDS, channel));
                if (!pools.selectiveAnchors.isEmpty()) {
                    Sample s = pools.selectiveAnchors.get(0);
                    singleShot("worst case 2: variable-predicate fanout join (scan-shaped, cold-ish)",
                            "SELECT ?p2 ?o2 WHERE { " + inGraph(s.g(), "?x " + term(s.p()) + " "
                                    + term(s.o()) + " . ?x ?p2 ?o2") + " } LIMIT 200",
                            localDS, remoteDS, channel);
                }
            }

            // ---- JIT/engine warmup on a disjoint random stream -------------
            long warmupStart = channel.getBytesFetched();
            List<Test> warmupBattery = buildBattery(pools, new Random(seed + 1), Math.max(warmup, 1));
            if (testFilter != null) {
                String wanted = testFilter;
                warmupBattery.removeIf(t -> !t.name().equals(wanted));
            }
            for (int w = 0; w < warmup; w++) {
                for (Test t : warmupBattery) {
                    Query q = t.queries().get(w % t.queries().size());
                    execute(localDS, q);
                    long before = channel.getBytesFetched();
                    long nano = System.nanoTime();
                    execute(remoteDS, q);
                    long delta = channel.getBytesFetched() - before;
                    if (delta > 10_000_000) {
                        // a warmup pick this hungry would ambush the measured runs
                        // too - name it so the battery's bounds can be fixed
                        System.out.printf("warmup outlier %s: %s ms, %s%n  %s%n", t.name(),
                                ms(System.nanoTime() - nano), mb(delta),
                                q.toString().replace('\n', ' '));
                    }
                }
            }
            System.out.printf("warmup: %s fetched%n%n", mb(channel.getBytesFetched() - warmupStart));

            // ---- measured runs --------------------------------------------
            System.out.printf("%-16s %5s %9s | %21s | %21s | %7s %16s%n",
                    "test", "tries", "rows/try", "LOCAL med / p95 / max", "REMOTE med / p95 / max",
                    "ratio", "fetched");
            System.out.println("-".repeat(118));
            long localTotal = 0;
            long remoteTotal = 0;
            long totalQueries = 0;
            int mismatches = 0;
            for (Test test : battery) {
                Series l = new Series();
                Series r = new Series();
                long bytesBefore = channel.getBytesFetched();
                long requestsBefore = channel.getRangeRequestCount();
                for (Query q : test.queries()) {
                    long a = System.nanoTime();
                    long localRows = execute(localDS, q);
                    long b = System.nanoTime();
                    long remoteRows = execute(remoteDS, q);
                    long c = System.nanoTime();
                    l.nanos.add(b - a);
                    r.nanos.add(c - b);
                    l.rows += localRows;
                    r.rows += remoteRows;
                    if (localRows != remoteRows) {
                        mismatches++;
                        System.out.printf("  MISMATCH %s: local %d rows, remote %d rows%n  %s%n",
                                test.name(), localRows, remoteRows, q);
                    }
                }
                long fetched = channel.getBytesFetched() - bytesBefore;
                long requests = channel.getRangeRequestCount() - requestsBefore;
                long lSum = l.nanos.stream().mapToLong(Long::longValue).sum();
                long rSum = r.nanos.stream().mapToLong(Long::longValue).sum();
                localTotal += lSum;
                remoteTotal += rSum;
                totalQueries += test.queries().size();
                System.out.printf("%-16s %5d %9.1f | %6s %6s %6s | %6s %6s %6s | %6.1fx %10s/%dr%n",
                        test.name(), test.queries().size(),
                        (double) l.rows / test.queries().size(),
                        ms(percentile(l.nanos, 50)), ms(percentile(l.nanos, 95)), ms(percentile(l.nanos, 100)),
                        ms(percentile(r.nanos, 50)), ms(percentile(r.nanos, 95)), ms(percentile(r.nanos, 100)),
                        (double) rSum / Math.max(1, lSum), mb(fetched), requests);
            }
            System.out.println("-".repeat(118));

            // ---- overall ---------------------------------------------------
            long bytes = channel.getBytesFetched();
            long requests = channel.getRangeRequestCount();
            System.out.printf("%nTOTALS over %d queries/store:%n", totalQueries);
            System.out.printf("  local  : %s total (%.2f ms/query avg)%n",
                    sec(localTotal), localTotal / 1e6 / totalQueries);
            System.out.printf("  remote : %s total (%.2f ms/query avg) -> %.1fx local%n",
                    sec(remoteTotal), remoteTotal / 1e6 / totalQueries,
                    (double) remoteTotal / Math.max(1, localTotal));
            System.out.printf("  result mismatches: %d%s%n", mismatches,
                    mismatches == 0 ? " (remote answers are identical)" : "  <-- INVESTIGATE");
            System.out.printf("  remote transfer: %s in %,d range requests = %.2f%% of the %s file%n",
                    mb(bytes), requests, 100.0 * bytes / channel.size(), mb(channel.size()));
            System.out.printf("  (open cost included above: %s in %d requests)%n", mb(openBytes), openRequests);
        } finally {
            local.close();
            remote.close();
        }
    }

    // ---------------------------------------------------------------- pools

    private static final class Pools {
        long graphCount;
        boolean iriSubjects;
        boolean hasDefaultGraph;
        final List<Node> graphs = new ArrayList<>();
        final List<Sample> samples = new ArrayList<>();
        final List<Sample> selectiveAnchors = new ArrayList<>();
        final List<Star> stars = new ArrayList<>();
        final List<Numeric> numerics = new ArrayList<>();
    }

    private static final String XSD_NS = "http://www.w3.org/2001/XMLSchema#";
    private static final String LANG_STRING = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString";

    /**
     * A term a battery query may anchor on: concrete, and cheap to locate.
     * Blank nodes are wildcards in patterns; custom-datatype literals
     * (geometry WKT in these stores) make dictionary location itself the
     * dominant cost - measured 700+ MB of transfer for ONE lookup - so that
     * pathology is measured separately as the single-shot worst case rather
     * than randomly ambushing the battery. XSD-typed and plain/lang literals
     * locate in logarithmic probes.
     */
    private static boolean anchorable(Node o) {
        if (o.isBlank()) {
            return false;
        }
        if (o.isURI()) {
            return true;
        }
        String dt = o.getLiteralDatatypeURI();
        boolean standard = dt == null || dt.startsWith(XSD_NS) || dt.equals(LANG_STRING);
        return standard && o.getLiteralLexicalForm().length() <= 64;
    }

    /**
     * Bounded harvest: the full graph list (a dictionary stream - cheap), then
     * a LIMITed slice of random graphs scattered across the store. Never a
     * full scan, so it stays fast on multi-GB files.
     */
    private static Pools harvest(Dataset ds, Random rnd, int sampleGraphs) {
        Pools pools = new Pools();
        List<Node> graphs = pools.graphs;
        Iterator<Node> it = ds.asDatasetGraph().listGraphNodes();
        while (it.hasNext()) {
            Node g = it.next();
            if (!Params.BGVOID.equals(g) && !Params.SPATIAL.equals(g) && g.isURI()) {
                graphs.add(g);
            }
        }
        pools.graphCount = graphs.size();

        List<Node> chosen = new ArrayList<>();
        if (!graphs.isEmpty()) {
            for (int i = 0; i < sampleGraphs; i++) {
                chosen.add(graphs.get(rnd.nextInt(graphs.size())));
            }
        }
        for (Node g : chosen) {
            Query q = QueryFactory.create("SELECT * WHERE { GRAPH " + term(g)
                    + " { ?s ?p ?o } } LIMIT 64");
            try (QueryExecution qe = QueryExecution.dataset(ds).query(q).build()) {
                ResultSet rs = qe.execSelect();
                while (rs.hasNext()) {
                    QuerySolution row = rs.next();
                    pools.samples.add(new Sample(g, row.get("s").asNode(),
                            row.get("p").asNode(), row.get("o").asNode()));
                }
            }
        }
        // default-graph triples participate too, when present
        Query dq = QueryFactory.create("SELECT * WHERE { ?s ?p ?o } LIMIT 64");
        try (QueryExecution qe = QueryExecution.dataset(ds).query(dq).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                QuerySolution row = rs.next();
                pools.hasDefaultGraph = true;
                pools.samples.add(new Sample(null, row.get("s").asNode(),
                        row.get("p").asNode(), row.get("o").asNode()));
            }
        }

        pools.iriSubjects = pools.samples.stream().anyMatch(s -> s.s().isURI());

        // Selective anchors for the JOIN tests: an anchor like "rdf:type X"
        // matches millions of subjects, and a join fanning out from it is a
        // store-wide scan no LIMIT can save (observed: 13 minutes / 3.8 GB
        // over HTTP). Qualify candidates locally - keep only (p,o) pairs
        // matching 1..50 subjects; the LIMIT 51 probe is milliseconds here.
        for (Sample s : pools.samples) {
            if (pools.selectiveAnchors.size() >= 512) {
                break;
            }
            if (!anchorable(s.o())) {
                continue;
            }
            Query probe = QueryFactory.create("SELECT ?x WHERE { "
                    + inGraph(s.g(), "?x " + term(s.p()) + " " + term(s.o())) + " } LIMIT 51");
            long matches = execute(ds, probe);
            if (matches >= 1 && matches <= 50) {
                pools.selectiveAnchors.add(s);
            }
        }

        // star pairs: same (g,s), two distinct predicates -> guaranteed joins
        Map<String, Sample> firstByGS = new HashMap<>();
        for (Sample s : pools.samples) {
            String key = s.g() + "|" + s.s();
            Sample first = firstByGS.putIfAbsent(key, s);
            // The (p1,o1) leg anchors the join, so o1 must be anchorable - a
            // blank node is a wildcard and a huge literal makes the anchor's
            // dictionary lookup the dominant (pathological) cost.
            if (first != null && !first.p().equals(s.p()) && anchorable(first.o())
                    && pools.stars.size() < 4096) {
                pools.stars.add(new Star(s.g(), s.s(), first.p(), first.o(), s.p()));
            }
        }
        // numeric-typed literal objects -> FILTER thresholds that always match >= once
        for (Sample s : pools.samples) {
            if (s.o().isLiteral() && pools.numerics.size() < 4096) {
                try {
                    Object value = s.o().getLiteralValue();
                    if (value instanceof Number n) {
                        pools.numerics.add(new Numeric(s.g(), s.p(), n.doubleValue()));
                    }
                } catch (RuntimeException notNumeric) {
                    // malformed literal - not usable as a threshold
                }
            }
        }
        return pools;
    }

    // -------------------------------------------------------------- battery

    /** The battery: one entry per query type, {@code tries} seeded-random instantiations each. */
    private static List<Test> buildBattery(Pools pools, Random rnd, int tries) {
        List<Test> battery = new ArrayList<>();
        List<Sample> named = pools.samples.stream().filter(s -> s.g() != null).toList();
        // Anchors may come from any graph (in these stores the short-valued
        // feature data lives in the default graph); every anchored test
        // carries a LIMIT so a common value cannot explode a random pick.
        List<Sample> anchors = pools.samples.stream().filter(s -> anchorable(s.o())).toList();

        addTest(battery, "pred-obj", "subjects carrying a known predicate+object (GPOS path)",
                tries, rnd, anchors, s ->
                "SELECT ?s WHERE { " + inGraph(s.g(), "?s " + term(s.p()) + " " + term(s.o()))
                        + " } LIMIT 100");
        addTest(battery, "pred-scan", "bounded scan of one predicate in one graph",
                tries, rnd, pools.samples, s ->
                "SELECT ?s ?o WHERE { " + inGraph(s.g(), "?s " + term(s.p()) + " ?o") + " } LIMIT 100");
        addTest(battery, "obj-lookup", "who points at a known value (object-led)",
                tries, rnd, anchors, s ->
                "SELECT ?s ?p WHERE { " + inGraph(s.g(), "?s ?p " + term(s.o())) + " } LIMIT 100");
        addTest(battery, "cross-graph", "which graphs contain a known predicate (federated discovery)",
                tries, rnd, named, s ->
                "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s " + term(s.p()) + " ?o } } LIMIT 10");
        addTest(battery, "graph-dump", "materialize an entire random named graph (spatial tile fetch)",
                tries, rnd, pools.graphs, g ->
                "SELECT ?s ?p ?o WHERE { GRAPH " + term(g) + " { ?s ?p ?o } }");
        addTest(battery, "star-join", "two-pattern join on a known subject",
                tries, rnd, pools.stars, st ->
                "SELECT ?s ?v WHERE { " + inGraph(st.g(), "?s " + term(st.p1()) + " "
                        + term(st.o1()) + " ; " + term(st.p2()) + " ?v") + " } LIMIT 100");
        addTest(battery, "count-pred", "COUNT(*) of one predicate in one NAMED graph (bounded by graph size)",
                tries, rnd, named, s ->
                "SELECT (COUNT(*) AS ?n) WHERE { GRAPH " + term(s.g()) + " { ?s " + term(s.p()) + " ?o } }");
        addTest(battery, "num-filter", "numeric range filter over a predicate in one graph",
                tries, rnd, pools.numerics, n ->
                "SELECT ?s ?v WHERE { " + inGraph(n.g(), "?s " + term(n.p())
                        + " ?v FILTER(?v >= " + n.threshold() + ")") + " } LIMIT 100");
        if (pools.hasDefaultGraph) {
            // bounded slice of the (potentially huge) default graph - LIMIT is
            // satisfied immediately, so this stays cheap on both stores
            List<Integer> offsets = new ArrayList<>();
            for (int i = 0; i < tries; i++) {
                offsets.add(rnd.nextInt(1000));
            }
            addTest(battery, "default-slice", "bounded slice of the default graph",
                    tries, rnd, offsets, off ->
                    "SELECT * WHERE { ?s ?p ?o } OFFSET " + off + " LIMIT 100");
        }
        return battery;
    }

    /** Wraps a pattern in GRAPH when the sample came from a named graph; default graph otherwise. */
    private static String inGraph(Node g, String pattern) {
        return (g == null) ? pattern : "GRAPH " + term(g) + " { " + pattern + " }";
    }

    private interface Template<T> {
        String instantiate(T pick);
    }

    private static <T> void addTest(List<Test> battery, String name, String what,
            int tries, Random rnd, List<T> pool, Template<T> template) {
        if (pool.isEmpty()) {
            System.out.println("skipping " + name + " (no matching terms in this store): " + what);
            return;
        }
        List<Query> queries = new ArrayList<>(tries);
        for (int i = 0; i < tries; i++) {
            T pick = pool.get(rnd.nextInt(pool.size()));
            queries.add(QueryFactory.create(template.instantiate(pick)));
        }
        battery.add(new Test(name, what, queries));
    }

    /** One paired measurement of a pathological query shape, excluded from the battery totals. */
    private static void singleShot(String label, String sparql, Dataset localDS, Dataset remoteDS,
            HTTPSeekableByteChannel channel) {
        Query q = QueryFactory.create(sparql);
        long bytesBefore = channel.getBytesFetched();
        long requestsBefore = channel.getRangeRequestCount();
        long a = System.nanoTime();
        long localRows = execute(localDS, q);
        long b = System.nanoTime();
        long remoteRows = execute(remoteDS, q);
        long c = System.nanoTime();
        System.out.printf("%s%n  local %s ms (%d rows) | remote %s ms (%d rows), %s in %d requests%n%n",
                label, ms(b - a), localRows, ms(c - b), remoteRows,
                mb(channel.getBytesFetched() - bytesBefore),
                channel.getRangeRequestCount() - requestsBefore);
    }

    /** Fully consumes the query's results; returns the row count. */
    private static long execute(Dataset ds, Query q) {
        try (QueryExecution qe = QueryExecution.dataset(ds).query(q).build()) {
            ResultSet rs = qe.execSelect();
            long rows = 0;
            while (rs.hasNext()) {
                rs.next();
                rows++;
            }
            return rows;
        }
    }

    // ------------------------------------------------------------ reporting

    /**
     * Strict N-Triples serialization: full IRIs, proper escaping, never a
     * prefixed name (the generated queries declare no prefixes).
     */
    private static String term(Node n) {
        return NodeFmtLib.strNT(n);
    }

    private static long percentile(List<Long> nanos, int pct) {
        List<Long> sorted = new ArrayList<>(nanos);
        sorted.sort(Long::compareTo);
        int idx = Math.min(sorted.size() - 1, Math.max(0, (int) Math.ceil(pct / 100.0 * sorted.size()) - 1));
        return sorted.get(idx);
    }

    private static String ms(long nanos) {
        double v = nanos / 1e6;
        return (v >= 100) ? String.format(Locale.ROOT, "%.0f", v)
             : (v >= 10) ? String.format(Locale.ROOT, "%.1f", v)
             : String.format(Locale.ROOT, "%.2f", v);
    }

    private static String sec(long nanos) {
        return String.format(Locale.ROOT, "%.2f s", nanos / 1e9);
    }

    private static String mb(long bytes) {
        return String.format(Locale.ROOT, "%.2f MB", bytes / 1e6);
    }
}
