package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.lib.NodeComparator;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * NodeComparator must be a strict total order - every sort and binary search in
 * the store depends on it. NodeValue.compareAlways alone is NOT one: for
 * XSD-indeterminate pairs (a timezone-less dateTime vs a timezoned one within
 * +/-14h, a month-based duration vs a day-based one) it falls back to term
 * order pairwise, which mixes two different orders and creates comparison
 * cycles. Before the fix, a file with ordinary mixed local/UTC timestamps
 * failed to build ("Cannot resolve Object (not in dictionary)") or, worse,
 * built and then silently dropped rows on lookup.
 */
class TemporalTotalOrderTest {

    private static final NodeComparator CMP = NodeComparator.INSTANCE;

    private static Node dt(String lex)  { return NodeFactory.createLiteralDT(lex, XSDDatatype.XSDdateTime); }
    private static Node dur(String lex) { return NodeFactory.createLiteralDT(lex, XSDDatatype.XSDduration); }

    /** The reproduced 3-cycle: naive < +14:00 < Z < naive under the old comparator. */
    @Test
    void reportedDateTimeCycleIsGone() {
        assertTotallyOrdered(List.of(
                dt("2020-01-02T00:00:00"),
                dt("2020-01-02T08:00:00+14:00"),
                dt("2020-01-01T20:00:00Z")));
    }

    @Test
    void durationCycleIsGone() {
        // P1Y < P13M by value, but P13M and P15D (and P15D and P1Y) were
        // month-vs-day indeterminate and fell back to term order: a cycle.
        assertTotallyOrdered(List.of(dur("P1Y"), dur("P13M"), dur("P15D")));
    }

    @Test
    void comparatorIsTotalOverMixedLiteralSpaces() {
        List<Node> nodes = mixedPool();
        assertTotallyOrdered(nodes);

        // The property that actually broke: sort, then find EVERY element again.
        Node[] sorted = nodes.toArray(Node[]::new);
        Arrays.sort(sorted, CMP);
        for (Node n : nodes) {
            assertTrue(Arrays.binarySearch(sorted, n, CMP) >= 0,
                    "binary search must find every sorted element, missed: " + n);
        }
    }

    /** BG-437: a total order sorts the same whatever the input order. */
    @Test
    void shuffledSortsAgreeAcrossSeeds() {
        List<Node> pool = mixedPool();
        List<Node> canonical = new ArrayList<>(pool);
        canonical.sort(CMP);
        for (int seed = 0; seed < 12; seed++) {
            List<Node> shuffled = new ArrayList<>(pool);
            java.util.Collections.shuffle(shuffled, new java.util.Random(seed));
            shuffled.sort(CMP);
            assertEquals(canonical, shuffled, "seed " + seed + ": sort result depends on input order");
        }
    }

    /**
     * BG-437 candidates: the two comparator regions that were actually cyclic
     * (g* kinds overlapping the year of dateTimes that straddle the +/-14h
     * window; numerics within float rounding distance of each other) are
     * present in the pool, so the transitivity sweep can see them.
     */
    private static List<Node> mixedPool() {
        List<Node> nodes = new ArrayList<>();
        nodes.add(g("2020", XSDDatatype.XSDgYear));
        nodes.add(g("2020Z", XSDDatatype.XSDgYear));
        nodes.add(g("2020-12Z", XSDDatatype.XSDgYearMonth));
        nodes.add(g("2020-12", XSDDatatype.XSDgYearMonth));
        nodes.add(g("2021-01-01", XSDDatatype.XSDdate));
        nodes.add(dt("2020-01-01T00:30:00+01:00"));
        nodes.add(dt("2019-12-31T23:45:00Z"));
        nodes.add(dt("2021-01-01T05:00:00+14:00"));
        nodes.add(dt("2020-12-31T20:00:00Z"));
        nodes.add(dt("2020-12-31T23:59:59"));
        for (String[] n : new String[][]{
                {"16777219", "integer"}, {"16777217", "integer"}, {"1.677722E7", "float"}, {"1.6777216E7", "float"},
                {"1.67772195E7", "double"}, {"0.10", "decimal"}, {"0.1", "float"}, {"0.100000001", "double"},
                {"0.1000000015", "decimal"}, {"0.10000000149011612", "decimal"}}) {
            XSDDatatype t = switch (n[1]) {
                case "integer" -> XSDDatatype.XSDinteger;
                case "float" -> XSDDatatype.XSDfloat;
                case "double" -> XSDDatatype.XSDdouble;
                default -> XSDDatatype.XSDdecimal;
            };
            nodes.add(NodeFactory.createLiteralDT(n[0], t));
        }
        // dateTimes: naive, UTC, and offsets all within each other's +/-14h windows
        for (int i = 0; i < 8; i++) {
            nodes.add(dt(String.format("2020-01-0%dT0%d:00:00", 1 + i % 3, i)));
            nodes.add(dt(String.format("2020-01-0%dT0%d:30:00Z", 1 + i % 3, i)));
            nodes.add(dt(String.format("2020-01-0%dT0%d:15:00+0%d:00", 1 + i % 3, i, 1 + i % 9)));
            nodes.add(dt(String.format("2020-01-0%dT0%d:45:00-0%d:30", 1 + i % 3, i, 1 + i % 9)));
        }
        // instant-equal, term-distinct dateTimes must order deterministically
        nodes.add(dt("2020-06-01T12:00:00Z"));
        nodes.add(dt("2020-06-01T12:00:00+00:00"));
        nodes.add(dt("2020-06-01T14:00:00+02:00"));
        // times and dates with and without timezones
        for (int i = 0; i < 4; i++) {
            nodes.add(NodeFactory.createLiteralDT(String.format("0%d:00:00", 2 * i), XSDDatatype.XSDtime));
            nodes.add(NodeFactory.createLiteralDT(String.format("0%d:00:00+1%d:00", 2 * i, i), XSDDatatype.XSDtime));
            nodes.add(NodeFactory.createLiteralDT(String.format("2020-01-1%d", i), XSDDatatype.XSDdate));
            nodes.add(NodeFactory.createLiteralDT(String.format("2020-01-1%d+14:00", i), XSDDatatype.XSDdate));
            nodes.add(NodeFactory.createLiteralDT(String.format("201%d", i), XSDDatatype.XSDgYear));
            nodes.add(NodeFactory.createLiteralDT(String.format("201%dZ", i), XSDDatatype.XSDgYear));
        }
        // durations: month-based, day/time-based, mixed, value-equal pair (P1D/PT24H)
        for (String d : new String[]{"P1Y", "P13M", "P15D", "P1M", "P100D", "P2M", "P30D",
                "P1M10D", "PT24H", "P1D", "PT36H", "P400D", "P1Y1D", "-P1M", "-P20D"}) {
            nodes.add(dur(d));
        }
        // neighbors from other value spaces (space-ranked by compareAlways)
        nodes.add(NodeFactory.createLiteralDT("2020-01-02T04:00:00", XSDDatatype.XSDstring));
        nodes.add(NodeFactory.createLiteralDT("zzz", XSDDatatype.XSDstring));
        nodes.add(NodeFactory.createLiteralDT("42", XSDDatatype.XSDinteger));
        nodes.add(NodeFactory.createLiteralDT("41.5", XSDDatatype.XSDdouble));
        nodes.add(NodeFactory.createLiteralDT("not-a-date", XSDDatatype.XSDdateTime)); // ill-formed
        // Jena 6 removed createLiteral(String); createLiteralString is the
        // equivalent (a plain literal IS an xsd:string in RDF 1.1).
        nodes.add(NodeFactory.createLiteralString("plain"));
        return nodes;
    }

    /**
     * BG-437 end to end: the formerly cyclic literals in one store. Every row
     * round-trips, and every stored object term is found again by exact
     * lookup (floats and doubles are canonicalized at write time, so the
     * store's own spelling is used; the term-exact kinds are also looked up
     * by their source spelling).
     */
    @Test
    void cyclicRegionLiteralsRoundTripAndAreFoundAgain() throws Exception {
        String[] lits = {
            "\"2020\"^^xsd:gYear", "\"2020Z\"^^xsd:gYear", "\"2020-12Z\"^^xsd:gYearMonth", "\"2020-12\"^^xsd:gYearMonth",
            "\"2020-01-01T00:30:00+01:00\"^^xsd:dateTime", "\"2019-12-31T23:45:00Z\"^^xsd:dateTime",
            "\"2021-01-01T05:00:00+14:00\"^^xsd:dateTime", "\"2020-12-31T20:00:00Z\"^^xsd:dateTime",
            "\"16777219\"^^xsd:integer", "\"16777217\"^^xsd:integer", "\"1.677722E7\"^^xsd:float", "\"1.6777216E7\"^^xsd:float",
            "\"1.67772195E7\"^^xsd:double", "\"0.10\"^^xsd:decimal", "\"0.1\"^^xsd:float", "\"0.100000001\"^^xsd:double",
            "\"0.1000000015\"^^xsd:decimal", "\"P1Y\"^^xsd:duration", "\"P400D\"^^xsd:duration", "\"42\"^^xsd:integer",
        };
        StringBuilder ttl = new StringBuilder("@prefix ex: <http://ex.org/> .\n@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n");
        for (int i = 0; i < lits.length; i++) {
            ttl.append("ex:s").append(i).append(" ex:v ").append(lits[i]).append(" .\n");
        }
        File src = dir.resolve("cyclic.ttl").toFile();
        File h5 = dir.resolve("cyclic.ttl.h5").toFile();
        Files.write(src.toPath(), ttl.toString().getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setSource(src).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        String pre = "PREFIX ex: <http://ex.org/> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ";
        try (BeakGraph bg = new BeakGraph(new HDF5Reader(h5))) {
            Dataset ds = bg.getDataset();
            assertEquals(lits.length, countRows(ds, pre + "SELECT ?s ?o WHERE { ?s ex:v ?o }"));
            int looked = 0;
            try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(pre + "SELECT ?s ?o WHERE { ?s ex:v ?o }")).build()) {
                ResultSet rs = qe.execSelect();
                while (rs.hasNext()) {
                    org.apache.jena.query.QuerySolution qs = rs.next();
                    String term = org.apache.jena.sparql.util.FmtUtils.stringForNode(qs.get("o").asNode());
                    assertEquals(1, countRows(ds, pre + "SELECT ?s WHERE { ?s ex:v " + term + " }"), "stored term not found again: " + term);
                    looked++;
                }
            }
            assertEquals(lits.length, looked);
            for (String lit : lits) {
                if (lit.contains("xsd:float") || lit.contains("xsd:double")) continue;   // canonicalized spellings
                assertEquals(1, countRows(ds, pre + "SELECT ?s WHERE { ?s ex:v " + lit + " }"), "source spelling not found: " + lit);
            }
            // Range filters over the mixed pool agree with the value order Jena uses.
            assertEquals(4, countRows(ds, pre + "SELECT ?s WHERE { ?s ex:v ?o FILTER(?o > 16777216 && ?o < 16777300) }"));
        }
    }

    private static void assertTotallyOrdered(List<Node> nodes) {
        int n = nodes.size();
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                int ij = Integer.signum(CMP.compare(nodes.get(i), nodes.get(j)));
                int ji = Integer.signum(CMP.compare(nodes.get(j), nodes.get(i)));
                assertEquals(-ji, ij, "antisymmetry violated for " + nodes.get(i) + " / " + nodes.get(j));
                if (i == j) continue;
                for (int k = 0; k < n; k++) {
                    int jk = Integer.signum(CMP.compare(nodes.get(j), nodes.get(k)));
                    int ik = Integer.signum(CMP.compare(nodes.get(i), nodes.get(k)));
                    if (ij < 0 && jk < 0) {
                        assertTrue(ik < 0, "transitivity violated: " + nodes.get(i) + " < "
                                + nodes.get(j) + " < " + nodes.get(k) + " but not i < k");
                    }
                }
            }
        }
    }

    @TempDir
    Path dir;

    /**
     * End-to-end: the exact failure mode - hundreds of mixed timezone-less and
     * timezoned dateTimes in one file. Under the cyclic comparator this build
     * threw "Cannot resolve Object (not in dictionary)" at index construction.
     */
    @Test
    void mixedTimezoneDateTimesRoundTrip() throws Exception {
        int count = 300;
        StringBuilder ttl = new StringBuilder("@prefix ex: <http://ex.org/> .\n"
                + "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n");
        for (int i = 0; i < count; i++) {
            int day = 1 + (i / 24) % 27;
            int hour = i % 24;
            int minute = (i * 7) % 60;
            String lex = String.format("2020-01-%02dT%02d:%02d:00", day, hour, minute);
            String form = switch (i % 3) {
                case 0 -> lex;
                case 1 -> lex + "Z";
                default -> lex + String.format("%s%02d:00", (i % 2 == 0 ? "+" : "-"), 1 + i % 13);
            };
            ttl.append(String.format("ex:s%d ex:when \"%s\"^^xsd:dateTime .%n", i, form));
        }

        File src = dir.resolve("mixedtz.ttl").toFile();
        File h5 = dir.resolve("mixedtz.ttl.h5").toFile();
        Files.write(src.toPath(), ttl.toString().getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setSource(src).setDestination(h5)
                .setSpatial(false).setFeatures(false).build().write();

        try (BeakGraph bg = new BeakGraph(new HDF5Reader(h5))) {
            Dataset ds = bg.getDataset();
            assertEquals(count, countRows(ds,
                    "PREFIX ex: <http://ex.org/> SELECT ?s ?o WHERE { ?s ex:when ?o }"),
                    "every mixed-timezone dateTime row must round-trip");
            // concrete-literal lookups exercise the read-side binary search on both forms
            assertEquals(1, countRows(ds, "PREFIX ex: <http://ex.org/> "
                    + "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> "
                    + "SELECT ?s WHERE { ?s ex:when \"2020-01-01T00:00:00\"^^xsd:dateTime }"));
            assertEquals(1, countRows(ds, "PREFIX ex: <http://ex.org/> "
                    + "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> "
                    + "SELECT ?s WHERE { ?s ex:when \"2020-01-01T01:07:00Z\"^^xsd:dateTime }"));
        }
    }

    @Test
    void mixedDurationsRoundTrip() throws Exception {
        String[] durations = {"P1Y", "P13M", "P15D", "P1M", "P100D", "P2M", "P30D", "P28D",
                "P1M10D", "PT24H", "P1D", "PT36H", "P400D", "P1Y1D", "P9M", "P276D",
                "-P1M", "-P20D", "P3D", "P20D", "PT240H", "P10D", "P1M9D", "P38D"};
        StringBuilder ttl = new StringBuilder("@prefix ex: <http://ex.org/> .\n"
                + "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n");
        for (int i = 0; i < durations.length; i++) {
            ttl.append(String.format("ex:d%d ex:lasts \"%s\"^^xsd:duration .%n", i, durations[i]));
        }

        File src = dir.resolve("durations.ttl").toFile();
        File h5 = dir.resolve("durations.ttl.h5").toFile();
        Files.write(src.toPath(), ttl.toString().getBytes(StandardCharsets.UTF_8));
        HDF5Writer.Builder().setSource(src).setDestination(h5)
                .setSpatial(false).setFeatures(false).build().write();

        try (BeakGraph bg = new BeakGraph(new HDF5Reader(h5))) {
            Dataset ds = bg.getDataset();
            assertEquals(durations.length, countRows(ds,
                    "PREFIX ex: <http://ex.org/> SELECT ?s ?o WHERE { ?s ex:lasts ?o }"));
            assertEquals(1, countRows(ds, "PREFIX ex: <http://ex.org/> "
                    + "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> "
                    + "SELECT ?s WHERE { ?s ex:lasts \"P13M\"^^xsd:duration }"));
        }
    }

    private static int countRows(Dataset ds, String query) {
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(query)).build()) {
            ResultSet rs = qe.execSelect();
            int rows = 0;
            while (rs.hasNext()) {
                rs.next();
                rows++;
            }
            return rows;
        }
    }

    // --- BG-434: dateTime and the g* kinds share ONE ARQ value space ---------

    private static Node g(String lex, XSDDatatype dt) { return NodeFactory.createLiteralDT(lex, dt); }

    @Test
    void crossKindInstantPairsAreTotallyOrdered() {
        // temporalGroup gave gYear its own group, so a gYear-vs-dateTime pair
        // went to compareAlways, where ARQ's shared DATETIME space made it
        // "not comparable" and fell back to LEXICAL order - while same-kind
        // pairs ordered as instants. Lexically "2020-06-01T…" > "2020" and
        // "1999-12-31T…" < "2020"; as instants 1999 < 2020 < 2020-06: mixing
        // the two orders across a third kind cycled.
        assertTotallyOrdered(List.of(
                dt("2020-06-01T00:00:00Z"),
                g("2020", XSDDatatype.XSDgYear),
                dt("1999-12-31T00:00:00Z"),
                g("2021", XSDDatatype.XSDgYear),
                g("2020-06", XSDDatatype.XSDgYearMonth),
                g("--06", XSDDatatype.XSDgMonth),
                g("--06-15", XSDDatatype.XSDgMonthDay),
                g("---15", XSDDatatype.XSDgDay),
                g("2020", XSDDatatype.XSDgYear).equals(null) ? null : dt("2020-06-01T00:00:00+05:00"),
                dt("2020-06-01T00:00:00")));
        // The documented kind rank holds across the space.
        assertTrue(CMP.compare(g("---15", XSDDatatype.XSDgDay), g("--06", XSDDatatype.XSDgMonth)) < 0);
        assertTrue(CMP.compare(g("--06", XSDDatatype.XSDgMonth), g("--06-15", XSDDatatype.XSDgMonthDay)) < 0);
        assertTrue(CMP.compare(g("--06-15", XSDDatatype.XSDgMonthDay), g("2020", XSDDatatype.XSDgYear)) < 0);
        assertTrue(CMP.compare(g("2020", XSDDatatype.XSDgYear), g("2020-06", XSDDatatype.XSDgYearMonth)) < 0);
        assertTrue(CMP.compare(g("2020-06", XSDDatatype.XSDgYearMonth), dt("1999-12-31T00:00:00Z")) < 0);
        assertTrue(CMP.compare(g("2020", XSDDatatype.XSDgYear), g("2021", XSDDatatype.XSDgYear)) < 0, "same kind: by instant");
    }
}
