package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * BG-331: differential test of range-filter pushdown against Jena's in-memory
 * dataset - the oracle that would have caught the earlier pushdown findings.
 * One store holds, under distinct predicates, every value space a constant
 * can come from: numbers of mixed datatypes, plain strings, language-tagged
 * and base-direction literals, dateTimes with and without timezone,
 * durations (month- and day-based, indeterminate pairs), composite (cdt:)
 * literals, booleans, an ill-typed literal, IRIs and triple terms. Every
 * stored term is used as the constant, on either side of every ordering
 * operator, on all four index routes: SO ({@code :s :p ?o}), POS
 * ({@code ?s :p ?o}), SPO_All ({@code ?s ?p ?o}) and OS ({@code ?s :p :o}
 * with a subject constant). BeakGraph must answer exactly what Jena answers.
 */
class RangePushdownParityTest {

    private static final String CDT = "http://w3id.org/awslabs/neptune/SPARQL-CDTs/";
    private static final String PREFIX = "PREFIX ex: <http://ex.org/> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> PREFIX cdt: <" + CDT + "> ";

    /** predicate -> the literal spellings under it (each also attached to the group's multi-valued subject). */
    private static final String[][] GROUPS = {
        {"num", "\"5\"^^xsd:int", "7", "\"5.5\"^^xsd:double", "\"-3\"^^xsd:int", "\"abc\"^^xsd:int", "\"5.0\"^^xsd:float", "\"1.0E300\"^^xsd:double", "\"0.1\"^^xsd:decimal"},
        {"str", "\"apple\"", "\"banana\"", "\"Apple\"", "\"\"", "\"apple\"^^xsd:string"},
        {"lang", "\"m\"@en", "\"n\"@en", "\"a\"@fr", "\"m\"@en-GB"},
        {"dl", "\"m\"@en--ltr", "\"m\"@en--rtl", "\"n\"@en--ltr", "\"m\"@en"},
        {"when", "\"2020-01-01T00:00:00Z\"^^xsd:dateTime", "\"2020-01-01T00:00:00\"^^xsd:dateTime", "\"2020-01-01T10:00:00+14:00\"^^xsd:dateTime", "\"2021-06-01T00:00:00Z\"^^xsd:dateTime", "\"2020-01-01\"^^xsd:date", "\"2020\"^^xsd:gYear"},
        {"lasts", "\"P1Y\"^^xsd:duration", "\"P1M\"^^xsd:duration", "\"P45D\"^^xsd:duration", "\"P400D\"^^xsd:duration", "\"PT24H\"^^xsd:duration"},
        {"list", "\"[2]\"^^cdt:List", "\"[9]\"^^cdt:List", "\"[10]\"^^cdt:List", "\"[1, 2]\"^^cdt:List", "\"[1,2]\"^^cdt:List"},
        {"map", "\"{\\\"a\\\": 1}\"^^cdt:Map", "\"{\\\"b\\\": 1}\"^^cdt:Map", "\"{\\\"a\\\":1}\"^^cdt:Map"},
        {"flag", "true", "false"},
        {"ref", "ex:a", "ex:b", "ex:c"},
        {"tt", "<<( ex:a ex:b ex:c )>>", "<<( ex:a ex:b ex:d )>>", "<<( ex:a ex:b \"5\"^^xsd:int )>>"},
    };

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;
    static Dataset reference;

    private static String fixture() {
        StringBuilder ttl = new StringBuilder("@prefix ex: <http://ex.org/> .\n@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n@prefix cdt: <" + CDT + "> .\n");
        for (String[] group : GROUPS) {
            String pred = group[0];
            for (int i = 1; i < group.length; i++) {
                ttl.append("ex:").append(pred).append(i).append(" ex:").append(pred).append(' ').append(group[i]).append(" .\n");
                ttl.append("ex:").append(pred).append("m ex:").append(pred).append(' ').append(group[i]).append(" .\n");
            }
        }
        return ttl.toString();
    }

    @BeforeAll
    static void build() throws Exception {
        String ttl = fixture();
        File src = dir.resolve("pushdown.ttl").toFile();
        Files.writeString(src.toPath(), ttl, StandardCharsets.UTF_8);
        File h5 = dir.resolve("pushdown.h5").toFile();
        HDF5Writer.Builder().setSource(src).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        bg = new BeakGraph(new HDF5Reader(h5));
        ds = bg.getDataset();
        DatasetGraph dsg = DatasetGraphFactory.create();
        RDFParser.create().source(src.toPath().toUri().toString()).lang(Lang.TURTLE).parse(dsg);
        reference = DatasetFactory.wrap(dsg);
    }

    @AfterAll
    static void close() {
        if (bg != null) bg.close();
    }

    private static List<String> rows(Dataset d, String query) {
        List<String> out = new ArrayList<>();
        try (QueryExecution qe = QueryExecution.dataset(d).query(QueryFactory.create(query)).build()) {
            ResultSet rs = qe.execSelect();
            while (rs.hasNext()) {
                QuerySolution qs = rs.next();
                StringBuilder sb = new StringBuilder();
                for (String v : rs.getResultVars()) sb.append(v).append('=').append(qs.get(v)).append(' ');
                out.add(sb.toString());
            }
        }
        Collections.sort(out);
        return out;
    }

    private static int compared;
    private static int nonEmpty;

    private static void assertSame(String query) {
        List<String> expected = rows(reference, query);
        assertEquals(expected, rows(ds, query), query);
        compared++;
        if (!expected.isEmpty()) nonEmpty++;
    }

    @Test
    void everyConstantOperatorAndRouteMatchesJena() {
        String[] ops = {"<", "<=", ">", ">=", "=", "!="};
        for (String[] group : GROUPS) {
            String pred = group[0];
            for (int i = 1; i < group.length; i++) {
                String c = group[i];
                for (String op : ops) {
                    for (String filter : new String[]{"?o " + op + " " + c, c + " " + op + " ?o"}) {
                        // SO: bound subject, bound predicate, variable object (multi-valued subject)
                        assertSame(PREFIX + "SELECT ?o WHERE { ex:" + pred + "m ex:" + pred + " ?o FILTER(" + filter + ") }");
                        // POS: bound predicate
                        assertSame(PREFIX + "SELECT ?s ?o WHERE { ?s ex:" + pred + " ?o FILTER(" + filter + ") }");
                        // SPO_All: full scan, the constant meets every value space in the store
                        assertSame(PREFIX + "SELECT ?s ?p ?o WHERE { ?s ?p ?o FILTER(" + filter + ") }");
                    }
                }
            }
        }
        // OS: bound predicate and object, a range on the SUBJECT (IRIs, and an absent IRI)
        for (String c : new String[]{"ex:ref1", "ex:ref2", "ex:refm", "ex:absent", "ex:zzz"}) {
            for (String op : ops) {
                for (String filter : new String[]{"?s " + op + " " + c, c + " " + op + " ?s"}) {
                    assertSame(PREFIX + "SELECT ?s WHERE { ?s ex:ref ex:b FILTER(" + filter + ") }");
                    assertSame(PREFIX + "SELECT ?s WHERE { ?s ?p ex:b FILTER(" + filter + ") }");
                }
            }
        }
        // Absent constants inside and outside every range.
        for (String c : new String[]{"6", "\"4.5\"^^xsd:double", "\"zzz\"", "\"\"@en", "\"2020-06-01T00:00:00Z\"^^xsd:dateTime",
                "\"P20D\"^^xsd:duration", "\"[5]\"^^cdt:List", "\"{}\"^^cdt:Map", "ex:absent", "<<( ex:a ex:b ex:zzz )>>"}) {
            for (String op : ops) {
                assertSame(PREFIX + "SELECT ?s ?o WHERE { ?s ?p ?o FILTER(?o " + op + " " + c + ") }");
            }
        }
        assertTrue(compared > 1500, "queries compared: " + compared);
        assertTrue(nonEmpty > 300, "the comparison is not vacuous: " + nonEmpty + " of " + compared + " queries had rows");
    }

    @Test
    void conjunctionsAndDisjunctionsOfRangesMatchJena() {
        for (String q : new String[]{
                "SELECT ?s ?o WHERE { ?s ex:num ?o FILTER(?o > 0 && ?o < 6) }",
                "SELECT ?s ?o WHERE { ?s ex:num ?o FILTER(?o >= \"5\"^^xsd:int && ?o <= \"5.0\"^^xsd:float) }",
                "SELECT ?s ?o WHERE { ?s ex:num ?o FILTER(?o < 0 || ?o > 6) }",
                "SELECT ?s ?o WHERE { ?s ?p ?o FILTER(?o > \"a\" && ?o < \"c\") }",
                "SELECT ?s ?o WHERE { ?s ex:when ?o FILTER(?o >= \"2020-01-01T00:00:00Z\"^^xsd:dateTime && ?o < \"2021-01-01T00:00:00Z\"^^xsd:dateTime) }",
                "SELECT ?s ?o WHERE { ?s ex:list ?o FILTER(?o > \"[1]\"^^cdt:List && ?o < \"[9]\"^^cdt:List) }",
                "SELECT ?s ?o WHERE { ?s ex:lang ?o FILTER(?o >= \"m\"@en && lang(?o) = \"en\") }",
                "SELECT ?s ?o WHERE { ?s ex:num ?o FILTER(?o > 4) FILTER(?o < 6) }",
                "SELECT ?s ?o WHERE { ?s ex:num ?o FILTER(!(?o > 4)) }",
                "SELECT ?s ?o WHERE { ?s ex:num ?o FILTER(?o > 4) } ORDER BY ?o ?s LIMIT 2"}) {
            assertSame(PREFIX + q);
        }
    }
}
