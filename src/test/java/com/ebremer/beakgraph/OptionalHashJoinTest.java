package com.ebremer.beakgraph;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.jena.OptionalHashJoin;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@code OPTIONAL { ?s <p> ?o }} over many rows reads the pattern once into a table ({@link OptionalHashJoin})
 * instead of looking it up per row, and over few rows looks it up per row as Jena does; either way the answer
 * is Jena's. The rows needed to read the table are lowered here to 4, so a small store exercises both.
 */
class OptionalHashJoinTest {

    private static final String NS = "http://ex.org/";
    private static final String PREFIX = "PREFIX ex: <" + NS + "> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n";
    private static final String UNION = "urn:x-arq:UnionGraph";

    /**
     * 30 charges, each with a booking; bookings start on day i and those with i % 3 != 0 end on day i + 5,
     * those with i % 5 == 0 end twice; every case has three docket entries. g1 and g2 repeat one end date.
     */
    private static String trig() {
        StringBuilder t = new StringBuilder("@prefix ex: <http://ex.org/> .\n@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n");
        for (int i = 0; i < 30; i++) {
            t.append("ex:charge").append(i).append(" ex:booking ex:b").append(i).append(" ; ex:case ex:case").append(i % 10).append(" .\n");
            t.append("ex:b").append(i).append(" ex:start ").append(i).append(" .\n");
            if (i % 3 != 0) t.append("ex:b").append(i).append(" ex:end ").append(i + 5).append(" .\n");
            if (i % 5 == 0) t.append("ex:b").append(i).append(" ex:end ").append(i + 7).append(" .\n");
        }
        for (int c = 0; c < 10; c++) {
            for (int e = 0; e < 3; e++) t.append("ex:case").append(c).append(" ex:entry ex:e").append(c).append('_').append(e).append(" .\n");
        }
        t.append("ex:lit ex:a \"text\" .\n");
        t.append("ex:g1 { ex:x1 ex:a ex:y1 . ex:y1 ex:end 1 . ex:y2 ex:end 2 . ex:x2 ex:a ex:y2 . ex:x3 ex:a ex:y3 . ex:x4 ex:a ex:y1 . ex:x5 ex:a ex:y9 . }\n");
        t.append("ex:g2 { ex:y1 ex:end 1 . ex:y1 ex:end 3 . ex:x6 ex:a ex:y1 . }\n");
        return t.toString();
    }

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset store;
    static Dataset reference;
    String oldMin;

    @BeforeAll
    static void build() throws Exception {
        Path src = dir.resolve("optional.trig");
        Files.write(src, trig().getBytes(StandardCharsets.UTF_8));
        File h5 = dir.resolve("optional.h5").toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        bg = new BeakGraph(new HDF5Reader(h5));
        store = bg.getDataset();
        DatasetGraph dsg = DatasetGraphFactory.create();
        RDFParser.create().source(src.toUri().toString()).lang(Lang.TRIG).parse(dsg);
        reference = DatasetFactory.wrap(dsg);
    }

    @AfterAll
    static void close() {
        if (bg != null) bg.close();
    }

    @BeforeEach
    void fewRowsMakeATable() {
        oldMin = System.setProperty("beakgraph.optional.hash.minrows", "4");
    }

    @AfterEach
    void restore() {
        if (oldMin == null) System.clearProperty("beakgraph.optional.hash.minrows");
        else System.setProperty("beakgraph.optional.hash.minrows", oldMin);
    }

    private static List<String> rows(Dataset d, String query) {
        List<String> out = new ArrayList<>();
        try (QueryExecution qe = QueryExecution.dataset(d).query(QueryFactory.create(PREFIX + query)).build()) {
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

    /** Asserts Jena's answer and the row count, and returns how many OPTIONALs were read into a table. */
    private static long tables(String query, int expectedRows) {
        long before = OptionalHashJoin.HITS.get();
        List<String> got = rows(store, query);
        long used = OptionalHashJoin.HITS.get() - before;
        assertEquals(rows(reference, query), got, query);
        assertEquals(expectedRows, got.size(), query);
        return used;
    }

    @Test
    void manyRowsReadThePatternOnce() {
        // 30 bookings: 8 without an end, 18 with one, 4 with two - 34 rows.
        assertEquals(1, tables("SELECT ?b ?e WHERE { ?c ex:booking ?b OPTIONAL { ?b ex:end ?e } }", 34));
    }

    @Test
    void theRowsDocketEntriesMultiply() {
        // jail-population's shape: each case's entries multiply its bookings before the OPTIONAL and the filters.
        String q = "SELECT DISTINCT ?c WHERE { VALUES ?d { 20 } ?c ex:booking ?b ; ex:case ?k . ?k ex:entry ?x . ?b ex:start ?s"
                + " OPTIONAL { ?b ex:end ?e } FILTER(?s <= ?d) FILTER(!bound(?e) || ?e >= ?d) }";
        assertEquals(1, tables(q, rows(reference, q).size()));
    }

    @Test
    void fewRowsGoARowAtATime() {
        assertEquals(0, tables("SELECT ?b ?e WHERE { VALUES ?b { ex:b1 ex:b2 } OPTIONAL { ?b ex:end ?e } }", 2));
    }

    @Test
    void anObjectBoundAlreadyGoesARowAtATime() {
        // ?e bound by the left side: compatibility on ?e too - Jena's way, the answer the same.
        tables("SELECT ?b ?e WHERE { ?b ex:start ?e OPTIONAL { ?b ex:end ?e } }", 30);
    }

    @Test
    void rowsWithoutTheSubjectGoARowAtATime() {
        tables("SELECT ?b ?y ?e WHERE { { ?b ex:start ?y } UNION { ?z ex:start ?y } OPTIONAL { ?b ex:end ?e } }", rows(reference,
                "SELECT ?b ?y ?e WHERE { { ?b ex:start ?y } UNION { ?z ex:start ?y } OPTIONAL { ?b ex:end ?e } }").size());
    }

    @Test
    void aLiteralOrAbsentTermAsTheSubjectMatchesNothing() {
        tables("SELECT ?b ?e WHERE { VALUES ?b { \"text\" ex:nothing ex:b1 ex:b2 ex:b3 ex:b4 ex:b5 } OPTIONAL { ?b ex:end ?e } }",
                rows(reference, "SELECT ?b ?e WHERE { VALUES ?b { \"text\" ex:nothing ex:b1 ex:b2 ex:b3 ex:b4 ex:b5 } OPTIONAL { ?b ex:end ?e } }").size());
        String q = "SELECT ?s ?o ?e WHERE { ?s ex:a ?o OPTIONAL { ?o ex:end ?e } }";
        tables(q, rows(reference, q).size()); // ex:lit's "text" is a literal in the object position
    }

    @Test
    void overTheUnionGraphAMatchInTwoGraphsCountsOnce() {
        String q = "SELECT ?x ?y ?e FROM <" + UNION + "> WHERE { ?x ex:a ?y OPTIONAL { ?y ex:end ?e } }";
        assertEquals(1, tables(q, rows(reference, q).size()));
    }

    @Test
    void underAGraphVariable() {
        String q = "SELECT ?g ?x ?e WHERE { GRAPH ?g { ?x ex:a ?y OPTIONAL { ?y ex:end ?e } } }";
        tables(q, rows(reference, q).size());
    }

    @Test
    void otherShapesAreJenas() {
        for (String q : new String[] {
                "SELECT ?b ?e WHERE { ?c ex:booking ?b OPTIONAL { ?b ex:end ?e FILTER(?e > 10) } }",
                "SELECT ?b ?e ?s WHERE { ?c ex:booking ?b OPTIONAL { ?b ex:end ?e ; ex:start ?s } }",
                "SELECT ?b ?e WHERE { ?c ex:booking ?b OPTIONAL { ?b ex:end 6 } }"}) {
            assertEquals(0, tables(q, rows(reference, q).size()), q);
        }
    }

    @Test
    void theTablesKeysAndValues() {
        OptionalHashJoin.LongMultiMap m = new OptionalHashJoin.LongMultiMap();
        for (long k = 0; k < 5000; k++) {
            for (long v = 0; v < k % 4; v++) m.put(k * 7919, k * 10 + v);
        }
        for (long k = 0; k < 5000; k++) {
            List<Long> vs = new ArrayList<>();
            for (int i = m.first(k * 7919); i >= 0; i = m.next(i)) vs.add(m.value(i));
            Collections.sort(vs);
            List<Long> expected = new ArrayList<>();
            for (long v = 0; v < k % 4; v++) expected.add(k * 10 + v);
            assertEquals(expected, vs, "key " + k);
        }
        assertEquals(-1, m.first(1));
    }
}
