package com.ebremer.beakgraph;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.core.PathMerge;
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
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpPath;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link PathMerge}: the triples around a property path joined into one BGP, the path after it, and the patterns
 * reached only through a path's end after the path. Every query is compared with Jena's own engine over the same
 * data; the counter says whether it was rewritten.
 */
class PathMergeTest {

    private static final String PREFIX = "PREFIX ex: <http://ex.org/>\n";
    private static final String UNION = "FROM <urn:x-arq:UnionGraph> ";
    private static final String TRIG = """
            @prefix ex: <http://ex.org/> .
            ex:g1 {
              ex:town1 ex:partOf ex:county1 . ex:county1 ex:partOf ex:state1 . ex:state1 ex:partOf ex:country .
              ex:town2 ex:partOf ex:county2 . ex:county2 ex:partOf ex:state1 . ex:town3 ex:partOf ex:state2 .
              ex:town1 a ex:Town ; ex:label "T1" . ex:town2 a ex:Town ; ex:label "T2" . ex:town3 a ex:Town ; ex:label "T3" .
              ex:county1 a ex:County . ex:county2 a ex:County .
              ex:state1 a ex:State ; ex:label "S1" . ex:state2 a ex:State ; ex:label "S2" . ex:country a ex:Country ; ex:label "C" .
            }
            ex:g2 {
              ex:f1 ex:within ex:cell1 ; a ex:Facility ; ex:label "F1" ; ex:industry ex:i1 .
              ex:f2 ex:within ex:cell2 ; a ex:Facility ; ex:label "F2" ; ex:industry ex:i2 .
              ex:f3 ex:within ex:cell3 ; a ex:Facility ; ex:label "F3" ; ex:industry ex:i1 .
              ex:list1 ex:member ex:i1 .
              ex:sp1 ex:within ex:cell1 ; ex:within ex:town1 ; a ex:Sample ; ex:value 5 .
              ex:sp2 ex:within ex:cell2 ; ex:within ex:town2 ; a ex:Sample ; ex:value 7 .
              ex:sp3 ex:within ex:cell3 ; ex:within ex:town3 ; a ex:Sample ; ex:value 9 .
            }
            """;

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset store;
    static Dataset reference;

    @BeforeAll
    static void build() throws Exception {
        Path src = dir.resolve("paths.trig");
        Files.write(src, TRIG.getBytes(StandardCharsets.UTF_8));
        File h5 = dir.resolve("paths.h5").toFile();
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

    /** Asserts Jena's answer; returns how many chains the store rewrote for it. */
    private static long rewrites(String query) {
        long before = PathMerge.HITS.get();
        List<String> got = rows(store, query);
        long n = PathMerge.HITS.get() - before;
        assertEquals(rows(reference, query), got, query);
        return n;
    }

    /** A triple after a path joins one before it by a variable that is not the path's end: merged, and Jena's answer. */
    @ParameterizedTest
    @ValueSource(strings = {
        // PFAS's shape: samples, a BIND, the + path to the state, the facilities in the samples' cells
        "SELECT * " + UNION + "{ ?sp a ex:Sample ; ex:within ?cell ; ex:within ?town ; ex:value ?v BIND(?v * 2 AS ?v2) "
            + "?town a ex:Town . ?town ex:partOf+ ?state . ?town ex:label ?tl . ?state a ex:State ; ex:label ?sl . "
            + "?f ex:within ?cell ; a ex:Facility ; ex:label ?fl ; ex:industry ?i . ?list ex:member ?i }",
        "SELECT * " + UNION + "{ ?sp a ex:Sample ; ex:within ?cell ; ex:within ?t . ?t ex:partOf+ ?s . ?f ex:within ?cell ; a ex:Facility . ?s a ex:State }",
        // two paths, the second from the first's far end
        "SELECT * " + UNION + "{ ?sp ex:within ?cell . ?sp ex:within ?t . ?t ex:partOf+ ?s . ?s ex:partOf* ?c . ?f ex:within ?cell . ?c a ex:Country }",
        // inside GRAPH
        "SELECT * { GRAPH ex:g2 { ?sp ex:within ?cell . ?sp ex:within+ ?x . ?f ex:within ?cell ; ex:label ?fl } }",
    })
    void mergedWithJenasAnswer(String query) {
        assertTrue(rewrites(query) >= 1, "rewritten: " + query);
    }

    /** The triples after the path joined only through its ends: left alone - the path narrows what follows - and Jena's answer. */
    @ParameterizedTest
    @ValueSource(strings = {
        // a filter: the path's far end a term (federation/sawgraph-spatial-hydrology-01's shape)
        "SELECT * " + UNION + "{ ?t a ex:Town . ?t ex:partOf+ ex:state1 . ?t ex:label ?l }",
        // two paths, each linked to the rest by its ends
        "SELECT * " + UNION + "{ ?t a ex:Town . ?t ex:partOf+ ?s . ?s a ex:State . ?s ex:partOf* ?c . ?c a ex:Country ; ex:label ?cl }",
        // an alternative path
        "SELECT * " + UNION + "{ ?sp a ex:Sample . ?sp (ex:within|ex:partOf)+ ?x . ?x a ex:State ; ex:label ?l }",
        // a group nothing reaches: a cross product
        "SELECT * " + UNION + "{ ?t a ex:Town . ?t ex:partOf+ ?s . ?s a ex:State . ?x a ex:Country }",
        // both ends of the path in groups of their own
        "SELECT * " + UNION + "{ ?f a ex:Facility . ?t ex:partOf+ ?s . ?t a ex:Town . ?s a ex:State }",
        // a path first (evoweb/connections-to-group's shape)
        "SELECT * " + UNION + "{ ex:town1 ex:partOf+ ?s . ?s ex:label ?l }",
        // no triple after the path
        "SELECT * " + UNION + "{ ?t a ex:Town . ?t ex:partOf+ ?s }",
        "SELECT * { GRAPH ex:g1 { ?t a ex:Town . ?t ex:partOf+ ?s . ?s ex:label ?l } }",
    })
    void leftAloneWithJenasAnswer(String query) {
        assertEquals(0, rewrites(query), "not rewritten: " + query);
    }

    /**
     * A BIND whose variable a triple uses (lifting it would drop that join), or which reads a variable bound only
     * after it: the BIND stays under the patterns that follow it - the chain under it may still be merged - and
     * the answer is Jena's.
     */
    @ParameterizedTest
    @ValueSource(strings = {
        "SELECT * " + UNION + "{ ?sp ex:value ?v BIND(\"T1\" AS ?w) ?sp ex:within ?t . ?t ex:partOf+ ?s . ?t ex:label ?w }",
        "SELECT * " + UNION + "{ ?sp ex:value ?v BIND(STR(?l) AS ?w) ?sp ex:within ?t . ?t ex:partOf+ ?s . ?t ex:label ?l }",
    })
    void aBindThatCannotRiseStaysWhereItIs(String query) {
        rewrites(query);
        Op op = PathMerge.transform(Algebra.optimize(Algebra.compile(QueryFactory.create(PREFIX + query))));
        Op body = (op instanceof OpProject p) ? p.getSubOp() : op;
        assertTrue(!(body instanceof OpExtend), "the BIND not lifted to the top: " + op);
    }

    /** Other shapes around a path, whichever way they go: Jena's answer. */
    @ParameterizedTest
    @ValueSource(strings = {
        "SELECT * " + UNION + "{ ?t a ex:Town . ?t ex:partOf+ ?s FILTER(?s != ex:country) ?s ex:label ?l }",
        "SELECT * " + UNION + "{ ?t a ex:Town . ?t ex:partOf+ ?s OPTIONAL { ?s ex:label ?l } }",
        "SELECT * " + UNION + "{ ?t a ex:Town . ?t ex:partOf+ ?s . ?s ex:label ?l MINUS { ?s a ex:Country } }",
        "SELECT (COUNT(*) AS ?n) " + UNION + "{ ?t a ex:Town . ?t ex:partOf* ?s . ?s ex:label ?l }",
        "SELECT DISTINCT ?sl " + UNION + "{ ?sp ex:within ?t . ?t a ex:Town . ?t ex:partOf+ ?s . ?s a ex:State ; ex:label ?sl } ORDER BY ?sl",
    })
    void otherShapesGiveJenasAnswer(String query) {
        rewrites(query);
    }

    @Test
    void theStateRunsAfterThePathAndTheBindOnTop() {
        String q = PREFIX + "SELECT * { ?sp a ex:Sample ; ex:within ?town ; ex:within ?cell ; ex:value ?v BIND(?v * 2 AS ?v2) "
                + "?town ex:partOf+ ?state . ?town ex:label ?tl . ?state a ex:State ; ex:label ?sl . ?f ex:within ?cell }";
        Op op = PathMerge.transform(Algebra.optimize(Algebra.compile(QueryFactory.create(q))));
        Op body = (op instanceof OpProject p) ? p.getSubOp() : op;
        assertTrue(body instanceof OpExtend, "the BIND on top: " + op);
        Op chain = ((OpExtend) body).getSubOp();
        assertTrue(chain instanceof OpSequence s && s.size() == 3, "BGP, path, BGP: " + op);
        OpSequence s = (OpSequence) chain;
        assertTrue(s.get(0) instanceof OpBGP core && core.getPattern().size() == 6, "the samples', the town's and the facility's triples first: " + op);
        assertTrue(s.get(1) instanceof OpPath, "then the path: " + op);
        assertTrue(s.get(2) instanceof OpBGP rest && rest.getPattern().size() == 2, "then the state's two: " + op);
    }

    @Test
    void offWhenTurnedOff() {
        String q = "SELECT * " + UNION + "{ ?sp ex:within ?cell ; ex:within ?t . ?t ex:partOf+ ?s . ?f ex:within ?cell . ?s ex:label ?l }";
        System.setProperty("beakgraph.path.merge", "false");
        try {
            assertEquals(0, rewrites(q));
        } finally {
            System.clearProperty("beakgraph.path.merge");
        }
        assertTrue(rewrites(q) >= 1);
    }
}
