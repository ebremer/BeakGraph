package com.ebremer.beakgraph.hdf5.jena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.readers.IndexReader;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.E_GreaterThan;
import org.apache.jena.sparql.expr.E_LessThan;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.expr.ExprVar;
import org.apache.jena.sparql.expr.NodeValue;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The FILTER range hints are resolved once per store and pattern shape by
 * {@link RangeBounds} and shared by every iterator (BG-301: the four copies
 * had drifted between signed and unsigned arithmetic; BG-62: every join
 * input row re-ran the dictionary probes), the object-side iterator narrows
 * a subject hint by binary search (BG-258), and a nested EXISTS gets its own
 * hints, never the outer filter's (BG-339).
 */
class RangeBoundsTest {

    private static final String EX = "http://ex.org/";
    private static final String PREFIX = "PREFIX ex: <" + EX + "> ";
    /** Only IRI objects: the store has no literals section, so the object dictionary answers -1 for a literal probe. */
    private static final String TTL = """
        @prefix ex: <http://ex.org/> .
        ex:s1 ex:p ex:o . ex:s2 ex:p ex:o . ex:s3 ex:p ex:o . ex:s4 ex:p ex:o . ex:s5 ex:p ex:o .
        ex:s6 ex:p ex:o . ex:s7 ex:p ex:o . ex:s8 ex:p ex:o . ex:s9 ex:p ex:o .
        ex:s1 ex:q ex:o2 . ex:s2 ex:q ex:o2 .
        """;
    /** Numeric objects for the EXISTS parity (the ValueClusterFilterTest shape plus ex:other). */
    private static final String NUMERIC_TTL = """
        @prefix ex: <http://ex.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        ex:m1 ex:value "5"^^xsd:int . ex:m2 ex:value 5 . ex:m3 ex:value "5.0"^^xsd:double .
        ex:m4 ex:value 4 . ex:m5 ex:value 6 . ex:m6 ex:value 7 .
        ex:m1 ex:other 50 . ex:m2 ex:other 500 . ex:m5 ex:other 1 . ex:m6 ex:other 2000 .
        ex:g1 { ex:m3 ex:other 9 . }
        """;

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static HDF5Reader reader;
    static Dataset ds;
    static Dataset truth;
    static BeakGraph numeric;
    static Dataset numericDs;
    static Dataset numericTruth;

    @BeforeAll
    static void build() throws Exception {
        File ttl = dir.resolve("iri.ttl").toFile();
        Files.writeString(ttl.toPath(), TTL, StandardCharsets.UTF_8);
        File h5 = dir.resolve("iri.ttl.h5").toFile();
        HDF5Writer.Builder().setSource(ttl).setDestination(h5).setSpatial(false).setFeatures(false).build().write();
        reader = new HDF5Reader(h5);
        bg = new BeakGraph(reader);
        ds = bg.getDataset();
        Model m = ModelFactory.createDefaultModel();
        RDFDataMgr.read(m, new StringReader(TTL), null, Lang.TURTLE);
        truth = DatasetFactory.create(m);

        File trig = dir.resolve("num.trig").toFile();
        Files.writeString(trig.toPath(), NUMERIC_TTL, StandardCharsets.UTF_8);
        File nh5 = dir.resolve("num.trig.h5").toFile();
        HDF5Writer.Builder().setSource(trig).setDestination(nh5).setSpatial(false).setFeatures(false).build().write();
        numeric = new BeakGraph(new HDF5Reader(nh5));
        numericDs = numeric.getDataset();
        numericTruth = RDFDataMgr.loadDataset(trig.getAbsolutePath());
    }

    @AfterAll
    static void close() {
        if (bg != null) bg.close();
        if (numeric != null) numeric.close();
    }

    private static List<String> rows(Dataset dataset, String query) {
        List<String> out = new ArrayList<>();
        try (QueryExecution qe = QueryExecution.dataset(dataset).query(QueryFactory.create(PREFIX + query)).build()) {
            ResultSet rs = qe.execSelect();
            List<String> vars = rs.getResultVars();
            while (rs.hasNext()) {
                QuerySolution row = rs.next();
                StringBuilder sb = new StringBuilder();
                for (String v : vars) sb.append(v).append('=').append(row.get(v)).append('|');
                out.add(sb.toString());
            }
        }
        Collections.sort(out);
        return out;
    }

    private static void parity(Dataset a, Dataset b, String query) {
        assertEquals(rows(b, query), rows(a, query), query);
    }

    @Test
    void aStoreWithoutLiteralsAnswersRangeFiltersAlikeOnEveryRoute() {
        // BG-301: the object dictionary answers -1 here; every route must agree with ARQ (no rows).
        for (String op : new String[]{"<", ">", "<=", ">="}) {
            parity(ds, truth, "SELECT ?s ?p ?o WHERE { ?s ?p ?o FILTER(?o " + op + " 5) }");        // SPO_All
            parity(ds, truth, "SELECT ?s ?o WHERE { ?s ex:p ?o FILTER(?o " + op + " 5) }");          // POS
            parity(ds, truth, "SELECT ?p ?o WHERE { ex:s1 ?p ?o FILTER(?o " + op + " 5) }");         // SO
            parity(ds, truth, "SELECT ?s WHERE { ?s ex:p ex:o FILTER(?s " + op + " 5) }");           // OS
        }
        PositionalDictionaryReader dict = (PositionalDictionaryReader) reader.getDictionary();
        ExprList lt = new ExprList(new E_LessThan(new ExprVar("o"), NodeValue.makeInteger(5)));
        Quad q = new Quad(Quad.defaultGraphIRI, Var.alloc("s"), Var.alloc("p"), Var.alloc("o"));
        RangeBounds b = RangeBounds.resolve(lt, q, dict);
        assertTrue(b.minO >= 0, "lower edges never go below the position's floor: " + b);
        assertTrue(b.maxO < b.minO || b.maxO < 1, "nothing can be below 5 in a store without literals: " + b);
        // BG-351: the absent literals section answers the insertion point just
        // past the entity block, so a lower bound empties the scan instead of
        // walking every object row.
        ExprList gt = new ExprList(new E_GreaterThan(new ExprVar("o"), NodeValue.makeInteger(5)));
        long entities = dict.getSubjects().getNumberOfNodes();
        assertEquals(entities + 1, RangeBounds.resolve(gt, q, dict).minO, "FILTER(?o > 5) starts past the last entity id");
    }

    @Test
    void boundsAreResolvedOncePerStoreAndPatternShape() {
        PositionalDictionaryReader dict = (PositionalDictionaryReader) reader.getDictionary();
        Quad q = new Quad(Quad.defaultGraphIRI, Var.alloc("s"), Var.alloc("p"), Var.alloc("o"));
        ExprList first = new ExprList(new E_GreaterThan(new ExprVar("o"), NodeValue.makeInteger(5)));
        ExprList second = new ExprList(new E_GreaterThan(new ExprVar("o"), NodeValue.makeInteger(5)));
        assertNotSame(first, second);
        RangeBounds a = RangeBounds.of(first, q, dict);
        RangeBounds b = RangeBounds.of(second, q, dict);
        assertSame(a, b, "an equal filter over the same pattern shape hits the per-store memo (BG-62)");
        Quad other = new Quad(Quad.defaultGraphIRI, Var.alloc("x"), Var.alloc("p"), Var.alloc("o"));
        assertEquals(a.toString(), RangeBounds.of(first, other, dict).toString(), "a renamed subject variable resolves to the same bounds");
        Quad objectRenamed = new Quad(Quad.defaultGraphIRI, Var.alloc("s"), Var.alloc("p"), Var.alloc("z"));
        assertNotSame(a, RangeBounds.of(first, objectRenamed, dict), "the hint no longer names the object: a different shape");
        assertSame(RangeBounds.NONE, RangeBounds.of(null, q, dict));
        assertSame(RangeBounds.NONE, RangeBounds.of(new ExprList(), q, dict));
    }

    private static Node defaultGraphNode() throws Exception {
        Field f = HDF5Reader.class.getDeclaredField("defaultGraph");
        f.setAccessible(true);
        return (Node) f.get(reader);
    }

    private static int subjectsUnder(ExprList hints) throws Exception {
        PositionalDictionaryReader dict = (PositionalDictionaryReader) reader.getDictionary();
        IndexReader gpos = reader.getIndexReader(Index.GPOS);
        Quad q = new Quad(defaultGraphNode(), Var.alloc("s"), NodeFactory.createURI(EX + "p"), NodeFactory.createURI(EX + "o"));
        BGIteratorOS it = new BGIteratorOS(dict, gpos, new BindingNodeId(), q, hints, reader.getNodeTable());
        int n = 0;
        while (it.hasNext()) { it.next(); n++; }
        return n;
    }

    @Test
    void objectSideIteratorNarrowsASubjectHintToTheExactRange() throws Exception {
        // BG-258: the (G,P,O) subject block is ascending in dictionary order
        // (ex:s1 < ... < ex:s9), so a subject hint is a binary-searched slice.
        assertEquals(9, subjectsUnder(null));
        assertEquals(4, subjectsUnder(new ExprList(new E_GreaterThan(new ExprVar("s"), NodeValue.makeNode(NodeFactory.createURI(EX + "s5"))))));
        assertEquals(2, subjectsUnder(new ExprList(new E_LessThan(new ExprVar("s"), NodeValue.makeNode(NodeFactory.createURI(EX + "s3"))))));
        ExprList both = new ExprList();
        both.add(new E_GreaterThan(new ExprVar("s"), NodeValue.makeNode(NodeFactory.createURI(EX + "s2"))));
        both.add(new E_LessThan(new ExprVar("s"), NodeValue.makeNode(NodeFactory.createURI(EX + "s6"))));
        assertEquals(3, subjectsUnder(both));
        assertEquals(0, subjectsUnder(new ExprList(new E_GreaterThan(new ExprVar("s"), NodeValue.makeNode(NodeFactory.createURI(EX + "s9"))))));
    }

    @Test
    void nestedExistsGetsItsOwnHintsNotTheOuterFilters() {
        // BG-339: the outer filter's hints must not narrow the inner BGP's object range.
        parity(numericDs, numericTruth,
                "SELECT ?s ?o WHERE { ?s ex:value ?o FILTER(?o > 4 && NOT EXISTS { ?s ex:other ?x FILTER(?x < 100) }) }");
        parity(numericDs, numericTruth,
                "SELECT ?s ?o WHERE { ?s ex:value ?o FILTER(?o > 4 && EXISTS { ?s ex:other ?x FILTER(?x < 100) }) }");
        parity(numericDs, numericTruth,
                "SELECT ?s ?o WHERE { ?s ex:value ?o FILTER(?o >= 5 && EXISTS { GRAPH ?g { ?s ex:other ?x } }) }");
        parity(numericDs, numericTruth,
                "SELECT ?s ?o ?x WHERE { ?s ex:value ?o FILTER(?o < 6) OPTIONAL { ?s ex:other ?x FILTER(?x > 40) } }");
    }
}
