package com.ebremer.beakgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.core.BeakGraph;
import io.jhdf.HdfFile;
import io.jhdf.api.Group;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.util.IsoMatcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * RDF 1.2 triple terms over a real method-0 store:
 * term-exact round-trip (nested terms, blank-node co-reference, embedded
 * dirLang identity), the id-level unification of variable-containing
 * triple-term patterns through every iterator family, the SPARQL 1.2 term
 * functions, union-graph dedup with embedded variables, and format-v5
 * stamping.
 */
class RDF12TripleTermTest {

    private static final String PRE = "PREFIX : <http://ex.org/> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ";

    private static final String TRIG = """
        @prefix : <http://ex.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        @prefix cdt: <http://w3id.org/awslabs/neptune/SPARQL-CDTs/> .

        :r :says <<( :a :b :c )>> .
        :r :says2 <<( :a :b <<( :x :y "lit" )>> )>> .
        _:b1 :label "outside" .
        :r :obs <<( _:b1 :p "inside" )>> .
        :r :only <<( :onlySubj :onlyPred :onlyObj )>> .
        :r :dl1 <<( :a :b "hello"@en--ltr )>> .
        :r :dl2 <<( :a :b "hello"@en--rtl )>> .
        :r :cdt <<( :a :b '[1, 2]'^^cdt:List )>> .
        :r :eq <<( :m :same :m )>> .
        :r :eq <<( :m :same :n )>> .
        :r :num <<( :a :v "42"^^xsd:int )>> .
        :s1 :plain :o1 .

        :g1 {
            :r :says <<( :a :b :c )>> .
            :s2 :p :o2 .
        }
        :g2 {
            :r :says <<( :a :b :c )>> .
        }
        """;

    @TempDir
    static Path dir;
    static BeakGraph bg;
    static Dataset ds;
    static File dest;

    @BeforeAll
    static void build() throws Exception {
        Path src = dir.resolve("tripleterms.trig");
        Files.writeString(src, TRIG);
        dest = dir.resolve("tripleterms.h5").toFile();
        // The engine named by -Dbeakgraph.test.engine (method 0 by default).
        WriterEngines.selected().assumeAvailable();
        WriterEngines.selected().buildStore(src.toFile(), dest);
        bg = BG.getBeakGraph(dest);
        ds = bg.getDataset();
    }

    @AfterAll
    static void close() throws Exception {
        if (bg != null) {
            bg.close();
        }
    }

    // ---------------------------------------------------------------- storage

    @Test
    void storeStampsFormatVersion5() {
        try (HdfFile hdf = new HdfFile(dest.toPath())) {
            Group bgGroup = (Group) hdf.getChild(".BG");
            assertEquals(5, ((Number) bgGroup.getAttribute("formatVersion").getData()).intValue());
        }
    }

    @Test
    void roundTripIsIsomorphic() {
        DatasetGraph expected = DatasetGraphFactory.create();
        RDFParser.create().fromString(TRIG).lang(Lang.TRIG).parse(expected);
        DatasetGraph got = DatasetGraphFactory.create();
        ds.asDatasetGraph().find().forEachRemaining(got::add);
        assertTrue(IsoMatcher.isomorphic(expected, got),
                "store must round-trip the dataset (incl. nested triple terms) isomorphically");
    }

    @Test
    void blankNodeCoReferenceInsideTripleTermSurvives() {
        // The bnode INSIDE <<( _:b1 :p "inside" )>> must be the SAME node as the
        // one outside carrying :label "outside" - the invariant whose absence
        // forced the CDT blank-node rejection policy.
        assertEquals(1, count(PRE + "SELECT ?b WHERE { ?b :label \"outside\" . :r :obs <<( ?b :p \"inside\" )>> }"));
    }

    @Test
    void numericCanonicalizationRecursesIntoTripleTerms() throws Exception {
        String two = """
            @prefix : <http://ex.org/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
            :r :num <<( :a :v "042"^^xsd:int )>> .
            :r :num <<( :a :v "42"^^xsd:int )>> .
            """;
        Path src = dir.resolve("canon.ttl");
        Files.writeString(src, two);
        File out = dir.resolve("canon.h5").toFile();
        WriterEngines.selected().buildStore(src.toFile(), out);
        try (BeakGraph b2 = BG.getBeakGraph(out)) {
            List<QuerySolution> rows = select(b2.getDataset(), PRE + "SELECT ?o WHERE { :r :num ?o }");
            assertEquals(1, rows.size(),
                    "lexical variants of one numeric value inside triple terms must collapse to one term");
        }
    }

    @Test
    void embeddedBaseDirectionKeepsTermsDistinct() {
        // Two triple terms differing ONLY in an embedded literal's base
        // direction: NodeCmp would call them equal (Finding 2); the structural
        // comparator branch must not.
        List<QuerySolution> a = select(ds, PRE + "SELECT ?o WHERE { :r :dl1 ?o }");
        List<QuerySolution> b = select(ds, PRE + "SELECT ?o WHERE { :r :dl2 ?o }");
        assertEquals(1, a.size());
        assertEquals(1, b.size());
        assertNotEquals(a.get(0).get("o").asNode(), b.get(0).get("o").asNode(),
                "terms differing only in embedded base direction must stay distinct");
    }

    // ------------------------------------------------------------ exact terms

    @Test
    void concreteTripleTermLookupHitsAndMisses() {
        assertTrue(ask(PRE + "ASK { :r :says <<( :a :b :c )>> }"));
        assertFalse(ask(PRE + "ASK { :r :says <<( :a :b :ZZZ )>> }"),
                "an absent concrete triple term must answer false, not scan");
        assertTrue(ask(PRE + "ASK { :r :says2 <<( :a :b <<( :x :y \"lit\" )>> )>> }"),
                "nested concrete triple term lookup");
    }

    @Test
    void tripleTermReturnedAsObjectVariable() {
        List<QuerySolution> rows = select(ds, PRE + "SELECT ?o WHERE { :r :says ?o }");
        assertEquals(1, rows.size());
        Node o = rows.get(0).get("o").asNode();
        assertTrue(o.isTripleTerm());
        Node expected = NodeFactory.createTripleTerm(
                NodeFactory.createURI("http://ex.org/a"),
                NodeFactory.createURI("http://ex.org/b"),
                NodeFactory.createURI("http://ex.org/c"));
        assertEquals(expected, o);
    }

    // --------------------------------------------- var-containing TT patterns

    @Test
    void patternThroughGSPBoundRoute() { // BGIteratorSO + matcher
        List<QuerySolution> rows = select(ds, PRE + "SELECT ?x WHERE { :r :says <<( :a :b ?x )>> }");
        assertEquals(1, rows.size());
        assertEquals("http://ex.org/c", rows.get(0).getResource("x").getURI());
    }

    @Test
    void patternThroughGPBoundRoute() { // BGIteratorPOS + matcher
        List<QuerySolution> rows = select(ds, PRE + "SELECT ?s ?x WHERE { ?s :says <<( ?a :b ?x )>> }");
        assertEquals(1, rows.size());
        assertEquals("http://ex.org/r", rows.get(0).getResource("s").getURI());
        assertEquals("http://ex.org/c", rows.get(0).getResource("x").getURI());
    }

    @Test
    void patternThroughScanRoute() { // BGIteratorSPO_All + matcher (P variable)
        List<QuerySolution> rows = select(ds, PRE + "SELECT ?p ?x WHERE { :r ?p <<( :a :b ?x )>> }");
        // :says matches <<( :a :b :c )>>, :dl1/:dl2/:cdt/:num match with other
        // subjects than :a? No - their terms all have subject :a and predicate
        // :b except :num (:a :v ...): expect :says->:c, :dl1->"hello"@en--ltr,
        // :dl2->"hello"@en--rtl, :cdt->'[1, 2]', and NOT :num (predicate :v),
        // NOT :says2 (object is a nested term with subject :a? no - :says2's
        // components are (:a :b <<...>>), so it DOES match with ?x = the nested term).
        assertEquals(5, rows.size());
    }

    @Test
    void nestedPatternUnifies() {
        List<QuerySolution> rows = select(ds,
                PRE + "SELECT ?v WHERE { :r :says2 <<( :a :b <<( :x :y ?v )>> )>> }");
        assertEquals(1, rows.size());
        assertEquals("lit", rows.get(0).getLiteral("v").getLexicalForm());
    }

    @Test
    void repeatedVariableInsidePatternEnforced() {
        List<QuerySolution> rows = select(ds, PRE + "SELECT ?x WHERE { :r :eq <<( ?x :same ?x )>> }");
        assertEquals(1, rows.size(), "only <<( :m :same :m )>> satisfies the repeated variable");
        assertEquals("http://ex.org/m", rows.get(0).getResource("x").getURI());
    }

    @Test
    void unmatchablePatternComponentAnswersEmptyFast() {
        assertFalse(ask(PRE + "ASK { :r :says <<( :ZZZ ?b ?c )>> }"),
                "a pattern component absent from the store can match nothing");
    }

    @Test
    void patternOutsideObjectPositionAnswersEmpty() {
        assertFalse(ask(PRE + "ASK { <<( ?a ?b ?c )>> :p :o1 }"),
                "var-containing triple terms cannot occur outside the object position in data");
    }

    // ------------------------------------------------------- SPARQL functions

    @Test
    void sparqlTermFunctionsWork() {
        List<QuerySolution> rows = select(ds, PRE
                + "SELECT ?s2 ?p2 ?o2 WHERE { :r :says ?t . FILTER(isTRIPLE(?t)) "
                + "BIND(SUBJECT(?t) AS ?s2) BIND(PREDICATE(?t) AS ?p2) BIND(OBJECT(?t) AS ?o2) }");
        assertEquals(1, rows.size());
        assertEquals("http://ex.org/a", rows.get(0).getResource("s2").getURI());
        assertEquals("http://ex.org/b", rows.get(0).getResource("p2").getURI());
        assertEquals("http://ex.org/c", rows.get(0).getResource("o2").getURI());
    }

    @Test
    void tripleFunctionEqualityMatches() {
        assertTrue(ask(PRE + "ASK { :r :says ?t . FILTER(?t = TRIPLE(:a, :b, :c)) }"));
    }

    // ----------------------------------------------------------- graph scope

    @Test
    void namedGraphAndUnionDedup() {
        // The same triple-term quad lives in :g1 AND :g2 - GRAPH ?g sees both...
        assertEquals(2, count(PRE + "SELECT ?g WHERE { GRAPH ?g { :r :says <<( :a :b ?x )>> } }"));
        // ...while the union graph dedups them into one row keyed on the
        // embedded variable. (Note Jena's sentinel is urn:x-arq:UnionGraph,
        // capital U - the lowercase spelling names an ordinary absent graph.)
        assertEquals(1, count(PRE + "SELECT ?x WHERE { GRAPH <urn:x-arq:UnionGraph> { :r :says <<( :a :b ?x )>> } }"));
    }

    // -------------------------------------------------------------- plumbing

    private static boolean ask(String q) {
        try (QueryExecution qe = QueryExecution.dataset(ds).query(QueryFactory.create(q)).build()) {
            return qe.execAsk();
        }
    }

    private static int count(String q) {
        return select(ds, q).size();
    }

    private static List<QuerySolution> select(Dataset dataset, String q) {
        List<QuerySolution> out = new ArrayList<>();
        try (QueryExecution qe = QueryExecution.dataset(dataset).query(QueryFactory.create(q)).build()) {
            qe.execSelect().forEachRemaining(out::add);
        }
        return out;
    }
}
