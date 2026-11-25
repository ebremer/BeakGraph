package com.ebremer.beakgraph.rdfarray;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ebremer.beakgraph.BG;
import com.ebremer.beakgraph.core.BeakGraph;
import com.ebremer.beakgraph.hdf5.jena.PatternMatchBG;
import com.ebremer.beakgraph.hdf5.writers.HDF5Writer;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.query.ResultSetRewindable;
import org.apache.jena.query.Syntax;
import org.apache.jena.sparql.resultset.ResultsCompare;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Sequence patterns matched on ids (RDFARRY.md §3.7, layer B) answer as
 * jena-array's solver does (layer A, {@code -Dbeakgraph.sequences.delegate}):
 * the same rows, as many times, for gaps and their placements, named gaps
 * whose run the store holds and does not, slots under every kind of pattern
 * element, bound variables and joins through slots, filters, nested patterns,
 * prefixes, and union reads where one triple stands in several graphs.
 */
class SequenceMatcherParityTest {

    @TempDir
    static Path dir;

    private static BeakGraph store;

    private static final String DATA = """
        PREFIX : <http://ex.org/>
        :a :p ( 7 7 ) .
        :b :p ( 1 2 3 ) .
        :c :p ( 1 undef 3 ) .
        :d :p ( undef ) .
        :e :p ( 5 ) .
        :f :p ( ( 1 2 ) ( 3 4 ) ) .
        :g :p () .
        :h :p ( :a :b ) .
        :i :p ( :i ) .
        :j :p ( 1 2 ) .
        :k :p ( "x" "y"@en <<( :a :q 1 )>> ) .
        :l :r ( 1 ( 2 undef ) ) .
        :m :p ( undef undef ) .
        :n :p ( 2 3 ) .
        :a :q 1 .
        :b :q 2 .
        :e :q 5 .
        :i :q :i .
        :p :p ( :p ) .
        GRAPH :g1 { :m :p ( 1 2 ) . :n :p ( undef ) . :n :p ( 5 ) . :o :p ( 7 7 ) . }
        GRAPH :g2 { :m :p ( 1 2 ) . :n :p ( 5 ) . :o :p ( 9 9 9 ) . :o :p ( 7 7 ) . }
        """;

    static Stream<String> queries() {
        return Stream.of(
            // gaps and their multiplicity
            "SELECT * { ?s :p ( ... ?x ... ) }",
            "SELECT * { ?s :p ( ?x ... ) }",
            "SELECT * { ?s :p ( ... ?x ) }",
            "SELECT * { ?s :p ( ... ) }",
            "SELECT * { ?s :p ( ... ... ) }",
            "SELECT * { ?s :p ( ... ?x ... ?y ... ) }",
            // fixed lengths, slots, undef
            "SELECT * { ?s :p ( ?x ) }",
            "SELECT * { ?s :p ( ?x ?y ) }",
            "SELECT * { ?s :p ( ?x ?y ?z ) }",
            "SELECT * { ?s :p ( ?x ?x ) }",
            "SELECT * { ?s :p ( ?x undef ?z ) }",
            "SELECT * { ?s :p ( undef ... ) }",
            "SELECT * { ?s :p ( undef ?x ) }",
            // constants and prefixes
            "SELECT * { ?s :p ( 1 ... ) }",
            "SELECT * { ?s :p ( 1 2 ... ) }",
            "SELECT * { ?s :p ( 1 ?x 3 ) }",
            "SELECT * { ?s :p ( 1 ?x ) }",
            "SELECT * { ?s :p ( ... 3 ) }",
            "SELECT * { ?s :p ( 42 ?x ) }",
            "SELECT * { ?s :p ( ?x 42 ) }",
            "SELECT * { ?s :p ( \"x\" ... ) }",
            // named gaps: a run the store holds, ( 1 2 ), and runs it does not
            "SELECT * { ?s :p ( ...?r ) }",
            "SELECT * { ?s :p ( ?x ...?r ) }",
            "SELECT * { ?s :p ( ...?r 3 ) }",
            "SELECT * { ?s :p ( ...?a ...?b ) }",
            "SELECT * { ?s :p ( ...?r ...?r ) }",
            "SELECT * { ?s :p ( ...?r undef ...?t ) }",
            "SELECT * { ?s :p ( ...?r 3 ) . ?t :p ?r }",
            "SELECT * { ?s :p ( ...?r ) FILTER(SEQLEN(?r) > 1) }",
            // ... bound before: to a stored run, to runs the store does not hold, and to one with a slot
            "SELECT * { { BIND(SEQ(1, 2) AS ?r) } UNION { BIND(SEQ(4, 4) AS ?r) } UNION { BIND(SEQ(1, ?none) AS ?r) }"
                    + " ?s :p ( ...?r 3 ) }",
            "SELECT * { { BIND(SEQ(7) AS ?r) } ?s :p ( ...?r ...?r ) }",
            "SELECT * { { BIND(SEQ(7) AS ?r) } UNION { BIND(SEQ() AS ?r) } ?s :p ( ...?r ... ) }",
            // nested patterns
            "SELECT * { ?s :p ( ( ?a ?b ) ... ) }",
            "SELECT * { ?s :p ( ... ( ?a ... ) ... ) }",
            "SELECT * { ?s :r ( 1 ( ?x ?y ) ) }",
            "SELECT * { ?s :r ( ?x ( ... ?y ) ) }",
            "SELECT * { ?s :p ( ?a ?b <<( ?t :q ?o )>> ) }",
            "SELECT * { ?s :p ( ... <<( :a ?q ?o )>> ) }",
            // the subject and the predicate inside the pattern
            "SELECT * { ?s :p ( ?s ) }",
            "SELECT * { ?s :p ( ?s ... ) }",
            "SELECT * { ?s ?p ( ?p ) }",
            "SELECT * { ?s ?p ( ... ?x ... ) }",
            "SELECT * { :b :p ( ?x ... ) }",
            "SELECT * { :b ?p ( ?x ... ) }",
            // joins, bound variables and slots
            "SELECT * { ?s :p ( ?x ) . ?y :q ?x }",
            "SELECT * { ?y :q ?x . ?s :p ( ?x ) }",
            "SELECT * { ?s :p ( ?x ?y ) . ?t :q ?x }",
            "SELECT * { ?s :p ( ?x ) OPTIONAL { ?y :q ?x } }",
            "SELECT * { ?y :q ?x OPTIONAL { ?s :p ( ?x ... ) } }",
            "SELECT * { VALUES ?x { 5 99 1 } ?s :p ( ?x ) }",
            "SELECT * { VALUES ?x { 5 99 1 } ?s :p ( ?x ?y ) }",
            "SELECT * { VALUES ?x { 99 } ?s :p ( ?x undef ) }",
            "SELECT * { ?s :p ( ?x ) . ?s :p ( ?y ) }",
            "SELECT * { ?s :p ( ?x ... ) . ?t :p ( ... ?x ) }",
            // filters, with and without slots
            "SELECT * { ?s :p ( ?x ... ) FILTER(?x > 1) }",
            "SELECT * { ?s :p ( ?x ?y ) . ?t :q ?y FILTER(?y > 1) }",
            "SELECT * { ?s :p ( ?x ?y ) . ?t :q ?x FILTER(BOUND(?x)) }",
            "SELECT * { ?s :p ( ?x ?y ) FILTER(!BOUND(?x)) }",
            // aggregates, DISTINCT, ASK
            "SELECT ?x (COUNT(*) AS ?n) { ?s :p ( ... ?x ... ) } GROUP BY ?x",
            "SELECT DISTINCT ?x { ?s :p ( ... ?x ... ) }",
            "SELECT (COUNT(*) AS ?n) { ?s ?p ( ... ) }",
            "ASK { ?s :p ( 1 2 ... ) }",
            "ASK { ?s :p ( 42 ... ) }",
            // graphs: one triple in two graphs counts once in their union
            "SELECT * { GRAPH ?g { ?s :p ( ?x ... ) } }",
            "SELECT * { GRAPH ?g { ?s :p ( ... ?x ... ) } }",
            "SELECT * FROM :g1 FROM :g2 { ?s :p ( ... ?x ... ) }",
            "SELECT * FROM :g1 FROM :g2 { ?s :p ( ?x ) }",
            "SELECT * FROM :g1 FROM :g2 { VALUES ?x { 5 } ?s :p ( ?x ) }",
            "SELECT * FROM :g1 FROM :g2 { ?s :p ( ...?r ) }",
            "SELECT * { GRAPH <urn:x-arq:UnionGraph> { ?s :p ( ... ?x ... ) } }",
            "SELECT * { GRAPH <urn:x-arq:UnionGraph> { VALUES ?x { 5 } ?s :p ( ?x ) } }",
            "SELECT * { ?s :p ( ?x ) GRAPH ?g { ?t :p ( ?x ) } }",
            "SELECT * { GRAPH :g2 { ?s :p ( ?x ?x ?x ) } }");
    }

    @BeforeAll
    static void build() throws Exception {
        Path src = dir.resolve("parity.trig");
        Files.writeString(src, DATA);
        File out = dir.resolve("parity.h5").toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(out).build().write();
        store = BG.getBeakGraph(out);
    }

    /** The queries with rows: two routes that agree on empty answers have shown nothing. */
    private static final java.util.concurrent.atomic.AtomicInteger ANSWERED = new java.util.concurrent.atomic.AtomicInteger();
    private static final java.util.concurrent.atomic.AtomicInteger RAN = new java.util.concurrent.atomic.AtomicInteger();

    @AfterAll
    static void close() throws Exception {
        if (store != null) {
            store.close();
        }
        long queries = queries().count();
        if (RAN.get() == queries) {   // the whole matrix ran, not one query picked out
            assertTrue(ANSWERED.get() >= queries - 5, ANSWERED.get() + " of " + queries + " queries have rows");
        }
    }

    @ParameterizedTest
    @MethodSource("queries")
    void matchingOnIdsAnswersAsJenaArray(String body) {
        Query query = QueryFactory.create("PREFIX : <http://ex.org/>\n" + body, Syntax.syntaxARQ);
        long delegated = PatternMatchBG.DELEGATED.get();
        Object onIds = run(query, false);
        assertEquals(delegated, PatternMatchBG.DELEGATED.get(), "matching on ids handed a BGP to jena-array's solver");
        Object reference = run(query, true);
        RAN.incrementAndGet();
        assertTrue(PatternMatchBG.DELEGATED.get() > delegated, "the reference run did not go to jena-array's solver");
        if (onIds instanceof ResultSetRewindable a && reference instanceof ResultSetRewindable r) {
            if (r.size() > 0) {
                ANSWERED.incrementAndGet();
            }
            boolean same = ResultsCompare.equalsByTerm(r, a);
            r.reset();
            a.reset();
            assertTrue(same, "rows differ\n--- jena-array:\n" + ResultSetFormatter.asText(r)
                    + "--- on ids:\n" + ResultSetFormatter.asText(a));
        } else {
            assertEquals(reference, onIds);
        }
    }

    /** The answer, read whole: a rewindable result set, or ASK's boolean. */
    private static Object run(Query query, boolean delegate) {
        String before = System.getProperty(PatternMatchBG.DELEGATE_PROPERTY);
        System.setProperty(PatternMatchBG.DELEGATE_PROPERTY, Boolean.toString(delegate));
        try (QueryExecution qe = QueryExecution.dataset(store.getDataset()).query(query).build()) {
            if (query.isAskType()) {
                return qe.execAsk();
            }
            return ResultSetFactory.makeRewindable(qe.execSelect());
        } finally {
            if (before == null) {
                System.clearProperty(PatternMatchBG.DELEGATE_PROPERTY);
            } else {
                System.setProperty(PatternMatchBG.DELEGATE_PROPERTY, before);
            }
        }
    }
}
