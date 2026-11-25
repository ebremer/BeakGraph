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
 *
 * <p>And so do they over the same data built with the element index
 * ({@code -seqindex}, RDFARRY.md step 8), read for every scan it has
 * candidates for ({@code beakgraph.seqindex.ratio=0}): a scan of the
 * sequences an element names answers as a scan of them all.
 */
class SequenceMatcherParityTest {

    @TempDir
    static Path dir;

    private static BeakGraph store;
    private static BeakGraph indexed;

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
            "SELECT * { GRAPH :g2 { ?s :p ( ?x ?x ?x ) } }",
            // containment, which the element index answers: constants, undef and bound variables after a gap
            "SELECT * { ?s :p ( ... 7 ... ) }",
            "SELECT * { ?s :p ( ... 2 ... ) }",
            "SELECT * { ?s :p ( ... undef ... ) }",
            "SELECT * { ?s :p ( ?x ... 3 ... ) }",
            "SELECT * { ?s :p ( ... :b ... ) }",
            "SELECT * { ?s :p ( ... ( 1 2 ) ... ) }",
            "SELECT * { ?s :p ( ... 42 ... ) }",
            "SELECT * { ?y :q ?m . ?s :p ( ... ?m ... ) }",
            "SELECT * { ?y :q ?m . ?s :p ( ?m ... ) }",
            "SELECT * { ?y :q ?m . ?s :p ( ... ?m ) }",
            "SELECT * { ?y :q ?m . ?s :p ( ... ?m undef ... ) }",
            "SELECT * { ?s :p ( ... ?m ... ) . ?y :q ?m }",
            "SELECT * { :b :q ?m . ?s :p ( ... ?m ... ) }",
            "SELECT * { :i :q ?m . ?s :p ( ... ?m ... ) }",
            "SELECT * { ?y :q ?m . ?s :p ( ...?r ?m ) }",
            "SELECT * { GRAPH ?g { ?s :p ( ... 7 ... ) } }",
            "SELECT * FROM :g1 FROM :g2 { ?s :p ( ... 5 ... ) }",
            "SELECT * FROM :g1 FROM :g2 { ?s :p ( ... undef ... ) }",
            "SELECT * { GRAPH <urn:x-arq:UnionGraph> { ?s :p ( ... 7 ... ) } }",
            "SELECT * { :e :q ?m GRAPH <urn:x-arq:UnionGraph> { ?s :p ( ... ?m ... ) } }",
            "SELECT * { :e :q ?m . GRAPH ?g { ?s :p ( ... ?m ... ) } }");
    }

    @BeforeAll
    static void build() throws Exception {
        Path src = dir.resolve("parity.trig");
        Files.writeString(src, DATA);
        File out = dir.resolve("parity.h5").toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(out).build().write();
        store = BG.getBeakGraph(out);
        File withIndex = dir.resolve("parity-seqindex.h5").toFile();
        HDF5Writer.Builder().setSource(src.toFile()).setDestination(withIndex).setSequenceIndex(true).build().write();
        indexed = BG.getBeakGraph(withIndex);
    }

    /** The queries with rows: two routes that agree on empty answers have shown nothing. */
    private static final java.util.concurrent.atomic.AtomicInteger ANSWERED = new java.util.concurrent.atomic.AtomicInteger();
    private static final java.util.concurrent.atomic.AtomicInteger RAN = new java.util.concurrent.atomic.AtomicInteger();

    /** The queries whose index run read the element index's candidates. */
    private static final java.util.concurrent.atomic.AtomicInteger INDEXED = new java.util.concurrent.atomic.AtomicInteger();

    @AfterAll
    static void close() throws Exception {
        if (store != null) {
            store.close();
        }
        if (indexed != null) {
            indexed.close();
        }
        long queries = queries().count();
        if (RAN.get() == queries) {   // the whole matrix ran, not one query picked out
            assertTrue(ANSWERED.get() >= queries - 7, ANSWERED.get() + " of " + queries + " queries have rows");
            assertTrue(INDEXED.get() >= 25, "only " + INDEXED.get() + " of " + queries + " queries read the element index");
        }
    }

    @ParameterizedTest
    @MethodSource("queries")
    void matchingOnIdsAnswersAsJenaArray(String body) {
        Query query = QueryFactory.create("PREFIX : <http://ex.org/>\n" + body, Syntax.syntaxARQ);
        long delegated = PatternMatchBG.DELEGATED.get();
        Object onIds = run(store, query, false);
        long scans = com.ebremer.beakgraph.hdf5.jena.BGIteratorPOS.CANDIDATE_SCANS.sum();
        Object onIndex = run(indexed, query, false);
        if (com.ebremer.beakgraph.hdf5.jena.BGIteratorPOS.CANDIDATE_SCANS.sum() > scans) {
            INDEXED.incrementAndGet();
        }
        assertEquals(delegated, PatternMatchBG.DELEGATED.get(), "matching on ids handed a BGP to jena-array's solver");
        Object reference = run(store, query, true);
        RAN.incrementAndGet();
        assertTrue(PatternMatchBG.DELEGATED.get() > delegated, "the reference run did not go to jena-array's solver");
        if (reference instanceof ResultSetRewindable r) {
            if (r.size() > 0) {
                ANSWERED.incrementAndGet();
            }
        }
        same(reference, onIds, "on ids");
        same(reference, onIndex, "on ids, through the element index");
    }

    private static void same(Object reference, Object answer, String route) {
        if (answer instanceof ResultSetRewindable a && reference instanceof ResultSetRewindable r) {
            boolean same = ResultsCompare.equalsByTerm(r, a);
            r.reset();
            a.reset();
            assertTrue(same, "rows differ\n--- jena-array:\n" + ResultSetFormatter.asText(r)
                    + "--- " + route + ":\n" + ResultSetFormatter.asText(a));
            r.reset();
            a.reset();
        } else {
            assertEquals(reference, answer, route);
        }
    }

    /**
     * The answer, read whole: a rewindable result set, or ASK's boolean. The element index, where the store
     * has one, is read for every scan it has candidates for.
     */
    private static Object run(BeakGraph g, Query query, boolean delegate) {
        String before = System.getProperty(PatternMatchBG.DELEGATE_PROPERTY);
        String ratio = System.getProperty("beakgraph.seqindex.ratio");
        System.setProperty(PatternMatchBG.DELEGATE_PROPERTY, Boolean.toString(delegate));
        System.setProperty("beakgraph.seqindex.ratio", "0");
        try (QueryExecution qe = QueryExecution.dataset(g.getDataset()).query(query).build()) {
            if (query.isAskType()) {
                return qe.execAsk();
            }
            return ResultSetFactory.makeRewindable(qe.execSelect());
        } finally {
            restore(PatternMatchBG.DELEGATE_PROPERTY, before);
            restore("beakgraph.seqindex.ratio", ratio);
        }
    }

    private static void restore(String key, String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }
}
