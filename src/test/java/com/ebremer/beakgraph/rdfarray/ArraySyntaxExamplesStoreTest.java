/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Adapted for BeakGraph from Apache Jena's rdf-array branch (Apache License
 * 2.0, the ASF header above retained): the cases and assertions are the
 * fork's; the data are BeakGraph stores built from the same documents, and
 * the queries run through each store's dataset.
 */

package com.ebremer.beakgraph.rdfarray;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.shared.impl.PrefixMappingImpl;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.exec.QueryExec;
import org.apache.jena.sparql.exec.RowSet;
import org.apache.jena.sparql.util.FmtUtils;
import org.junit.jupiter.api.Test;

/**
 * The worked examples of the array-syntax design note (rdf-array/docs/array-syntax.md, section 4.1):
 * gaps, named gaps, UNNEST, indexes, slices and IN over nested sequence data. Each query's rows
 * are the note's.
 */
class ArraySyntaxExamplesStoreTest {

    @org.junit.jupiter.api.io.TempDir
    static java.nio.file.Path storeDir;
    private static RdfArrayStores STORES;

    @org.junit.jupiter.api.BeforeAll
    static void openStores() {
        STORES = new RdfArrayStores(storeDir);
    }

    @org.junit.jupiter.api.AfterAll
    static void closeStores() throws Exception {
        STORES.closeAll();
    }

    static final String PREFIX = "PREFIX : <http://ex/>\nPREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n";

    /** The data of section 4, in Turtle-array. */
    static final String DATA = PREFIX
            + ":obs1 :component ( ( \"8480-6\" \"systolic\"  ( 120 \"mm[Hg]\" ) )\n"
            + "                   ( \"8462-4\" \"diastolic\" (  80 \"mm[Hg]\" ) ) ) .\n"
            + ":obs2 :component ( ( \"8480-6\" \"systolic\"  ( 150 \"mm[Hg]\" ) )\n"
            + "                   ( \"8462-4\" \"diastolic\" (  95 \"mm[Hg]\" ) ) ) .\n"
            + ":obs3 :component ( ( \"8480-6\" \"systolic\"  ( \"high\" \"mm[Hg]\" ) ) ) .\n"
            + ":m1   :matrix ( ( 1 2 3 ) ( 4 5 6 ) ) .\n"
            + ":m2   :matrix ( ( 1 2 3 ) ( 4 5 ) ) .\n"
            + ":poly :points ( ( 0 0 ) ( 3 0 ) ( 3 4 ) ) .\n"
            + ":kv   :pairs  ( \"a\" 1 \"b\" 2 \"c\" 3 ) .\n"
            + ":tree :node   ( \"root\" ( ( \"left\" ( ) ) ( \"right\" ( ( \"leaf\" ( ) ) ) ) ) ) .\n";

    static Graph data() {
        return STORES.graph(DATA, Lang.TURTLE);
    }

    private static final PrefixMapping PM = new PrefixMappingImpl().setNsPrefix("", "http://ex/");

    /** Run a SELECT query; each row as "v1 | v2 | ..." over the query's result variables, unbound as "". */
    static List<String> rows(String queryString) {
        Query query = QueryFactory.create(PREFIX + queryString);
        List<String> rows = new ArrayList<>();
        try ( QueryExec qExec = RdfArrayStores.exec(data()).query(query).build() ) {
            RowSet rs = qExec.select();
            while ( rs.hasNext() ) {
                Binding b = rs.next();
                StringBuilder sb = new StringBuilder();
                for ( String v : query.getResultVars() ) {
                    if ( sb.length() > 0 )
                        sb.append(" | ");
                    Node n = b.get(v);
                    sb.append(n == null ? "" : FmtUtils.stringForNode(n, PM));
                }
                rows.add(sb.toString());
            }
        }
        return rows;
    }

    /** Compare rows as multisets (the note's tables are not ordered). */
    static void check(String queryString, String... expected) {
        List<String> actual = new ArrayList<>(rows(queryString));
        List<String> exp = new ArrayList<>(List.of(expected));
        Collections.sort(actual);
        Collections.sort(exp);
        assertEquals(exp, actual);
    }

    @Test public void tuple_anywhere_in_a_list_of_tuples() {
        check("SELECT ?obs ?v ?unit WHERE { ?obs :component ( ... ( \"8480-6\" ?name ( ?v ?unit ) ) ... ) }",
              ":obs1 | 120 | \"mm[Hg]\"",
              ":obs2 | 150 | \"mm[Hg]\"",
              ":obs3 | \"high\" | \"mm[Hg]\"");
    }

    @Test public void first_and_last_member_nested() {
        check("SELECT ?x0 ?last WHERE { :poly :points ( ( ?x0 ?y0 ) ... ?last ) }",
              "0 | ( 3 4 )");
    }

    @Test public void head_and_tail() {
        check("SELECT ?h ?t ?t2 WHERE { :poly :points ( ?h ...?t ) . :poly :points ?pts BIND(?pts[2...] AS ?t2) }",
              "( 0 0 ) | ( ( 3 0 ) ( 3 4 ) ) | ( ( 3 0 ) ( 3 4 ) )");
    }

    @Test public void position_from_a_pattern() {
        check("SELECT ?x ?i WHERE { :poly :points ( ...?pre ?x ... ) BIND(SEQLEN(?pre) + 1 AS ?i) }",
              "( 0 0 ) | 1", "( 3 0 ) | 2", "( 3 4 ) | 3");
    }

    @Test public void gaps_at_two_levels_of_recursive_data() {
        check("SELECT ?child ?grandchild WHERE { :tree :node ( \"root\" ( ... ( ?child ( ... ( ?grandchild ?kids ) ... ) ) ... ) ) }",
              "\"right\" | \"leaf\"");
    }

    @Test public void unnest_twice() {
        check("SELECT ?m ?i ?j ?x WHERE { ?m :matrix ?rows . UNNEST(?rows AS ?row AT ?i) UNNEST(?row AS ?x AT ?j) }",
              ":m1 | 1 | 1 | 1", ":m1 | 1 | 2 | 2", ":m1 | 1 | 3 | 3",
              ":m1 | 2 | 1 | 4", ":m1 | 2 | 2 | 5", ":m1 | 2 | 3 | 6",
              ":m2 | 1 | 1 | 1", ":m2 | 1 | 2 | 2", ":m2 | 1 | 3 | 3",
              ":m2 | 2 | 1 | 4", ":m2 | 2 | 2 | 5");
    }

    @Test public void unnest_then_index_into_each_tuple() {
        check("SELECT ?obs ?i ?code ?v WHERE { ?obs :component ?c . UNNEST(?c AS ?comp AT ?i)"
              + " BIND(?comp[1] AS ?code) BIND(?comp[3][1] AS ?v) FILTER(?v >= 90) }",
              ":obs1 | 1 | \"8480-6\" | 120",
              ":obs2 | 1 | \"8480-6\" | 150",
              ":obs2 | 2 | \"8462-4\" | 95");
    }

    @Test public void slice_of_every_row() {
        check("SELECT ?m ?i ?cols WHERE { ?m :matrix ?rows . UNNEST(?rows AS ?row AT ?i) BIND(?row[2...3] AS ?cols) }",
              ":m1 | 1 | ( 2 3 )", ":m1 | 2 | ( 5 6 )", ":m2 | 1 | ( 2 3 )", ":m2 | 2 | ( 5 )");
    }

    @Test public void stride() {
        check("SELECT ?k ?v WHERE { :kv :pairs ?p . UNNEST(?p AS ?k AT ?i) BIND(?p[?i + 1] AS ?v) FILTER(?i - 2 * FLOOR(?i / 2) = 1) }",
              "\"a\" | 1", "\"b\" | 2", "\"c\" | 3");
    }

    @Test public void chained_indexes() {
        check("SELECT ?m ?cell ?len WHERE { ?m :matrix ?rows . BIND(?rows[2][3] AS ?cell) BIND(SEQLEN(?rows[2]) AS ?len) }",
              ":m1 | 6 | 3", ":m2 |  | 2");
    }

    @Test public void a_sequence_as_the_needle() {
        check("SELECT ?s WHERE { ?s :points ?pts FILTER(SEQ(3, 4) IN ?pts) }", ":poly");
    }

    @Test public void whole_nested_values() {
        check("SELECT ?m ?n WHERE { ?m :matrix ?rows FILTER(?rows = SEQ(SEQ(1, 2, 3), SEQ(4, 5, 6))) ?n :matrix ( ( 1 2 3 ) ( 4 5 6 ) ) }",
              ":m1 | :m1");
    }

    // Section 5: edge cases.

    @Test public void no_stride_in_patterns() {
        // Every adjacent pair, ("b" 2) as well as (1 "b").
        assertEquals(5, rows("SELECT ?k ?v WHERE { :kv :pairs ( ... ?k ?v ... ) }").size());
    }

    @Test public void empty_sequence_in_pattern_and_data() {
        check("SELECT ?label WHERE { :tree :node ( \"root\" ( ... ( ?label () ) ... ) ) }", "\"left\"");
    }
}
