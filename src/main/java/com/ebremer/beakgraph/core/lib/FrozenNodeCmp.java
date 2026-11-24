/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 *   SPDX-License-Identifier: Apache-2.0
 */

package com.ebremer.beakgraph.core.lib;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

/**
 * The order of RDF terms "by syntax" exactly as Apache Jena 6.2.0's
 * {@code org.apache.jena.sparql.util.NodeCmp.compareRDFTerms} gave it, kept
 * inside BeakGraph so that the format's term order (SPECIFICATIONS.md §6)
 * cannot move when Jena does. Adapted from that class (and the
 * {@code isSimpleString} / {@code isLangString} tests of Jena's
 * {@code rdf.model.impl.Util}); its behaviour is unchanged.
 * <p>
 * Why a copy: dictionary ids ARE ranks in BeakGraph's order, and that order
 * delegated these comparisons to Jena. Jena 6.3.0 switched them to Unicode
 * code-point order (GH-4300, GH-4365) and regrouped its literal order around
 * rdf:dirLangString. A store built under 6.2.0 would then hold its terms in
 * an order the reader no longer searches in, and lookups of strings mixing a
 * supplementary character with one in U+E000-U+FFFF would miss. Strings here
 * compare by UTF-16 code unit ({@code String.compareTo}), the order §2
 * specifies.
 * <p>
 * Answers -1, 0 or +1. Two distinct rdf:dirLangString literals with the same
 * lexical form can answer 0, as in 6.2.0 (it read them as datatyped literals);
 * {@link NodeComparator#compareExactLiteralTerms} breaks that tie.
 */
final class FrozenNodeCmp {

    private static final int LESS = -1;
    private static final int EQUAL = 0;
    private static final int GREATER = 1;
    private static final int INDETERMINATE = 2;

    private FrozenNodeCmp() {}

    /** Jena 6.2.0's {@code NodeCmp.compareRDFTerms}. */
    static int compareRDFTerms(Node node1, Node node2) {
        if (node1 == null) {
            return (node2 == null) ? EQUAL : LESS;
        }
        if (node2 == null) {
            return GREATER;
        }
        if (node1.isLiteral() && node2.isLiteral()) {
            return compareLiteralsBySyntax(node1, node2);
        }
        // Variables < blank nodes < IRIs < literals < triple terms
        if (node1.isVariable()) {
            return node2.isVariable() ? strCompare(node1.getName(), node2.getName()) : LESS;
        }
        if (node2.isVariable()) {
            return GREATER;
        }
        if (node1.isBlank()) {
            return node2.isBlank() ? strCompare(node1.getBlankNodeLabel(), node2.getBlankNodeLabel()) : LESS;
        }
        if (node2.isBlank()) {
            return GREATER;
        }
        if (node1.isURI()) {
            return node2.isURI() ? strCompare(node1.getURI(), node2.getURI()) : LESS;
        }
        if (node2.isURI()) {
            return GREATER;
        }
        if (node2.isLiteral()) {
            return GREATER;
        }
        if (node1.isLiteral()) {
            return LESS;
        }
        if (node1.isTripleTerm()) {
            if (node2.isTripleTerm()) {
                Triple t1 = node1.getTriple();
                Triple t2 = node2.getTriple();
                int x = compareRDFTerms(t1.getSubject(), t2.getSubject());
                if (x != EQUAL) {
                    return x;
                }
                x = compareRDFTerms(t1.getPredicate(), t2.getPredicate());
                if (x != EQUAL) {
                    return x;
                }
                return compareRDFTerms(t1.getObject(), t2.getObject());
            }
        }
        if (node2.isTripleTerm()) {
            return GREATER;
        }
        throw new IllegalArgumentException("Compare: " + node1 + "  " + node2);
    }

    /**
     * Two literals by kind, not by value: simple (xsd:string) < language-tagged
     * (rdf:langString) < the rest; the rest by lexical form, then datatype IRI.
     */
    private static int compareLiteralsBySyntax(Node node1, Node node2) {
        if (node1.equals(node2)) {
            return EQUAL;
        }
        boolean simple1 = isSimpleString(node1);
        boolean simple2 = isSimpleString(node2);
        if (simple1 && simple2) {
            return strCompare(node1.getLiteralLexicalForm(), node2.getLiteralLexicalForm());
        }
        if (simple1) {
            return LESS;
        }
        if (simple2) {
            return GREATER;
        }
        int x = compareRDFLangTerms(node1, node2);
        if (x != INDETERMINATE) {
            return x;
        }
        x = strCompare(node1.getLiteralLexicalForm(), node2.getLiteralLexicalForm());
        if (x != EQUAL) {
            return x;
        }
        return strCompare(node1.getLiteralDatatypeURI(), node2.getLiteralDatatypeURI());
    }

    /** May answer {@link #INDETERMINATE}: neither literal is an rdf:langString. */
    private static int compareRDFLangTerms(Node node1, Node node2) {
        boolean lang1 = isLangString(node1);
        boolean lang2 = isLangString(node2);
        if (lang1 && lang2) {
            String tag1 = node1.getLiteralLanguage();
            String tag2 = node2.getLiteralLanguage();
            int x = strCompareIgnoreCase(tag1, tag2);
            if (x != EQUAL) {
                return x;
            }
            x = strCompare(node1.getLiteralLexicalForm(), node2.getLiteralLexicalForm());
            if (x != EQUAL) {
                return x;
            }
            x = strCompare(tag1, tag2);
            if (x != EQUAL) {
                return x;
            }
            throw new IllegalStateException("Same lang tag (inc case), same lexical form but not node.equals: "
                    + node1 + " " + node2);
        }
        if (lang1) {
            return LESS;
        }
        if (lang2) {
            return GREATER;
        }
        return INDETERMINATE;
    }

    /** A plain literal or an xsd:string: no language tag, no other datatype. */
    private static boolean isSimpleString(Node n) {
        RDFDatatype dt = n.getLiteralDatatype();
        if (dt == null) {
            return !hasLanguage(n);
        }
        return dt.equals(XSDDatatype.XSDstring);
    }

    /** An rdf:langString: a language tag and no base direction. */
    private static boolean isLangString(Node n) {
        return hasLanguage(n) && n.getLiteralBaseDirection() == null;
    }

    private static boolean hasLanguage(Node n) {
        String lang = n.getLiteralLanguage();
        return lang != null && !lang.isEmpty();
    }

    /** UTF-16 code-unit order, as {@code String.compareTo}. */
    private static int strCompare(String s1, String s2) {
        return Integer.signum(s1.compareTo(s2));
    }

    private static int strCompareIgnoreCase(String s1, String s2) {
        return Integer.signum(s1.compareToIgnoreCase(s2));
    }
}
