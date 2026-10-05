package com.ebremer.beakgraph.core.lib;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.xml.datatype.DatatypeConstants;
import javax.xml.datatype.Duration;
import javax.xml.datatype.XMLGregorianCalendar;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.TextDirection;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.nodevalue.NodeValueNode;
import org.apache.jena.sparql.util.NodeCmp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enforces a strict total ordering of RDF nodes. CRITICAL for the HDF5 binary
 * searches: dictionary ids ARE comparator ranks, so the sort order must match
 * the dictionary's id assignment exactly (SPECIFICATIONS.md §6). Five kinds,
 * in this order:
 * <ol>
 *   <li>the default-graph sentinels ({@code null}, {@code urn:x-arq:DefaultGraph},
 *       {@code urn:x-arq:DefaultGraphNode}), before everything else;</li>
 *   <li>blank nodes (macro kind 1), by label;</li>
 *   <li>IRIs (2), by string;</li>
 *   <li>literals (3), by value - numerically, temporally, lexicographically -
 *       with an exact-term tie-break so value-equal distinct terms stay distinct;</li>
 *   <li>RDF 1.2 triple terms (4), structurally: subject, then predicate, then
 *       object, each through this comparator.</li>
 * </ol>
 * Language-tagged literals are compared on the tag as stored; see the note at
 * {@link #compareExactLiteralTerms} for the tag-formatting precondition.
 * See SPECIFICATIONS.md §6 (dictionary order).
 */
public class NodeComparator implements Comparator<Node> {

    public static final NodeComparator INSTANCE = new NodeComparator();

    private static final Logger logger = LoggerFactory.getLogger(NodeComparator.class);
    /** Datatype IRIs whose value construction has failed - warned once each. */
    private static final Set<String> WARNED_DATATYPES = ConcurrentHashMap.newKeySet();

    protected NodeComparator() {}

    /**
     * Node-to-NodeValue conversion hook. {@code NodeValue.makeNode} funnels
     * through Jena's ONE global bounded cache; under a multi-threaded sort of
     * literal-heavy data that cache's eviction lock becomes the bottleneck
     * (Caffeine "excessive wait times" warnings). Subclasses may override with
     * a local cache - the ordering semantics must remain exactly makeNode's.
     */
    protected NodeValue nodeValue(Node n) {
        return NodeValue.makeNode(n);
    }

    /**
     * The value of one literal, classified consistently against EVERY partner.
     * {@code NodeValue.makeNode} never throws for an ill-formed lexical form
     * (it yields a plain node value that compareAlways files in the last,
     * term-ordered cluster), so an exception here is abnormal - and it used to
     * be caught around the whole comparison, ordering that ONE pair by term
     * while every other pair involving the same literal compared by value:
     * exactly the pairwise value/term mix the CDT and language branches above
     * exist to avoid, cyclic, silent, and input-order dependent. The fallback
     * is now per literal: the literal becomes a plain node value - the same
     * classification an unparseable literal gets - for every comparison it
     * takes part in, and the datatype is logged once (BG-19).
     */
    private NodeValue valueOf(Node n) {
        try {
            return nodeValue(n);
        } catch (RuntimeException e) {
            String dt = n.getLiteralDatatypeURI();
            if (WARNED_DATATYPES.add(dt == null ? "" : dt)) {
                logger.warn("Cannot build a value for literal {} (datatype {}); literals of this datatype whose value "
                        + "fails to build are ordered as unparseable literals", n, dt, e);
            }
            return new NodeValueNode(n);
        }
    }

    @Override
    public int compare(Node n1, Node n2) {
        // 1. Handle the Default Graph first (Always ID 0 or implicitly lowest)
        boolean n1isDefault = isDefaultGraph(n1);
        boolean n2isDefault = isDefaultGraph(n2);

        if (n1isDefault && n2isDefault) {
            // Both ARQ sentinels rank before everything else, but they are two
            // DISTINCT RDF terms: returning 0 for the pair collapsed them onto
            // one dictionary id when a source used urn:x-arq:DefaultGraphNode as
            // a data term. Order by exact term (null ≡ the default graph IRI;
            // "urn:x-arq:DefaultGraph" still sorts first, keeping the graph
            // dictionary's lowest-id assumption).
            Node t1 = (n1 == null) ? Quad.defaultGraphIRI : n1;
            Node t2 = (n2 == null) ? Quad.defaultGraphIRI : n2;
            return NodeCmp.compareRDFTerms(t1, t2);
        }
        if (n1isDefault) return -1;
        if (n2isDefault) return 1;

        // 2. Enforce RDF Term Macro-Ordering (BNode < URI < Literal < TripleTerm)
        // This ensures the sorted array perfectly aligns with how IDs are chunked
        int type1 = getMacroType(n1);
        int type2 = getMacroType(n2);

        if (type1 != type2) {
            return Integer.compare(type1, type2);
        }

        // 3a. Both are triple terms (macro type 4): compare STRUCTURALLY through
        // this comparator, position by position. Delegating the pair to
        // NodeCmp.compareRDFTerms (as every other same-kind pair does below)
        // would re-import Finding 2 through the components: NodeCmp answers 0
        // for distinct rdf:dirLangString literals, so two triple terms
        // differing only in an embedded base direction would collapse onto one
        // dictionary id. Recursing through compare() gives components exactly
        // the orderings the dictionary already uses - the dirLang repair, the
        // CDT lexical short-circuit, the temporal total orders - and stays a
        // strict total order by induction (RDF 1.2 forbids cyclic terms).
        if (n1.isTripleTerm() && n2.isTripleTerm()) {
            org.apache.jena.graph.Triple t1 = n1.getTriple();
            org.apache.jena.graph.Triple t2 = n2.getTriple();
            int c = compare(t1.getSubject(), t2.getSubject());
            if (c != 0) {
                return c;
            }
            c = compare(t1.getPredicate(), t2.getPredicate());
            if (c != 0) {
                return c;
            }
            return compare(t1.getObject(), t2.getObject());
        }

        // 3. Both nodes are the SAME RDF Term Type.
        // If they are both Literals, we must sort by actual Value (e.g. 2 < 10), not String ("10" < "2")
        if (n1.isLiteral() && n2.isLiteral()) {
            // Composite (cdt:List / cdt:Map) literals never go through compareAlways:
            // there, same-datatype pairs compare by VALUE while any pair touching an
            // ill-formed literal fell back to TERM order, and mixing the two orders
            // is cyclic ("[9]" < "[10]" < "[5" < "[9]") - the same non-transitivity
            // hazard as the temporal spaces below, i.e. a build-order-dependent
            // dictionary. CDT has no canonical form, so term identity IS lexical
            // identity: (datatype IRI, lexical form) is a self-consistent total
            // order, and it skips a value parse that costs O(list length) inside
            // every dictionary binary-search probe. The cross-datatype direction
            // (List before Map) matches compareAlways' value-space rank, and MIXED
            // pairs (one composite, one not) stay on compareAlways, whose
            // classification is uniform by datatype - verified: ill-formed and
            // well-formed composites rank identically against every other value
            // space, without throwing.
            if (CdtTerms.isComposite(n1) && CdtTerms.isComposite(n2)) {
                int byDatatype = n1.getLiteralDatatypeURI().compareTo(n2.getLiteralDatatypeURI());
                if (byDatatype != 0) {
                    return byDatatype;
                }
                return n1.getLiteralLexicalForm().compareTo(n2.getLiteralLexicalForm());
            }

            // Language-tagged pairs (rdf:langString / rdf:dirLangString) never go
            // through compareAlways either. The tag is compared CASE-SENSITIVELY
            // (String.compareTo), which is only a total order over terms because
            // Jena canonicalizes every tag at construction: NodeFactory
            // .createLiteralLang / createLiteralDirLang run LangTagX
            // .formatLanguageTag, so "EN-us" and "en-US" are the SAME Node and
            // case-variant spellings never reach this comparator as distinct
            // terms. Building language-tagged Nodes any other way (LiteralLabel
            // Factory, Node_Literal directly - writer keys, reader, probe keys)
            // would break that precondition and with it the dictionary order;
            // NodeComparatorDirLangTest#jenaTagFormattingStillPresent is the
            // canary (BG-370). Jena 6.1.0's base-direction support
            // there is incoherent - same-(lex,lang) cross-kind or cross-direction
            // pairs THROW, different-language dirLangString pairs answer 0 for
            // DISTINCT terms, and mixed-kind pairs flip between lang-first and
            // lex-first ordering, which is cyclic against the clean (lang, lex)
            // order plain langString pairs get. Compare explicitly on
            // (language tag, lexical form, base direction) instead: for two plain
            // langString literals that is exactly the (lang, lex) order
            // compareAlways already produces - this branch only EXTENDS it to base
            // directions - and every language-kinded literal occupies the same
            // value-space rank against other spaces (verified), so pairs with only
            // one language-kinded side stay on compareAlways safely.
            if (hasLanguage(n1) && hasLanguage(n2)) {
                int c = n1.getLiteralLanguage().compareTo(n2.getLiteralLanguage());
                if (c != 0) {
                    return c;
                }
                c = n1.getLiteralLexicalForm().compareTo(n2.getLiteralLexicalForm());
                if (c != 0) {
                    return c;
                }
                return Integer.compare(directionRank(n1), directionRank(n2));
            }
            NodeValue nv1 = valueOf(n1);
            NodeValue nv2 = valueOf(n2);
            try {
                // Timezone-sensitive value spaces cannot go through compareAlways:
                // it answers value order for XSD-determinate pairs but silently falls
                // back to TERM order for indeterminate ones (a timezone-less dateTime
                // vs a timezoned one within +/-14h; a month-based duration vs a
                // day-based one). Mixing the two orders pairwise is not transitive -
                // e.g. "2020-01-02T00:00:00" < "2020-01-02T08:00:00+14:00" <
                // "2020-01-01T20:00:00Z" < the first - and a cyclic comparator breaks
                // every sort and binary search in the store (locate() misses stored
                // literals; builds fail with "Cannot resolve Object"). These spaces
                // get a self-consistent total order below that agrees with XSD value
                // order wherever XSD order is determinate, so the ValueCluster range
                // pushdown stays over-inclusive and value-equal terms stay adjacent.
                int group = temporalGroup(nv1);
                if (group != 0 && group == temporalGroup(nv2)) {
                    if (group == GROUP_DURATION) {
                        return compareDurationTotal(nv1, nv2, n1, n2);
                    }
                    if (group == GROUP_INSTANT) {
                        // One ARQ value space holds dateTime and the g* kinds;
                        // a fixed kind rank keeps cross-kind pairs off the
                        // lexical fallback compareAlways would give them.
                        int byKind = Integer.compare(instantKindRank(nv1), instantKindRank(nv2));
                        if (byKind != 0) {
                            return byKind;
                        }
                    }
                    return compareTemporalTotal(nv1, nv2, n1, n2);
                }

                // Numbers of ANY XSD numeric datatype: exact value order. The
                // SPARQL promotion compareAlways applies to mixed pairs (decimal
                // vs float as floats) is lossy, and mixing it with the exact
                // decimal-vs-decimal order and a lexical tie-break was cyclic.
                // Value-equal terms fall to the exact-term tie-break below.
                if (nv1.isNumber() && nv2.isNumber()) {
                    int byNumber = NumericOrder.compare(nv1, nv2);
                    if (byNumber != 0) {
                        return byNumber;
                    }
                    return compareExactLiteralTerms(n1, n2);
                }

                // compareAlways provides a strict SPARQL "ORDER BY" ordering by VALUE,
                // handling mixed datatypes safely without throwing exceptions. Across
                // distinct value spaces (number vs string vs dateTime vs date ...) it
                // orders by a fixed value-space rank independent of the actual values,
                // so it stays transitive there.
                int byValue = NodeValue.compareAlways(nv1, nv2);
                if (byValue != 0) {
                    return byValue;
                }
                // Value-equal but possibly term-distinct (e.g. "1"^^xsd:int vs
                // "1"^^xsd:integer, "1.0" vs "1.00", or an int equal to a double).
                // Break the tie on the exact RDF term so distinct terms get distinct,
                // stable dictionary positions instead of collapsing onto one id (which
                // would make locate() return the wrong term).
                return compareExactLiteralTerms(n1, n2);
            } catch (RuntimeException e) {
                // No silent per-pair fallback (see valueOf): a comparison that
                // fails is an error, and a failed build beats an input-order
                // dependent dictionary whose lookups miss stored terms.
                throw new IllegalStateException("Cannot order literals " + n1 + " and " + n2 + ": " + e, e);
            }
        }

        // 4. If they are both URIs or both BNodes, fallback to Jena's standard lexicographical sort
        return NodeCmp.compareRDFTerms(n1, n2);
    }

    /**
     * Final tie-break on the exact RDF term for the literal path. Delegates to
     * Jena's {@code NodeCmp}, then repairs one Jena 6.1.0 gap: {@code
     * compareRDFTerms} answers 0 for DISTINCT {@code rdf:dirLangString}
     * literals - {@code Util.isLangString} is false when a base direction is
     * present, so the comparison falls through to lexical form + datatype URI
     * and ignores both the language tag and the direction. A comparator
     * answering "equal" for non-equal terms collapses them onto one dictionary
     * id (ids ARE comparator ranks), so the remaining tie is broken on
     * (language tag, base direction). The refinement only splits pairs NodeCmp
     * already considers equal, so it cannot disturb the order of any other
     * pair. Simplify when NodeCmp orders dirLangString correctly upstream -
     * NodeComparatorDirLangTest#jenaNodeCmpGapStillPresent is the canary.
     * <p>
     * Precondition, here and in the language branch of {@link #compare}: the
     * language tag is compared case-sensitively, which is a strict total order
     * over terms only because {@code NodeFactory.createLiteralLang} /
     * {@code createLiteralDirLang} canonicalize every tag through
     * {@code LangTagX.formatLanguageTag} at construction, so two spellings of
     * one tag are one Node. Never build language-tagged Nodes through
     * {@code LiteralLabelFactory} or {@code Node_Literal} directly (BG-370).
     */
    private static int compareExactLiteralTerms(Node n1, Node n2) {
        int c = NodeCmp.compareRDFTerms(n1, n2);
        if (c != 0 || n1.equals(n2)) {
            return c;
        }
        String lang1 = n1.getLiteralLanguage();
        String lang2 = n2.getLiteralLanguage();
        c = ((lang1 == null) ? "" : lang1).compareTo((lang2 == null) ? "" : lang2);
        if (c != 0) {
            return c;
        }
        return Integer.compare(directionRank(n1), directionRank(n2));
    }

    /** absent < ltr < rtl - any fixed order works; it only needs to be total and stable. */
    private static int directionRank(Node n) {
        TextDirection d = n.getLiteralBaseDirection();
        if (d == null) {
            return 0;
        }
        return (d == TextDirection.LTR) ? 1 : 2;
    }

    private static boolean hasLanguage(Node n) {
        String lang = n.getLiteralLanguage();
        return lang != null && !lang.isEmpty();
    }

    private static final int GROUP_INSTANT = 1;
    private static final int GROUP_DURATION = 9;

    /**
     * Classifies a literal into one of the timezone-sensitive temporal value
     * spaces (or 0 for everything else), mirroring Jena's own value spaces:
     * dateTime, dateTimeStamp and the five g* kinds share ONE space
     * (ARQ's VSPACE_DATETIME), date and time have their own, duration its own.
     * The grouping used to give each g* kind a space of its own, which sent a
     * gYear-vs-dateTime pair to compareAlways; there the shared space made
     * the pair "not comparable" and fell back to lexical order - mixed with
     * the instant order of same-kind pairs, a cycle. Within the instant space
     * a fixed kind rank ({@link #instantKindRank}) orders cross-kind pairs and
     * same-kind pairs compare as instants. Ill-formed literals answer false
     * to all predicates and stay on the compareAlways path.
     */
    private static int temporalGroup(NodeValue nv) {
        if (nv.isDateTime() || nv.isGYear() || nv.isGYearMonth()
                || nv.isGMonth() || nv.isGMonthDay() || nv.isGDay()) {
            return GROUP_INSTANT;
        }
        if (nv.isDate())       return 2;
        if (nv.isTime())       return 3;
        if (nv.isDuration())   return GROUP_DURATION;
        return 0;
    }

    /** gDay < gMonth < gMonthDay < gYear < gYearMonth < dateTime (the rank SPECIFICATIONS.md 6.2 documents). */
    private static int instantKindRank(NodeValue nv) {
        if (nv.isGDay())       return 0;
        if (nv.isGMonth())     return 1;
        if (nv.isGMonthDay())  return 2;
        if (nv.isGYear())      return 3;
        if (nv.isGYearMonth()) return 4;
        return 5;
    }

    /**
     * Total order for two temporal values of the same value space: a missing
     * timezone is pinned to UTC, which makes the XSD ordering total (both
     * operands become determinate instants) while agreeing with it on every
     * pair that was already determinate (UTC lies inside the +/-14h window XSD
     * uses for timezone-less values). Instant-equal values (same point on the
     * timeline, e.g. "...Z" vs "...+00:00") fall through to the exact-term
     * tie-break, exactly as value-equal literals do on the compareAlways path.
     */
    private static int compareTemporalTotal(NodeValue nv1, NodeValue nv2, Node n1, Node n2) {
        XMLGregorianCalendar a = (XMLGregorianCalendar) nv1.getDateTime().clone();
        XMLGregorianCalendar b = (XMLGregorianCalendar) nv2.getDateTime().clone();
        if (a.getTimezone() == DatatypeConstants.FIELD_UNDEFINED) a.setTimezone(0);
        if (b.getTimezone() == DatatypeConstants.FIELD_UNDEFINED) b.setTimezone(0);
        int r = a.compare(b);
        if (r == DatatypeConstants.LESSER)  return -1;
        if (r == DatatypeConstants.GREATER) return 1;
        return NodeCmp.compareRDFTerms(n1, n2);
    }

    /**
     * Total order for durations: by total months, then by total seconds, then
     * by exact term. XSD duration equality is exactly (months, seconds)
     * equality, so value-equal durations ("P1D" vs "PT24H") stay adjacent for
     * ValueCluster; and within each pure kind (year/month-only with
     * year/month-only, day/time-only with day/time-only) the order equals XSD
     * value order. ARQ answers cross-kind pairs "not comparable" (so
     * {@code P400D > P1Y} is false, never an exception) - compareAlways
     * used to term-order them pairwise-inconsistently; here they get the
     * fixed months-first rank. MIXED durations (a year/month part AND a
     * day/time part, e.g. "P1M35D") are one XSD class of their own, which
     * XSD orders by its four-reference-point rule: that order is
     * determinate for pairs this (months, seconds) order disagrees with
     * ("P1M35D" is XSD-greater than "P2M1D" yet ranks before it). The
     * dictionary order is still a strict total order; it just is not ARQ's
     * there, so the range pushdown must not narrow around a mixed-duration
     * constant - see FilterBounds.orderAgreesWithArq (BG-326).
     */
    private static int compareDurationTotal(NodeValue nv1, NodeValue nv2, Node n1, Node n2) {
        Duration d1 = nv1.getDuration();
        Duration d2 = nv2.getDuration();
        int c = totalMonths(d1).compareTo(totalMonths(d2));
        if (c != 0) return c;
        c = totalSeconds(d1).compareTo(totalSeconds(d2));
        if (c != 0) return c;
        return NodeCmp.compareRDFTerms(n1, n2);
    }

    private static BigInteger totalMonths(Duration d) {
        BigInteger years  = fieldInt(d, DatatypeConstants.YEARS);
        BigInteger months = fieldInt(d, DatatypeConstants.MONTHS);
        BigInteger total = years.multiply(BigInteger.valueOf(12)).add(months);
        return d.getSign() < 0 ? total.negate() : total;
    }

    private static BigDecimal totalSeconds(Duration d) {
        BigDecimal seconds = new BigDecimal(fieldInt(d, DatatypeConstants.DAYS)).multiply(BigDecimal.valueOf(86400))
                .add(new BigDecimal(fieldInt(d, DatatypeConstants.HOURS)).multiply(BigDecimal.valueOf(3600)))
                .add(new BigDecimal(fieldInt(d, DatatypeConstants.MINUTES)).multiply(BigDecimal.valueOf(60)))
                .add(fieldDec(d, DatatypeConstants.SECONDS));
        return d.getSign() < 0 ? seconds.negate() : seconds;
    }

    private static BigInteger fieldInt(Duration d, DatatypeConstants.Field f) {
        Number n = d.getField(f);
        return n == null ? BigInteger.ZERO : (BigInteger) n;
    }

    private static BigDecimal fieldDec(Duration d, DatatypeConstants.Field f) {
        Number n = d.getField(f);
        return n == null ? BigDecimal.ZERO : (BigDecimal) n;
    }

    /**
     * Maps a Node to its macro kind: BNode (1) < URI (2) < Literal (3) <
     * TripleTerm (4) - the order SPECIFICATIONS.md §6 fixes for the dictionary.
     * An RDF 1.2 triple term ({@code Node_Triple}) ranks after every literal
     * and is compared structurally in {@link #compare}. Anything else
     * (a variable, {@code Node.ANY}) is not an RDF term and cannot be ranked.
     */
    private int getMacroType(Node n) {
        if (n.isBlank()) return 1;
        if (n.isURI()) return 2;
        if (n.isLiteral()) return 3;
        if (n.isTripleTerm()) return 4;
        throw new IllegalArgumentException("Not an RDF term (blank node, IRI, literal or triple term): " + n);
    }

    private boolean isDefaultGraph(Node n) {
        if (n == null) return true;
        return n.equals(Quad.defaultGraphIRI) || n.equals(Quad.defaultGraphNodeGenerated);
    }
}
