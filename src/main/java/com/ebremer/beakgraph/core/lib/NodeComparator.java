package com.ebremer.beakgraph.core.lib;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Comparator;
import javax.xml.datatype.DatatypeConstants;
import javax.xml.datatype.Duration;
import javax.xml.datatype.XMLGregorianCalendar;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.util.NodeCmp;

/**
 * Enforces a strict Total Ordering of RDF Nodes.
 * CRITICAL for HDF5 Binary Search:
 * Ensures the sorting order perfectly matches the Monolithic Dictionary ID assignments:
 * 1. Default Graph
 * 2. Blank Nodes
 * 3. URIs
 * 4. Literals (Sorted numerically/temporally/lexicographically by value)
 */
public class NodeComparator implements Comparator<Node> {

    public static final NodeComparator INSTANCE = new NodeComparator();

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

        // 2. Enforce RDF Term Macro-Ordering (BNode < URI < Literal)
        // This ensures the sorted array perfectly aligns with how IDs are chunked
        int type1 = getMacroType(n1);
        int type2 = getMacroType(n2);

        if (type1 != type2) {
            return Integer.compare(type1, type2);
        }

        // 3. Both nodes are the SAME RDF Term Type.
        // If they are both Literals, we must sort by actual Value (e.g. 2 < 10), not String ("10" < "2")
        if (n1.isLiteral() && n2.isLiteral()) {
            try {
                NodeValue nv1 = nodeValue(n1);
                NodeValue nv2 = nodeValue(n2);

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
                    return group == GROUP_DURATION
                            ? compareDurationTotal(nv1, nv2, n1, n2)
                            : compareTemporalTotal(nv1, nv2, n1, n2);
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
                return NodeCmp.compareRDFTerms(n1, n2);
            } catch (Exception e) {
                // Absolute fallback if Jena fails to parse a highly malformed literal
                return NodeCmp.compareRDFTerms(n1, n2);
            }
        }

        // 4. If they are both URIs or both BNodes, fallback to Jena's standard lexicographical sort
        return NodeCmp.compareRDFTerms(n1, n2);
    }

    private static final int GROUP_DURATION = 9;

    /**
     * Classifies a literal into one of the timezone-sensitive temporal value
     * spaces (or 0 for everything else). The grouping mirrors Jena's own value
     * spaces - dateTime and dateTimeStamp share one space, date/time/g* each
     * have their own - so the special-cased ordering below applies exactly where
     * compareAlways would have compared by value-or-term, and never across two
     * spaces that compareAlways ranks by value space. Ill-formed literals answer
     * false to all predicates and stay on the compareAlways path.
     */
    private static int temporalGroup(NodeValue nv) {
        if (nv.isDateTime())   return 1;
        if (nv.isDate())       return 2;
        if (nv.isTime())       return 3;
        if (nv.isGYear())      return 4;
        if (nv.isGYearMonth()) return 5;
        if (nv.isGMonth())     return 6;
        if (nv.isGMonthDay())  return 7;
        if (nv.isGDay())       return 8;
        if (nv.isDuration())   return GROUP_DURATION;
        return 0;
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
     * ValueCluster; and within each XSD-comparable kind (month-based with
     * month-based, day/time-based with day/time-based) the order equals XSD
     * value order. Cross-kind pairs - which SPARQL comparison rejects and
     * compareAlways used to term-order pairwise-inconsistently - get the fixed
     * months-first rank.
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
     * Maps a Node to an integer rank to enforce BNode < URI < Literal.
     */
    private int getMacroType(Node n) {
        if (n.isBlank()) return 1;
        if (n.isURI()) return 2;
        if (n.isLiteral()) return 3;
        // Should never happen in valid RDF, but safe fallback
        return 4;
    }

    private boolean isDefaultGraph(Node n) {
        if (n == null) return true;
        return n.equals(Quad.defaultGraphIRI) || n.equals(Quad.defaultGraphNodeGenerated);
    }
}
