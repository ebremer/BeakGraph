package com.ebremer.beakgraph.core.fuseki;

import com.ebremer.beakgraph.core.lib.HyperLogLog;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.TextDirection;

/**
 * Distinct-node counter with a bounded footprint: EXACT (a concurrent set of
 * the nodes) up to {@code exactLimit}, then it spills once into a
 * {@link HyperLogLog} sketch (64 KiB, ~0.8% error) and stops retaining nodes.
 * This is what keeps the VoID statistics from silently holding a large
 * fraction of a billion-quad store's dictionary on the heap: small and
 * medium graphs report exact counts (unchanged output, byte-for-byte), huge
 * ones report deterministic estimates.
 *
 * <p>Thread-safe. The one soft spot is the spill instant itself: an add that
 * races the fold can be missed, bounding the error by the number of threads
 * active at that moment - noise at the cardinalities where the sketch is in
 * play.
 */
final class DistinctNodeCounter {

    private final int exactLimit;
    private volatile Set<Node> exact = ConcurrentHashMap.newKeySet();
    private volatile HyperLogLog sketch;
    /** Insertions that grew the set - a cheap stand-in for CHM.size(), which walks every segment (BG-255). */
    private final AtomicInteger grown = new AtomicInteger();

    DistinctNodeCounter(int exactLimit) {
        this.exactLimit = exactLimit;
    }

    void add(Node n) {
        Set<Node> e = exact;
        if (e != null) {
            // The limit is checked only when the set actually grew, and against a
            // counter rather than the set's size: this runs once per quad on
            // every engine's ingest path. EXACT mode (no limit) never checks.
            if (e.add(n) && exactLimit != Integer.MAX_VALUE && grown.incrementAndGet() > exactLimit) {
                spill();
            }
            return;
        }
        sketch.add(hash(n));
    }

    private synchronized void spill() {
        Set<Node> e = exact;
        if (e == null) {
            return; // another thread already spilled
        }
        HyperLogLog h = new HyperLogLog();
        for (Node n : e) {
            h.add(hash(n));
        }
        sketch = h;   // publish before dropping the set: count() never sees both null
        exact = null;
    }

    long count() {
        Set<Node> e = exact;
        return (e != null) ? e.size() : sketch.estimate();
    }

    /** True while counts are still exact (used by reporting/tests). */
    boolean isExact() {
        return exact != null;
    }

    /**
     * 64-bit content hash of a node: a rolling polynomial over the term's
     * textual identity (kind-tagged; literals include lexical form, datatype,
     * and language), finished with a splitmix64 avalanche. Full 64-bit space,
     * so hash collisions are negligible even at 10^10 distinct terms.
     */
    static long hash(Node n) {
        long h;
        if (n.isURI()) {
            h = poly(11, n.getURI());
        } else if (n.isBlank()) {
            h = poly(13, n.getBlankNodeLabel());
        } else if (n.isLiteral()) {
            h = poly(17, n.getLiteralLexicalForm());
            h = h * 31 + poly(19, n.getLiteralDatatypeURI());
            String lang = n.getLiteralLanguage();
            if (lang != null && !lang.isEmpty()) {
                h = h * 31 + poly(23, lang);
            }
            // Base direction is part of term identity (rdf:dirLangString):
            // without this, "x"@en--ltr and "x"@en--rtl hash together and the
            // HLL undercounts distinct objects.
            TextDirection dir = n.getLiteralBaseDirection();
            if (dir != null) {
                h = h * 31 + (dir == TextDirection.LTR ? 37 : 41);
            }
        } else if (n.isTripleTerm()) {
            // Structural and recursive: stable across Jena toString changes, and
            // embedded dirLang/CDT term identity flows through the branches above.
            org.apache.jena.graph.Triple t = n.getTriple();
            h = 43 * 0x9E3779B97F4A7C15L;
            h = h * 31 + hash(t.getSubject());
            h = h * 31 + hash(t.getPredicate());
            h = h * 31 + hash(t.getObject());
        } else {
            h = poly(29, n.toString());
        }
        return HyperLogLog.mix64(h);
    }

    private static long poly(long seed, String s) {
        long h = seed * 0x9E3779B97F4A7C15L;
        for (int i = 0; i < s.length(); i++) {
            h = 31 * h + s.charAt(i);
        }
        return h;
    }
}
