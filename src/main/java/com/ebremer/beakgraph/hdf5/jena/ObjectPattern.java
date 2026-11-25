package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.core.lib.SeqTerms;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import java.util.ArrayDeque;
import org.apache.jena.graph.Node;

/**
 * A compound-term PATTERN in the object position, matched on ids against each
 * candidate object id the scan yields (RDFARRY.md §3.7, layer B): a triple
 * term with a variable inside ({@link TripleTermMatcher}), or a sequence with
 * a variable, a gap or a nested pattern inside ({@link SequenceMatcher}).
 *
 * <p>The iterators that scan objects ({@link BGIteratorPOS},
 * {@link BGIteratorSO}, {@link BGIteratorSPO_All}) clamp the scan to
 * {@link #firstObjectId()}..{@link #lastObjectId()}, then ask
 * {@link #match} for each candidate. A pattern of either kind is never a
 * term to locate: {@link BGIteratorMaster} routes it as an unbound object.
 */
interface ObjectPattern {

    /**
     * Whether {@code n} is a compound-term pattern: a triple term or a
     * sequence with a variable, a gap or a nested pattern inside, at any
     * depth. THE classification every routing site uses.
     */
    static boolean isPattern(Node n) {
        return TripleTermMatcher.isPattern(n) || SeqTerms.isPattern(n);
    }

    /**
     * {@code pattern} ({@link #isPattern}) compiled against the store, or null
     * when it can match nothing in it: callers answer null with no rows,
     * never by scanning.
     */
    static ObjectPattern compile(Node pattern, PositionalDictionaryReader dict) {
        return pattern.isTripleTerm() ? TripleTermMatcher.compile(pattern, dict) : SequenceMatcher.compile(pattern, dict);
    }

    /** The lowest object id that can match (a scan's lower clamp). */
    long firstObjectId();

    /** The highest object id that can match (a scan's upper clamp). */
    long lastObjectId();

    /**
     * Whether a candidate can match in more than one way, so that
     * {@link #match} may give several rows: a sequence pattern with a gap
     * ({@code ( ... ?x ... )} over {@code ( 7 7 )} binds {@code ?x} twice).
     */
    boolean multiple();

    /**
     * Matches stored object {@code objectId} and, on a match, binds the
     * pattern's variables into {@code row} (chaining layers as they fill).
     * Returns the row of the first match, or null when there is none; with
     * {@link #multiple()}, every further match is added to {@code more}, which
     * is then non-null. On null the caller DISCARDS {@code row}: partial
     * bindings may have landed in it (every caller builds one per candidate).
     */
    BindingNodeId match(long objectId, BindingNodeId row, NodeTable nodeTable, ArrayDeque<BindingNodeId> more);
}
