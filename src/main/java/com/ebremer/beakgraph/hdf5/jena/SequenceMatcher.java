package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.core.NodeTable;
import com.ebremer.beakgraph.core.lib.SeqTerms;
import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;

/**
 * Id-level matching of a sequence PATTERN ({@code ( 1 ?x ... )}, a sequence
 * with a variable, a gap or a nested pattern inside) against stored
 * sequences, straight off {@code sequenceStarts} and {@code sequenceElements}
 * (RDFARRY.md §3.7, layer B): no candidate is materialized, and an element is
 * read only when the pattern reaches it.
 *
 * <p>It answers what jena-array's {@code SolverRX3.match} answers over the
 * same data (RDFARRY.md §1, item 8; rdf-undef §3):
 * <ul>
 *   <li>a constant element matches the same term, which is the same id: it
 *       is located once, when the pattern compiles, and a constant the store
 *       does not hold means the pattern matches nothing;</li>
 *   <li>{@code undef} matches only an empty slot (id 0), and a constant never
 *       does;</li>
 *   <li>a variable at a slot stays unbound, and one bound already - in the
 *       row, or earlier in the pattern - is compatible with a slot: a bound
 *       value is never written into the pattern;</li>
 *   <li>a gap takes every run of elements, so the multiplicity is the number
 *       of placements: {@code ( ... ?x ... )} over {@code ( 7 7 )} binds
 *       {@code ?x = 7} twice. A named gap binds the run as a sequence: the
 *       stored one, by id, when the store holds it, else the term itself
 *       ({@link BindingNodeId#withAbsentTerm});</li>
 *   <li>a nested sequence or triple-term pattern matches a nested term of its
 *       kind, never a slot.</li>
 * </ul>
 *
 * <p>Compiled once per iterator. The leading constant elements are a prefix,
 * and the sequences holding a prefix are an id range ({@link #firstObjectId},
 * {@link #lastObjectId}) that the scan is clamped to; the length is checked
 * from the starts before an element is read. With the element index
 * ({@code -seqindex}), an element after the prefix that every match holds -
 * a constant, {@code undef}, or a variable the row binds already - names the
 * only sequences that can match ({@link #candidates}).
 */
public final class SequenceMatcher implements ObjectPattern {

    private static final byte CONSTANT = 0, VARIABLE = 1, NESTED = 2, GAP = 3;

    private final PositionalDictionaryReader dict;
    private final int n;
    private final byte[] kind;
    private final long[] constant;          // CONSTANT: the element's object id; 0 is undef
    private final Var[] var;                // VARIABLE; GAP: the named gap's variable, or null
    private final ObjectPattern[] nested;   // NESTED: a sequence or triple-term pattern
    private final int[] fixedFrom;          // the non-gap elements at index i and after
    private final int[] gapsFrom;           // the gaps at index i and after
    private final long first, last;
    private final int prefix;               // the leading constants, which first..last already stands for
    private final boolean multiple;

    private SequenceMatcher(PositionalDictionaryReader dict, byte[] kind, long[] constant, Var[] var,
                            ObjectPattern[] nested, long first, long last, int prefix) {
        this.dict = dict;
        this.prefix = prefix;
        this.n = kind.length;
        this.kind = kind;
        this.constant = constant;
        this.var = var;
        this.nested = nested;
        this.first = first;
        this.last = last;
        this.fixedFrom = new int[n + 1];
        this.gapsFrom = new int[n + 1];
        boolean many = false;
        for (int i = n - 1; i >= 0; i--) {
            fixedFrom[i] = fixedFrom[i + 1] + (kind[i] == GAP ? 0 : 1);
            gapsFrom[i] = gapsFrom[i + 1] + (kind[i] == GAP ? 1 : 0);
            if (kind[i] == NESTED && nested[i].multiple()) {
                many = true;
            }
        }
        // One gap has one placement: what the fixed elements leave it.
        this.multiple = many || gapsFrom[0] > 1;
    }

    /**
     * {@code pattern} (a sequence, {@link SeqTerms#isPattern}) compiled
     * against the store, or null when it can match nothing there: the store
     * holds no sequences, a constant element is not in its dictionary, or no
     * stored sequence begins with the constant prefix.
     */
    static SequenceMatcher compile(Node pattern, PositionalDictionaryReader dict) {
        if (dict.firstSequenceObjectId() == Long.MAX_VALUE || !pattern.isNodeSeq()) {
            return null;
        }
        if (dict.arraysOnly()) {
            // Sequences held only in the arrays have no element ids, and a constant the dictionary lacks may be
            // one of their leaves: such a store's patterns go to jena-array's solver (PatternMatchBG), and an
            // answer on ids here would be wrong, not slow (RDFARRY.md step 9, part 2).
            throw new IllegalStateException("sequence patterns over a store with sequences held only in /.BG/arrays"
                    + " are matched by jena-array's solver, not on ids");
        }
        List<Node> elements = pattern.getSeq();
        int n = elements.size();
        byte[] kind = new byte[n];
        long[] constant = new long[n];
        Var[] var = new Var[n];
        ObjectPattern[] nested = new ObjectPattern[n];
        for (int i = 0; i < n; i++) {
            Node e = elements.get(i);
            if (SeqTerms.isGap(e)) {
                kind[i] = GAP;
                var[i] = SeqTerms.gapVar(e);
            } else if (SeqTerms.isUndef(e)) {
                kind[i] = CONSTANT;     // 0: only a slot
            } else if (e.isVariable()) {
                kind[i] = VARIABLE;
                var[i] = Var.alloc(e);
            } else if (ObjectPattern.isPattern(e)) {
                ObjectPattern inner = ObjectPattern.compile(e, dict);
                if (inner == null) {
                    return null;
                }
                kind[i] = NESTED;
                nested[i] = inner;
            } else {
                long id = dict.getObjects().locate(e);
                if (id < 1) {
                    return null;        // no stored sequence holds it
                }
                kind[i] = CONSTANT;
                constant[i] = id;
            }
        }
        int prefix = 0;
        while (prefix < n && kind[prefix] == CONSTANT) {
            prefix++;
        }
        long[] range = dict.sequencePrefixRange(java.util.Arrays.copyOf(constant, prefix));
        if (range == null) {
            return null;
        }
        return new SequenceMatcher(dict, kind, constant, var, nested, range[0], range[1], prefix);
    }

    /**
     * The rows of a scan that bound {@code objectVar} to each candidate's
     * object id, matched against the sequence pattern {@code pattern}: each
     * row once per match, without {@code objectVar}. A union read takes this
     * route, so that it removes duplicate TRIPLES across graphs before
     * matching (HDF5Reader.readUnion): a pattern with a gap gives one row per
     * placement, and a variable bound already passes a slot, so two distinct
     * triples may match with equal rows, and each counts.
     */
    public static java.util.Iterator<BindingNodeId> matchEach(java.util.Iterator<BindingNodeId> rows, Var objectVar,
            Node pattern, PositionalDictionaryReader dict, NodeTable nodeTable) {
        SequenceMatcher m = compile(pattern, dict);
        if (m == null) {
            return java.util.Collections.emptyIterator();
        }
        return org.apache.jena.atlas.iterator.Iter.flatMap(rows, r -> {
            ArrayDeque<BindingNodeId> more = m.multiple ? new ArrayDeque<>() : null;
            BindingNodeId one = m.match(NodeId.id(r.get(objectVar)), r.copyWithout(objectVar), nodeTable, more);
            if (one == null) {
                return java.util.Collections.emptyIterator();
            }
            if (more == null || more.isEmpty()) {
                return org.apache.jena.atlas.iterator.Iter.singletonIterator(one);
            }
            more.addFirst(one);
            return more.iterator();
        });
    }

    /**
     * The element index's candidates for {@code pattern} and input row {@code row} ({@link #candidates}), or
     * null when it names none or the store has no index; none at all when the pattern can match nothing.
     */
    public static SequenceCandidates candidatesFor(Node pattern, PositionalDictionaryReader dict, BindingNodeId row) {
        if (!dict.hasSequenceIndex()) {
            return null;
        }
        SequenceMatcher m = compile(pattern, dict);
        return (m == null) ? SequenceCandidates.holding(dict) : m.candidates(row);
    }

    /**
     * What the join reorder weighs a sequence pattern by (RDFARRY.md step 8), apart from the rows its
     * predicate has in the prefix's sequences, {@code firstObjectId..lastObjectId}, which only the indexes
     * count: the share of those rows that match, the share the scan reads, and the rows a match gives.
     *
     * @param share the matching share: for each element after the prefix that every match holds - a
     *              constant, {@code undef}, or a variable bound before - the share of the sequences
     *              holding it (the element index's count; {@link #GUESS} without one), multiplied
     * @param readShare the share the scan reads: the rarest such element's, when the element index lets the
     *                  scan read only its sequences; else 1
     * @param placements the rows one matching sequence gives: with two gaps or more around a variable left
     *                   free, about as many as the free run's places, {@code length - fixed + 1}, per gap
     *                   past the first; else 1
     */
    public record Estimate(long firstObjectId, long lastObjectId, double share, double readShare, double placements) {}

    /**
     * The share of sequences guessed to hold an element when the store has no element index to count them:
     * Jena's guess for a constant.
     */
    public static final double GUESS = 0.1;

    /**
     * The estimate for {@code pattern} given which variables are bound before it (their values unknown);
     * null when it matches nothing in the store.
     */
    public static Estimate estimate(Node pattern, PositionalDictionaryReader dict, java.util.function.Predicate<Var> bound) {
        SequenceMatcher m = compile(pattern, dict);
        if (m == null) {
            return null;
        }
        boolean index = dict.hasSequenceIndex();
        double sequences = Math.max(1, dict.sequenceCount());
        double perElement = 0, slotShare = 0;
        if (index) {
            long[] size = dict.sequenceIndexSize();
            perElement = (size[0] == 0) ? 0 : (double) size[1] / size[0];
            long[] slots = (dict.numSlots() > 0) ? dict.sequencesHolding(0) : null;
            slotShare = (slots == null) ? 0 : (slots[1] - slots[0]) / sequences;
        }
        double share = 1, rarest = 1;
        int free = 0;
        for (int i = m.prefix; i < m.n; i++) {
            double s;
            if (m.kind[i] == CONSTANT) {
                long[] r = index ? dict.sequencesHolding(m.constant[i]) : null;
                s = index ? ((r == null) ? 0 : (r[1] - r[0]) / sequences) : GUESS;
            } else if (m.kind[i] == VARIABLE && bound.test(m.var[i])) {
                s = index ? Math.min(1, perElement / sequences + slotShare) : GUESS;
            } else {
                if (m.kind[i] == VARIABLE || m.kind[i] == NESTED) {
                    free++;
                }
                continue;
            }
            share *= s;
            rarest = Math.min(rarest, s);
        }
        double readShare = (index && rarest * BGIteratorPOS.CANDIDATE_RATIO < 1) ? rarest : 1;
        double placements = 1;
        int gaps = m.gapsFrom[0];
        if (gaps >= 2 && free > 0) {
            long[] a = dict.sequenceBounds(m.first), b = dict.sequenceBounds(m.last);
            double length = (double) (b[1] - a[0]) / (m.last - m.first + 1);
            placements = Math.min(1e6, Math.pow(Math.max(1, length - m.fixedFrom[0] + 1), Math.min(gaps - 1, free)));
        }
        return new Estimate(m.first, m.last, share, readShare, placements);
    }

    @Override
    public long firstObjectId() {
        return first;
    }

    @Override
    public long lastObjectId() {
        return last;
    }

    @Override
    public boolean multiple() {
        return multiple;
    }

    /**
     * The sequences that can match for input row {@code row}, from the
     * element index: those holding the rarest element after the prefix that
     * every match must hold. That is a constant or {@code undef} (a slot),
     * which matches only itself, or a variable {@code row} binds already,
     * which matches its value or passes a slot: the sequences holding either.
     * A variable bound to a term the store does not hold passes only a slot.
     * Null without the index, or when no element names sequences: a gap, a
     * nested pattern, an unbound variable, or one bound to a predicate id.
     */
    @Override
    public SequenceCandidates candidates(BindingNodeId row) {
        if (!dict.hasSequenceIndex()) {
            return null;
        }
        boolean slots = dict.numSlots() > 0;
        SequenceCandidates best = null;
        for (int i = prefix; i < n; i++) {
            SequenceCandidates c = null;
            if (kind[i] == CONSTANT) {
                c = SequenceCandidates.holding(dict, constant[i]);
            } else if (kind[i] == VARIABLE && row != null) {
                long b = row.get(var[i]);
                if (b == NodeId.NONE || NodeId.isPredicateSpace(b)) {
                    continue;
                }
                if (NodeId.isSpecial(b)) {
                    c = slots ? SequenceCandidates.holding(dict, 0) : SequenceCandidates.holding(dict);
                } else {
                    c = slots ? SequenceCandidates.holding(dict, NodeId.id(b), 0) : SequenceCandidates.holding(dict, NodeId.id(b));
                }
            }
            if (c != null && (best == null || c.size() < best.size())) {
                best = c;
            }
        }
        return best;
    }

    @Override
    public BindingNodeId match(long objectId, BindingNodeId row, NodeTable nodeTable, ArrayDeque<BindingNodeId> more) {
        if (objectId < first || objectId > last) {
            return null;        // no stored sequence with the prefix (a nested element, or no sequence at all)
        }
        long[] bounds = dict.sequenceBounds(objectId);
        long length = bounds[1] - bounds[0];
        if ((gapsFrom[0] == 0) ? length != n : length < fixedFrom[0]) {
            return null;
        }
        Sink sink = new Sink(more);
        solve(0, bounds[0], bounds[1], row, nodeTable, sink);
        return sink.first;
    }

    /** The matches: the first, and the rest in the caller's queue. */
    private static final class Sink {
        private final ArrayDeque<BindingNodeId> more;
        private BindingNodeId first;

        Sink(ArrayDeque<BindingNodeId> more) {
            this.more = more;
        }

        void add(BindingNodeId row) {
            if (first == null) {
                first = row;
            } else {
                more.add(row);
            }
        }
    }

    /**
     * Matches pattern elements {@code i}.. against stored elements
     * {@code pos}..{@code end} (exclusive) of the element store, binding into
     * {@code row}, which this call owns: a placement of a gap, or a match of a
     * nested pattern with several, continues on a layer of its own over it.
     */
    private void solve(int i, long pos, long end, BindingNodeId row, NodeTable nodeTable, Sink sink) {
        while (i < n) {
            long left = end - pos;
            if (left < fixedFrom[i]) {
                return;
            }
            switch (kind[i]) {
                case GAP -> {
                    long longest = left - fixedFrom[i + 1];
                    if (gapsFrom[i + 1] == 0) {
                        // The last gap: its run is what the fixed elements after it leave.
                        row = bindRun(var[i], pos, pos + longest, row, nodeTable);
                        if (row == null) {
                            return;
                        }
                        pos += longest;
                        i++;
                        continue;
                    }
                    for (long run = 0; run <= longest; run++) {
                        BindingNodeId placed = bindRun(var[i], pos, pos + run, new BindingNodeId(row), nodeTable);
                        if (placed != null) {
                            solve(i + 1, pos + run, end, placed, nodeTable, sink);
                        }
                    }
                    return;
                }
                case CONSTANT -> {
                    if (dict.sequenceElementAt(pos) != constant[i]) {
                        return;
                    }
                }
                case VARIABLE -> {
                    long e = dict.sequenceElementAt(pos);
                    if (e != 0) {   // at a slot the variable stays as it is: unbound, or bound and compatible
                        if (row.isFull()) {
                            row = new BindingNodeId(row);
                        }
                        if (!row.putCompatible(var[i], NodeId.pack(NodeType.OBJECT, e), nodeTable)) {
                            return;
                        }
                    }
                }
                default -> {    // NESTED
                    long e = dict.sequenceElementAt(pos);
                    if (e == 0) {
                        return;     // a slot is no term
                    }
                    ObjectPattern inner = nested[i];
                    if (inner.multiple()) {
                        ArrayDeque<BindingNodeId> rest = new ArrayDeque<>();
                        BindingNodeId one = inner.match(e, new BindingNodeId(row), nodeTable, rest);
                        if (one == null) {
                            return;
                        }
                        solve(i + 1, pos + 1, end, one, nodeTable, sink);
                        for (BindingNodeId r : rest) {
                            solve(i + 1, pos + 1, end, r, nodeTable, sink);
                        }
                        return;
                    }
                    row = inner.match(e, row, nodeTable, null);
                    if (row == null) {
                        return;
                    }
                }
            }
            pos++;
            i++;
        }
        if (pos == end) {
            sink.add(row);
        }
    }

    /**
     * Binds the named gap {@code v} to the run of stored elements
     * {@code from}..{@code to} (exclusive), as a sequence; the anonymous gap
     * ({@code v} null) binds nothing. A run is a stored sequence, by id, when
     * the store holds one equal to it, else a term of its own. Returns the
     * row, or null when {@code v} is bound already to a different term.
     */
    private BindingNodeId bindRun(Var v, long from, long to, BindingNodeId row, NodeTable nodeTable) {
        if (v == null) {
            return row;
        }
        long[] ids = new long[Math.toIntExact(to - from)];
        for (int k = 0; k < ids.length; k++) {
            ids[k] = dict.sequenceElementAt(from + k);
        }
        long stored = dict.locateSequence(ids);
        long existing = row.get(v);
        if (existing == NodeId.NONE) {
            if (stored >= 1) {
                if (row.isFull()) {
                    row = new BindingNodeId(row);
                }
                row.bind(v, NodeId.pack(NodeType.OBJECT, stored));
                return row;
            }
            return row.withAbsentTerm(v, term(ids, nodeTable));
        }
        if (NodeId.isDoesNotExist(existing)) {
            // Bound to a term this store does not hold: the run is equal to it only if it is not stored either.
            Node bound = row.absentTerm(v);
            return (stored < 1 && bound != null && bound.equals(term(ids, nodeTable))) ? row : null;
        }
        if (stored < 1) {
            return null;    // a stored term is never one the store does not hold
        }
        return row.putCompatible(v, NodeId.pack(NodeType.OBJECT, stored), nodeTable) ? row : null;
    }

    /** The sequence of stored elements {@code ids} as a term. */
    private static Node term(long[] ids, NodeTable nodeTable) {
        List<Node> elements = new ArrayList<>(ids.length);
        for (long id : ids) {
            elements.add((id == 0) ? SeqTerms.UNDEF : nodeTable.getNodeForNodeId(NodeId.pack(NodeType.OBJECT, id)));
        }
        return SeqTerms.create(elements);
    }
}
