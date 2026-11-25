package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.hdf5.readers.PositionalDictionaryReader;

/**
 * The stored sequences a sequence pattern can match, read from the element index ({@code -seqindex},
 * RDFARRY.md step 8): the sequences holding one element, or holding either of two (a bound variable's
 * value, or a slot, which passes it). A superset of the matches, never fewer: the matcher still reads and
 * checks each one, so this only spares the scan the sequences that cannot match.
 *
 * <p>A cursor over ascending object ids, for a merge with an index run of objects: {@link #current},
 * {@link #seek} to an id, {@link #advance} past the current one. Stateful, so each scan takes its own
 * ({@link #fresh}).
 *
 * <p>Each element's sequences come from the index, except the slot's within one scan
 * ({@link #withSlots}): the sequences with a slot are candidates for every bound variable, and over a
 * whole store they may be most of them, where few or none are objects of the scan's predicate.
 */
public final class SequenceCandidates {

    private final PositionalDictionaryReader dict;
    private final long[] element;   // per source: the element it holds
    private final long[][] list;    // per source: its object ids, or null to read them from the index
    private final long[] from;      // per source: where its entries begin (the index's positions, or the list's)
    private final long[] to;        // and end, exclusive
    private final long[] at;        // the cursor's position in each
    private final long[] head;      // the object id at it, Long.MAX_VALUE once past the end
    private final long size;

    private SequenceCandidates(PositionalDictionaryReader dict, long[] element, long[][] list, long[] from, long[] to) {
        this.dict = dict;
        this.element = element;
        this.list = list;
        this.from = from;
        this.to = to;
        this.at = from.clone();
        this.head = new long[from.length];
        long n = 0;
        for (int i = 0; i < from.length; i++) {
            n += to[i] - from[i];
            head[i] = unnamed(i) ? Long.MAX_VALUE : read(i);
        }
        this.size = n;
    }

    /** Whether source {@code i} stands for the sequences held only in the arrays, not yet named for a scan. */
    private boolean unnamed(int i) {
        return element[i] == HELD && list[i] == null;
    }

    /**
     * The sequences holding any of {@code elementIds} (object-space ids, 0 for a slot); none when no
     * sequence holds any of them. The caller guarantees the store has the element index.
     */
    static SequenceCandidates holding(PositionalDictionaryReader dict, long... elementIds) {
        return of(dict, dict.arraysOnly(), elementIds);
    }

    /** The sequences the element index names as holding any of {@code elementIds}: its own entries, nothing more. */
    static SequenceCandidates indexed(PositionalDictionaryReader dict, long... elementIds) {
        return of(dict, false, elementIds);
    }

    private static SequenceCandidates of(PositionalDictionaryReader dict, boolean held, long... elementIds) {
        int k = elementIds.length + 1;
        long[] e = new long[k], f = new long[k], t = new long[k];
        int n = 0;
        for (long id : elementIds) {
            long[] r = (id < 0) ? null : dict.sequencesHolding(id);     // -1: a term the dictionary lacks
            if (r != null && r[1] > r[0]) {
                e[n] = id;
                f[n] = r[0];
                t[n++] = r[1];
            }
        }
        if (held) {
            e[n++] = HELD;      // the sequences held only in the arrays, which the index has no entries for
        }
        return new SequenceCandidates(dict, java.util.Arrays.copyOf(e, n), new long[n][],
                java.util.Arrays.copyOf(f, n), java.util.Arrays.copyOf(t, n));
    }

    /** The source standing for the sequences held only in the arrays (format v8), until a scan names them ({@link #withHeld}). */
    static final long HELD = -2;

    /** Whether these candidates still stand for the sequences held only in the arrays: a scan must name its own. */
    boolean holdsHeld() {
        for (int i = 0; i < element.length; i++) {
            if (element[i] == HELD && list[i] == null) {
                return true;
            }
        }
        return false;
    }

    /** These candidates, from the first, with the sequences held only in the arrays narrowed to {@code objectIds} (ascending). */
    SequenceCandidates withHeld(long[] objectIds) {
        long[][] l = list.clone();
        long[] f = from.clone(), t = to.clone();
        for (int i = 0; i < element.length; i++) {
            if (element[i] == HELD) {
                l[i] = objectIds;
                f[i] = 0;
                t[i] = objectIds.length;
            }
        }
        return new SequenceCandidates(dict, element, l, f, t);
    }

    /** The same sequences, from the first, for another scan. */
    public SequenceCandidates fresh() {
        return new SequenceCandidates(dict, element, list, from, to);
    }

    /** Whether the sequences with a slot are among these: the ones {@link #withSlots} narrows. */
    boolean holdsSlots() {
        for (long e : element) {
            if (e == 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * These candidates, from the first, with the sequences that hold a slot narrowed to
     * {@code objectIds} (ascending): those of them a scan's run holds.
     */
    SequenceCandidates withSlots(long[] objectIds) {
        long[][] l = list.clone();
        long[] f = from.clone(), t = to.clone();
        for (int i = 0; i < element.length; i++) {
            if (element[i] == 0) {
                l[i] = objectIds;
                f[i] = 0;
                t[i] = objectIds.length;
            }
        }
        return new SequenceCandidates(dict, element, l, f, t);
    }

    /** How many entries the index holds for these elements: the candidates, counting one twice that holds both. */
    public long size() {
        return size;
    }

    /** The smallest candidate object id at or after the cursor; {@link Long#MAX_VALUE} when there is none. */
    public long current() {
        if (holdsHeld()) {
            throw new IllegalStateException("the sequences held only in the arrays were never named for this scan");
        }
        long c = Long.MAX_VALUE;
        for (long h : head) {
            c = Math.min(c, h);
        }
        return c;
    }

    /** Moves the cursor to the first candidate whose object id is {@code objectId} or later. */
    public void seek(long objectId) {
        for (int i = 0; i < head.length; i++) {
            if (head[i] < objectId) {
                if (list[i] == null) {
                    at[i] = dict.sequenceIndexSeek(at[i] + 1, to[i], objectId);
                } else {
                    int p = java.util.Arrays.binarySearch(list[i], (int) at[i] + 1, (int) to[i], objectId);
                    at[i] = (p >= 0) ? p : -(p + 1);
                }
                head[i] = read(i);
            }
        }
    }

    /** Moves the cursor past the current candidate. */
    public void advance() {
        long c = current();
        for (int i = 0; i < head.length; i++) {
            if (head[i] == c) {
                at[i]++;
                head[i] = read(i);
            }
        }
    }

    private long read(int i) {
        if (at[i] >= to[i]) {
            return Long.MAX_VALUE;
        }
        return (list[i] == null) ? dict.sequenceIndexObjectId(at[i]) : list[i][(int) at[i]];
    }
}
