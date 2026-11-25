package com.ebremer.beakgraph.hdf5.writers;

import com.ebremer.beakgraph.Params;
import com.ebremer.beakgraph.core.DictionaryWriter;
import com.ebremer.beakgraph.hdf5.BlockedEncoder;
import com.ebremer.beakgraph.hdf5.WritableGroup;
import static com.ebremer.beakgraph.utils.UTIL.MinBits;
import java.util.Arrays;
import java.util.function.LongUnaryOperator;

/**
 * The element index ({@code -seqindex}, format v7, RDFARRY.md step 8): which sequences hold an element.
 * It is the sequence section's transpose, in the same form. {@code sequenceStarts} and
 * {@code sequenceElements} give each sequence its element ids; these three give each element id its
 * sequences:
 * <ul>
 *   <li>{@link Params#SEQUENCE_INDEX_ELEMENTS}: the distinct element ids some sequence holds, ascending.
 *       0, a slot, comes first when any sequence has one: a variable bound already passes a slot, so the
 *       sequences with slots are candidates for every bound variable.</li>
 *   <li>{@link Params#SEQUENCE_INDEX_STARTS}: one more entry than there are elements; element {@code k}'s
 *       sequences are {@code sequences[starts[k], starts[k+1])}.</li>
 *   <li>{@link Params#SEQUENCE_INDEX_SEQUENCES}: sequence ordinals (the object id less the first
 *       sequence's), ascending within an element, each once however often the sequence holds it.</li>
 * </ul>
 * All three are block-structured (SPECIFICATIONS.md §5.1a, §7.6d) and follow {@code sequenceArrays} in the
 * literals section. The index is a function of the sequence section alone, so every engine writes the same
 * bytes: the in-memory writers through {@link #add}, the disk engines by an external sort of the same
 * pairs ({@code HugeSequences}).
 *
 * <p>Positions are not kept. The matcher reads each candidate sequence anyway, to bind its variables and
 * count its gaps' placements, so nothing would read them.
 */
public final class SequenceIndex {

    private SequenceIndex() {}

    /**
     * Asks {@code literals}, the literals section, to write the element index after its sequences.
     * Nothing happens without sequences, or when {@code enabled} is false.
     */
    public static void enable(boolean enabled, DictionaryWriter literals) {
        if (enabled && literals instanceof MultiTypeDictionaryWriter section) {
            section.setSequenceIndex(true);
        }
    }

    /**
     * Adds the element index of the sequence section held in memory to {@code group}: {@code sequences}
     * sequences, where sequence {@code k}'s elements are {@code elements(starts(k))} up to
     * {@code elements(starts(k + 1))}, {@code elementCount} elements in all.
     */
    static void add(WritableGroup group, LongUnaryOperator starts, long sequences,
                    LongUnaryOperator elements, long elementCount) {
        if (elementCount == 0) {
            return;             // every sequence is (): nothing to index, and no dataset
        }
        Columns c = build(starts, sequences, elements, elementCount);
        BlockedEncoder.add(group, Params.SEQUENCE_INDEX_ELEMENTS, c.elements().length, i -> c.elements()[(int) i]);
        BlockedEncoder.add(group, Params.SEQUENCE_INDEX_STARTS, c.starts().length, i -> c.starts()[(int) i]);
        BlockedEncoder.add(group, Params.SEQUENCE_INDEX_SEQUENCES, c.sequences().length, i -> c.sequences()[(int) i]);
    }

    /** The three columns of the index. */
    record Columns(long[] elements, long[] starts, long[] sequences) {}

    /**
     * The (element, sequence) pairs, sorted and each once, as the three columns. A pair is one long, the
     * element above the ordinal, when the two fit 63 bits together, as they do in any store an in-memory
     * writer can hold; otherwise the pairs are sorted by element, stably, so the ordinals stay ascending.
     */
    static Columns build(LongUnaryOperator starts, long sequences, LongUnaryOperator elements, long elementCount) {
        int count = Math.toIntExact(elementCount);
        long maxElement = 0;
        for (long i = 0; i < elementCount; i++) {
            maxElement = Math.max(maxElement, elements.applyAsLong(i));
        }
        int ordinalBits = MinBits(Math.max(1, sequences - 1));
        if (MinBits(maxElement) + ordinalBits > 63) {
            return buildWide(starts, sequences, elements, count);
        }
        long[] pairs = new long[count];
        int p = 0;
        for (long k = 0; k < sequences; k++) {
            long to = starts.applyAsLong(k + 1);
            for (long i = starts.applyAsLong(k); i < to; i++) {
                pairs[p++] = (elements.applyAsLong(i) << ordinalBits) | k;
            }
        }
        Arrays.parallelSort(pairs);
        long mask = (1L << ordinalBits) - 1;
        return columns(count, i -> pairs[i] >>> ordinalBits, i -> pairs[i] & mask);
    }

    private static Columns buildWide(LongUnaryOperator starts, long sequences, LongUnaryOperator elements, int count) {
        long[] element = new long[count];
        long[] ordinal = new long[count];
        int p = 0;
        for (long k = 0; k < sequences; k++) {
            long to = starts.applyAsLong(k + 1);
            for (long i = starts.applyAsLong(k); i < to; i++) {
                element[p] = elements.applyAsLong(i);
                ordinal[p++] = k;
            }
        }
        Integer[] order = new Integer[count];
        for (int i = 0; i < count; i++) {
            order[i] = i;
        }
        Arrays.sort(order, (a, b) -> Long.compareUnsigned(element[a], element[b]));    // stable: ordinals stay ascending
        return columns(count, i -> element[order[i]], i -> ordinal[order[i]]);
    }

    private interface At {
        long at(int i);
    }

    /** The columns of {@code count} sorted pairs, duplicates dropped. */
    private static Columns columns(int count, At element, At ordinal) {
        int distinctElements = 0, distinctPairs = 0;
        for (int i = 0; i < count; i++) {
            boolean newElement = i == 0 || element.at(i) != element.at(i - 1);
            if (newElement) {
                distinctElements++;
            }
            if (newElement || ordinal.at(i) != ordinal.at(i - 1)) {
                distinctPairs++;
            }
        }
        long[] elementIds = new long[distinctElements];
        long[] elementStarts = new long[distinctElements + 1];
        long[] sequenceOrdinals = new long[distinctPairs];
        int e = -1, q = 0;
        for (int i = 0; i < count; i++) {
            boolean newElement = i == 0 || element.at(i) != element.at(i - 1);
            if (newElement) {
                elementIds[++e] = element.at(i);
                elementStarts[e] = q;
            }
            if (newElement || ordinal.at(i) != ordinal.at(i - 1)) {
                sequenceOrdinals[q++] = ordinal.at(i);
            }
        }
        elementStarts[distinctElements] = q;
        return new Columns(elementIds, elementStarts, sequenceOrdinals);
    }
}
