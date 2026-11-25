package com.ebremer.beakgraph.huge;

import com.ebremer.beakgraph.huge.HugeRecords.IdQuad;
import com.ebremer.beakgraph.huge.HugeRecords.RowId;
import com.ebremer.beakgraph.huge.HugeRecords.TermRow;
import static com.ebremer.beakgraph.utils.UTIL.MinBits;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.jena.graph.Node;

/**
 * Disk-engine sequence machinery (format v7, SPECIFICATIONS.md §7.6b), the
 * twin of {@link HugeTripleTerms}. During the literals-dictionary encode,
 * each sequence row (in section order) writes where its elements end into
 * {@code sequenceStarts} - the lengths are known per term, so the prefix sums
 * ARE the starts - and spills each non-slot element as
 * {@code (element term, global element index)}. After the encode, one
 * term-ordered pass merged against the entity and literal dictionary files
 * resolves every element to its object-space id with monotone cursors, and an
 * index-ordered second sort lays the ids out as {@code sequenceElements},
 * writing 0 for each index no record carries: the slots, which never spill.
 *
 * <p>Why one pass suffices, as for triple terms: the element stream is
 * NodeComparator-sorted, so entity elements (blank nodes, IRIs) form its
 * prefix and literals, triple terms and nested sequences its suffix.
 *
 * <p>With {@code -seqindex}, the pass that lays out {@code sequenceElements}
 * also hands each (element id, sequence ordinal) pair, slots included, to one
 * more sort, and the sorted pairs become the element index's three columns
 * ({@link com.ebremer.beakgraph.hdf5.writers.SequenceIndex}): the bytes the
 * in-memory writers write.
 */
final class HugeSequences implements AutoCloseable {

    private final SorterProvider provider;
    private final Path workDir;
    private final RecordSorter<TermRow> refs;
    private final SpillBitPackedBuffer starts;
    private long count = 0;
    private long elements = 0;
    private long slots = 0;
    /** {@code -arrays}: the encode's sequences go to it too; null without it. */
    private HugeArrays arrays;
    /** {@code -seqindex}: whether {@link #resolve} also builds the element index. */
    private boolean index;
    /** The element index's columns (elements, starts, sequences) once resolved; null without one. */
    private SpillBitPackedBuffer[] indexColumns;

    HugeSequences(SorterProvider provider, Path workDir) throws IOException {
        this.provider = provider;
        this.workDir = workDir;
        this.refs = provider.termSorter(workDir, "seqrefs");
        // The blocked transfer re-encodes the values, so the spill's width only
        // has to hold them: the total element count is not known until the end.
        this.starts = new SpillBitPackedBuffer(workDir.resolve("sequenceStarts.store"), 64);
        this.starts.writeLong(0);
    }

    /**
     * Writer callback for one sequence row, called in section order: spills the
     * element references, records where the sequence's elements end, and
     * returns its 0-based ordinal - exactly the row's {@code offsets} value.
     */
    long onSequence(Node seq) throws IOException {
        long k = count++;
        if (arrays != null && arrays.onSequence(k, seq)) {
            starts.writeLong(elements);   // -arrays-only: held only in the arrays, an empty range
            return k;
        }
        List<Node> el = seq.getSeq();
        for (int i = 0; i < el.size(); i++) {
            Node e = el.get(i);
            if (e.isNodeUndef()) {
                slots++;
            } else {
                refs.add(new TermRow(e, elements + i));
            }
        }
        elements += el.size();
        starts.writeLong(elements);
        return k;
    }

    long count() {
        return count;
    }

    /** {@code -arrays-only}: whether some sequence is held only in the arrays, without elements. */
    boolean heldOnlyInArrays() {
        return arrays != null && arrays.heldCount() > 0;
    }

    /** {@code -arrays}: hands every sequence of the encode to {@code a} as well, in order. */
    void setArrays(HugeArrays a) {
        this.arrays = a;
    }

    /** {@code -seqindex}: {@link #resolve} builds the element index too. */
    void setIndex(boolean on) {
        this.index = on;
    }

    /**
     * The element index's columns, in the order they are written - {@code sequenceIndexElements},
     * {@code sequenceIndexStarts}, {@code sequenceIndexSequences} - or null without {@code -seqindex}, or
     * when no sequence has an element. Valid after {@link #resolve}.
     */
    SpillBitPackedBuffer[] indexColumns() {
        return indexColumns;
    }

    long elementCount() {
        return elements;
    }

    long slots() {
        return slots;
    }

    /** The completed {@code sequenceStarts} spill: {@code count() + 1} entries, the first 0. */
    SpillBitPackedBuffer starts() throws IOException {
        starts.complete();
        return starts;
    }

    /**
     * Resolves every spilled element and returns the completed
     * {@code sequenceElements} spill (entries in global index order, 0 for a
     * slot). Must run after {@code entFile} and {@code litFile} are complete
     * and before the pipeline deletes them.
     */
    SpillBitPackedBuffer resolve(RecordSegments<Node> entFile, RecordSegments<Node> litFile,
                                 long numEntities, long numObjects) throws IOException {
        long spilled = elements - slots;
        // A row is an element's index among them all, slots included, so the rows reach elements - 1: with
        // the spilled count as their bound, hugeUltra's packed sorter refused a store whose slots carried the
        // last index past a power of two.
        RecordSorter<RowId> resolved = provider.rowIdSorter(workDir, "seqids", elements, numObjects);
        try (var refStream = refs.sorted();
             var ents = entFile.read();
             var lits = litFile.read()) {
            HugeBuildPipeline.DictCursor entCur = new HugeBuildPipeline.DictCursor(ents, () -> {});
            HugeBuildPipeline.DictCursor litCur = new HugeBuildPipeline.DictCursor(lits, () -> {});
            while (refStream.hasNext()) {
                TermRow ref = refStream.next();
                Node term = ref.term();
                long id = (term.isLiteral() || term.isTripleTerm() || term.isNodeSeq())
                        ? litCur.locate(ref.key(), "sequence element") + numEntities
                        : entCur.locate(ref.key(), "sequence element");
                resolved.add(new RowId(ref.row(), id));
            }
        } finally {
            refs.close();
        }

        int width = 1 + MinBits(numObjects);
        if (width > 57) width = 64; // bit-packed widths are 1..57 or 64
        SpillBitPackedBuffer store = new SpillBitPackedBuffer(workDir.resolve("sequenceElements.store"), width);
        // -seqindex: (element, ordinal) pairs, sorted as the quads (element, ordinal, 0, 0) in GSPO order,
        // which is element order, then ordinal order. Ordinals are below count, element ids at most numObjects.
        RecordSorter<IdQuad> pairs = (index && elements > 0)
                ? provider.quadSorter(workDir, "seqindex", com.ebremer.beakgraph.hdf5.Index.GSPO,
                        Math.max(numObjects, count), 1, 1)
                : null;
        boolean ok = false;
        try (var ids = resolved.sorted();
             Elements out = new Elements(store, pairs)) {
            long next = 0;
            long zeros = 0;
            while (ids.hasNext()) {
                RowId r = ids.next();
                if (r.row() < next) {
                    throw new IllegalStateException("Sequence element misalignment: index " + r.row()
                            + " where " + next + " or later was expected");
                }
                while (next < r.row()) {     // the slots before it
                    out.write(0);
                    next++;
                    zeros++;
                }
                out.write(r.id());
                next++;
            }
            while (next < elements) {        // trailing slots
                out.write(0);
                next++;
                zeros++;
            }
            if (zeros != slots) {
                throw new IllegalStateException("Resolved " + (elements - zeros) + " sequence elements and "
                        + zeros + " slots where " + (elements - slots) + " and " + slots + " were recorded");
            }
            store.complete();
            if (pairs != null) {
                indexColumns = indexColumns(pairs, numObjects);
            }
            ok = true;
            return store;
        } finally {
            resolved.close();
            if (pairs != null) {
                pairs.close();
            }
            if (!ok) {
                try { store.close(); } catch (IOException ignored) { }
            }
        }
    }

    /**
     * Where {@link #resolve} writes the elements, in index order: to the element store, and with
     * {@code -seqindex} as (element, ordinal) pairs too, the ordinal followed through
     * {@code sequenceStarts} as the index passes each sequence's end.
     */
    private final class Elements implements AutoCloseable {
        private final SpillBitPackedBuffer store;
        private final RecordSorter<IdQuad> pairs;
        private final SpillBitPackedBuffer.Unpacker starts;
        private long at = 0;
        private long ordinal = -1;
        private long end = 0;           // where the elements of sequence `ordinal` end

        Elements(SpillBitPackedBuffer store, RecordSorter<IdQuad> pairs) throws IOException {
            this.store = store;
            this.pairs = pairs;
            this.starts = (pairs == null) ? null : HugeSequences.this.starts().values();
            if (starts != null) {
                starts.next();          // 0, where the first sequence begins
            }
        }

        void write(long id) throws IOException {
            store.writeLong(id);
            if (pairs != null) {
                while (at >= end) {     // the sequences that end here, the empty ones among them
                    end = starts.next();
                    ordinal++;
                }
                pairs.add(new IdQuad(id, ordinal, 0, 0));
            }
            at++;
        }

        @Override
        public void close() throws IOException {
            if (starts != null) {
                starts.close();
            }
        }
    }

    /** The sorted pairs as the element index's three columns, each pair once. */
    private SpillBitPackedBuffer[] indexColumns(RecordSorter<IdQuad> pairs, long numObjects) throws IOException {
        SpillBitPackedBuffer elementIds = new SpillBitPackedBuffer(workDir.resolve("sequenceIndexElements.store"),
                width(numObjects));
        SpillBitPackedBuffer elementStarts = new SpillBitPackedBuffer(workDir.resolve("sequenceIndexStarts.store"),
                width(elements));
        SpillBitPackedBuffer ordinals = new SpillBitPackedBuffer(workDir.resolve("sequenceIndexSequences.store"),
                width(count));
        boolean ok = false;
        try (var sorted = pairs.sorted()) {
            long element = -1, last = -1, held = 0;
            elementStarts.writeLong(0);
            while (sorted.hasNext()) {
                IdQuad pair = sorted.next();
                if (pair.g() != element) {
                    if (element >= 0) {
                        elementStarts.writeLong(held);
                    }
                    element = pair.g();
                    elementIds.writeLong(element);
                    last = -1;
                }
                if (pair.s() != last) {
                    last = pair.s();
                    ordinals.writeLong(last);
                    held++;
                }
            }
            elementStarts.writeLong(held);
            elementIds.complete();
            elementStarts.complete();
            ordinals.complete();
            ok = true;
            return new SpillBitPackedBuffer[] {elementIds, elementStarts, ordinals};
        } finally {
            if (!ok) {
                for (SpillBitPackedBuffer b : new SpillBitPackedBuffer[] {elementIds, elementStarts, ordinals}) {
                    try { b.close(); } catch (IOException ignored) { }
                }
            }
        }
    }

    /** The spill width for values up to {@code max}: bit-packed widths are 1..57 or 64. */
    private static int width(long max) {
        int w = MinBits(Math.max(1, max));
        return (w > 57) ? 64 : w;
    }

    @Override
    public void close() throws IOException {
        refs.close();
        starts.close();
        if (indexColumns != null) {
            for (SpillBitPackedBuffer b : indexColumns) {
                b.close();
            }
        }
    }
}
