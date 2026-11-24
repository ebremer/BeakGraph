package com.ebremer.beakgraph.huge;

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
 */
final class HugeSequences implements AutoCloseable {

    private final SorterProvider provider;
    private final Path workDir;
    private final RecordSorter<TermRow> refs;
    private final SpillBitPackedBuffer starts;
    private long count = 0;
    private long elements = 0;
    private long slots = 0;

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
        RecordSorter<RowId> resolved = provider.rowIdSorter(workDir, "seqids", spilled, numObjects);
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
        boolean ok = false;
        try (var ids = resolved.sorted()) {
            long next = 0;
            long zeros = 0;
            while (ids.hasNext()) {
                RowId r = ids.next();
                if (r.row() < next) {
                    throw new IllegalStateException("Sequence element misalignment: index " + r.row()
                            + " where " + next + " or later was expected");
                }
                while (next < r.row()) {     // the slots before it
                    store.writeLong(0);
                    next++;
                    zeros++;
                }
                store.writeLong(r.id());
                next++;
            }
            while (next < elements) {        // trailing slots
                store.writeLong(0);
                next++;
                zeros++;
            }
            if (zeros != slots) {
                throw new IllegalStateException("Resolved " + (elements - zeros) + " sequence elements and "
                        + zeros + " slots where " + (elements - slots) + " and " + slots + " were recorded");
            }
            store.complete();
            ok = true;
            return store;
        } finally {
            resolved.close();
            if (!ok) {
                try { store.close(); } catch (IOException ignored) { }
            }
        }
    }

    @Override
    public void close() throws IOException {
        refs.close();
        starts.close();
    }
}
