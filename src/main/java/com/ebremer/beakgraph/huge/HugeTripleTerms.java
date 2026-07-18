package com.ebremer.beakgraph.huge;

import com.ebremer.beakgraph.huge.HugeRecords.RowId;
import com.ebremer.beakgraph.huge.HugeRecords.TermRow;
import static com.ebremer.beakgraph.utils.UTIL.MinBits;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

/**
 * Disk-engine triple-term component machinery (PLAN Part IV §IV.8, realizing
 * §3.2.7's join stage): during the literals-dictionary encode, each triple
 * term's component TERMS spill as {@code (term, key)} records with
 * {@code key = ordinal*3 + slot}; after the encode, one term-ordered pass -
 * merged against the entity and literal dictionary files with monotone
 * cursors, plus the in-RAM predicate rank map - resolves every reference to an
 * id with no random lookups, and a key-ordered second sort lays the ids out as
 * the fixed-stride {@code tripleTerms} store.
 *
 * <p>Why one pass suffices: the reference stream is NodeComparator-sorted, so
 * entity components (bnodes, IRIs) form its prefix and literal/nested-term
 * components its suffix - each dictionary cursor only ever moves forward.
 * Predicate references are IRIs interleaved in the prefix; they resolve
 * through the RAM map and never advance either cursor.
 */
final class HugeTripleTerms implements AutoCloseable {

    private final SorterProvider provider;
    private final Path workDir;
    private final RecordSorter<TermRow> refs;
    private final Map<Node, Long> predicateIds;
    private long count = 0;

    /**
     * @param predicateIds final (rank) id per predicate node - complete before
     *                     any dictionary encode runs, because the pipeline
     *                     sorts predicates in RAM first
     */
    HugeTripleTerms(SorterProvider provider, Path workDir, Map<Node, Long> predicateIds) {
        this.provider = provider;
        this.workDir = workDir;
        this.refs = provider.termSorter(workDir, "ttrefs");
        this.predicateIds = predicateIds;
    }

    /**
     * Writer callback for one triple-term dictionary row, called in section
     * order; spills the three component references and returns the term's
     * 0-based ordinal - exactly the row's {@code offsets} value.
     */
    long onTripleTerm(Node tt) throws IOException {
        long k = count++;
        Triple t = tt.getTriple();
        refs.add(new TermRow(t.getSubject(), k * 3));
        refs.add(new TermRow(t.getPredicate(), k * 3 + 1));
        refs.add(new TermRow(t.getObject(), k * 3 + 2));
        return k;
    }

    long count() {
        return count;
    }

    /**
     * Resolves every spilled reference and returns the completed fixed-stride
     * component store (width sized to the object space, entries in
     * {@code (ordinal, slot)} order). Must run after {@code entFile} and
     * {@code litFile} are complete and before the pipeline deletes them.
     */
    SpillBitPackedBuffer resolve(RecordFile<Node> entFile, RecordFile<Node> litFile,
                                 long numEntities, long numObjects) throws IOException {
        RecordSorter<RowId> resolved = provider.rowIdSorter(workDir, "ttids", count * 3, numObjects);
        try (var refStream = refs.sorted();
             var ents = entFile.read();
             var lits = litFile.read()) {
            HugeBuildPipeline.DictCursor entCur = new HugeBuildPipeline.DictCursor(ents, () -> {});
            HugeBuildPipeline.DictCursor litCur = new HugeBuildPipeline.DictCursor(lits, () -> {});
            while (refStream.hasNext()) {
                TermRow ref = refStream.next();
                long key = ref.row();
                Node term = ref.term();
                long id;
                if (key % 3 == 1) {
                    Long p = predicateIds.get(term);
                    if (p == null) {
                        throw new IllegalStateException(
                                "Cannot resolve triple-term predicate (not in dictionary): " + term);
                    }
                    id = p;
                } else if (term.isLiteral() || term.isTripleTerm()) {
                    id = litCur.locate(term, "triple-term object") + numEntities;
                } else {
                    id = entCur.locate(term, "triple-term component");
                }
                resolved.add(new RowId(key, id));
            }
        } finally {
            refs.close();
        }

        int width = 1 + MinBits(numObjects);
        if (width > 57) width = 64; // bit-packed widths are 1..57 or 64
        SpillBitPackedBuffer store = new SpillBitPackedBuffer(workDir.resolve("tripleTerms.store"), width);
        try (var ids = resolved.sorted()) {
            long expect = 0;
            while (ids.hasNext()) {
                RowId r = ids.next();
                if (r.row() != expect) {
                    throw new IllegalStateException("Triple-term component misalignment: key "
                            + r.row() + " where " + expect + " was expected");
                }
                expect++;
                store.writeLong(r.id());
            }
            if (expect != count * 3) {
                throw new IllegalStateException("Resolved " + expect
                        + " triple-term components for " + count + " terms");
            }
        } finally {
            resolved.close();
        }
        store.complete();
        return store;
    }

    @Override
    public void close() throws IOException {
        refs.close();
    }
}
