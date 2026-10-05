package com.ebremer.beakgraph.huge;

import com.ebremer.beakgraph.core.lib.CachingNodeComparator;
import com.ebremer.beakgraph.hdf5.Index;
import java.nio.file.Path;

/**
 * Factory for the sorters {@link HugeBuildPipeline} runs on. The pipeline
 * tells the provider what is being sorted and (for the id-record sorters) the
 * value ranges involved, so implementations can pick representations - the
 * default wraps the sequential {@link ExternalSorter}; the hugeUltra provider
 * returns background-spilling parallel sorters and bit-packed primitive
 * sorters instead.
 */
public interface SorterProvider {

    /** Sorter for one term column's (term, row) records, in NodeComparator term order. */
    RecordSorter<HugeRecords.TermRow> termSorter(Path workDir, String tag);

    /**
     * Sorter for (row, id) join results back into row order.
     * {@code maxRow}/{@code maxId} bound the values that will be added.
     */
    RecordSorter<HugeRecords.RowId> rowIdSorter(Path workDir, String tag, long maxRow, long maxId);

    /**
     * Sorter for dictionary-encoded quads in {@code order}'s component order.
     * The three counts bound the ids (graph/subject ids &le; numEntities,
     * predicate ids &le; numPredicates, object ids &le; numObjects).
     */
    RecordSorter<HugeRecords.IdQuad> quadSorter(Path workDir, String tag, Index order,
                                                long numEntities, long numPredicates, long numObjects);

    /**
     * Default per-sorter byte budget for buffered term records: one eighth
     * of the heap divided by the batches one sorter can hold at once (1 for
     * the sequential {@link ExternalSorter}, 2 for the background-spilling
     * hugeUltra sorter), so the three live term sorters (graph, subject,
     * object) together buffer at most about 3/8 of the heap however large the
     * literals are (BG-125). Never below 16 MiB.
     */
    static long defaultTermSpillBytes(int batchesInFlight) {
        return Math.max(16L << 20, Runtime.getRuntime().maxMemory() / (8L * Math.max(1, batchesInFlight)));
    }

    /** The sequential engine used by -method 1 with the default term byte budget. */
    static SorterProvider sequential(int termSpillBatch, int idSpillBatch, int mergeFanIn) {
        return sequential(termSpillBatch, idSpillBatch, mergeFanIn, defaultTermSpillBytes(1));
    }

    /**
     * The sequential engine used by -method 1: plain {@link ExternalSorter}s;
     * term runs spill at {@code termSpillBatch} records or
     * {@code termSpillBytes} of estimated term heap, whichever comes first.
     */
    static SorterProvider sequential(int termSpillBatch, int idSpillBatch, int mergeFanIn, long termSpillBytes) {
        return new SorterProvider() {
            @Override
            public RecordSorter<HugeRecords.TermRow> termSorter(Path workDir, String tag) {
                // A PER-SORTER memoizing comparator, as the hugeUltra provider
                // has always had (BG-249). Cap ~= two spill batches of nodes.
                int memo = (int) Math.min(1 << 22, Math.max(1 << 16, 2L * termSpillBatch));
                return new ExternalSorter<>(workDir, tag, HugeRecords.TERM_ROW_CODEC,
                        HugeRecords.termOrder(new CachingNodeComparator(memo)), termSpillBatch, mergeFanIn,
                        HugeRecords.TERM_ROW_BYTES, termSpillBytes);
            }

            @Override
            public RecordSorter<HugeRecords.RowId> rowIdSorter(Path workDir, String tag, long maxRow, long maxId) {
                return new ExternalSorter<>(workDir, tag, HugeRecords.ROW_ID_CODEC,
                        HugeRecords.ROW_ORDER, idSpillBatch, mergeFanIn);
            }

            @Override
            public RecordSorter<HugeRecords.IdQuad> quadSorter(Path workDir, String tag, Index order,
                                                               long numEntities, long numPredicates, long numObjects) {
                return new ExternalSorter<>(workDir, tag, HugeRecords.ID_QUAD_CODEC,
                        order == Index.GSPO ? HugeRecords.GSPO_ORDER : HugeRecords.GPOS_ORDER,
                        idSpillBatch, mergeFanIn);
            }
        };
    }
}
