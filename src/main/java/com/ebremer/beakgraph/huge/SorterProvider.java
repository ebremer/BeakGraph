package com.ebremer.beakgraph.huge;

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

    /** The sequential engine used by -method 1: plain {@link ExternalSorter}s. */
    static SorterProvider sequential(int termSpillBatch, int idSpillBatch, int mergeFanIn) {
        return new SorterProvider() {
            @Override
            public RecordSorter<HugeRecords.TermRow> termSorter(Path workDir, String tag) {
                return new ExternalSorter<>(workDir, tag, HugeRecords.TERM_ROW_CODEC,
                        HugeRecords.TERM_ORDER, termSpillBatch, mergeFanIn);
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
