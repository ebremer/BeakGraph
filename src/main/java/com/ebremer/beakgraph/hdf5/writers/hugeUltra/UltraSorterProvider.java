package com.ebremer.beakgraph.hdf5.writers.hugeUltra;

import com.ebremer.beakgraph.core.lib.TermKey;
import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.huge.HugeIO;
import com.ebremer.beakgraph.huge.HugeRecords;
import com.ebremer.beakgraph.huge.HugeRecords.RowId;
import com.ebremer.beakgraph.huge.HugeRecords.TermRow;
import com.ebremer.beakgraph.huge.NodeCodec;
import com.ebremer.beakgraph.huge.RecordSorter;
import com.ebremer.beakgraph.huge.SorterProvider;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ForkJoinPool;

/**
 * The -method 4 sorter suite. Terms sort on a {@link ParallelSpillSorter}
 * with two accelerations: the rows' sort keys ({@link TermKey}), which hold
 * each term's place in NodeComparator's order so that a comparison reads
 * two of them instead of working both out again, and a grouped run format
 * that writes each term's text once per run (with a varint row list)
 * instead of once per occurrence. Id records (row joins and
 * encoded quads) sort on bit-packed primitive sorters with no objects at all.
 */
public final class UltraSorterProvider implements SorterProvider {

    private final int termSpillBatch;
    private final int idSpillBatch;
    private final int fanIn;
    private final long termSpillBytes;
    private final int maxConcurrentMerges;
    private final ForkJoinPool pool;

    /** File descriptors a sorter level may hold in open runs by default; sizes the merge concurrency. */
    public static final int DEFAULT_FD_BUDGET = 1024;

    /**
     * Merge groups one sorter runs at a time: at most one per core, and no
     * more than fit {@link #DEFAULT_FD_BUDGET} open files at {@code fanIn + 1}
     * per running merge. A level's merges used to be submitted all at once,
     * so -cores 32 with fan-in 128 held ~4000 descriptors and 32 batches of
     * live records (BG-133).
     */
    public static int defaultMergeConcurrency(int fanIn, int cores) {
        return Math.max(1, Math.min(Math.max(1, cores), DEFAULT_FD_BUDGET / (Math.max(2, fanIn) + 1)));
    }

    /** With the default term byte budget ({@link SorterProvider#defaultTermSpillBytes}, two batches in flight). */
    public UltraSorterProvider(int termSpillBatch, int idSpillBatch, int fanIn, ForkJoinPool pool) {
        this(termSpillBatch, idSpillBatch, fanIn, SorterProvider.defaultTermSpillBytes(2), pool);
    }

    /**
     * @param termSpillBytes estimated term heap one term sorter buffers before
     *                       a run spills, whatever the record count (BG-125)
     */
    public UltraSorterProvider(int termSpillBatch, int idSpillBatch, int fanIn, long termSpillBytes, ForkJoinPool pool) {
        this(termSpillBatch, idSpillBatch, fanIn, termSpillBytes,
                defaultMergeConcurrency(fanIn, pool.getParallelism()), pool);
    }

    /** @param maxConcurrentMerges merge groups one sorter level runs at a time (BG-133) */
    public UltraSorterProvider(int termSpillBatch, int idSpillBatch, int fanIn, long termSpillBytes,
                               int maxConcurrentMerges, ForkJoinPool pool) {
        this.termSpillBatch = termSpillBatch;
        this.idSpillBatch = idSpillBatch;
        this.fanIn = fanIn;
        this.termSpillBytes = termSpillBytes;
        this.maxConcurrentMerges = Math.max(1, maxConcurrentMerges);
        this.pool = pool;
    }

    @Override
    public RecordSorter<TermRow> termSorter(Path workDir, String tag) {
        // On the rows' keys: a literal's value is made once for the key,
        // without Jena's global cache, and not once more for every comparison.
        return new ParallelSpillSorter<>(workDir, tag,
                HugeRecords.TERM_ORDER,
                new GroupedTermFormat(), termSpillBatch, fanIn, pool,
                HugeRecords.TERM_ROW_BYTES, termSpillBytes, maxConcurrentMerges);
    }

    @Override
    public RecordSorter<RowId> rowIdSorter(Path workDir, String tag, long maxRow, long maxId) {
        return new PackedRowIdSorter(workDir, tag, maxRow, maxId, idSpillBatch, fanIn, pool, pool, maxConcurrentMerges);
    }

    @Override
    public RecordSorter<HugeRecords.IdQuad> quadSorter(Path workDir, String tag, Index order,
                                                       long numEntities, long numPredicates, long numObjects) {
        return new PackedQuadSorter(workDir, tag, order, numEntities, numPredicates, numObjects,
                idSpillBatch, fanIn, pool, pool, maxConcurrentMerges);
    }

    // ------------------------------------------------------------------
    // grouped term run format
    // ------------------------------------------------------------------

    /**
     * Run layout: (term, count, row*count)* - each distinct term's text hits
     * the disk once per run. Runs are sorted, so equal terms are consecutive;
     * intermediate merges re-buffer and re-group, so repeats collapse further
     * at every merge level. Read back, the rows of a term share ONE sort key,
     * so a merge works a term's place in the order out once a run, not once
     * a row.
     */
    static final class GroupedTermFormat implements ParallelSpillSorter.RunFormat<TermRow> {

        @Override
        public void writeRun(DataOutput out, Object[] sorted, int n) throws IOException {
            int i = 0;
            while (i < n) {
                TermRow first = (TermRow) sorted[i];
                int j = i + 1;
                while (j < n && sameTerm(first, (TermRow) sorted[j])) {
                    j++;
                }
                NodeCodec.writeNode(out, first.term());
                HugeIO.writeVarLong(out, j - i);
                for (int k = i; k < j; k++) {
                    HugeIO.writeVarLong(out, ((TermRow) sorted[k]).row());
                }
                i = j;
            }
        }

        /** Rows that share a key are of one term without a look at it. */
        private static boolean sameTerm(TermRow a, TermRow b) {
            return a.key() == b.key() || a.term().equals(b.term());
        }

        @Override
        public RunStream<TermRow> newStream() {
            return new RunStream<>() {
                private TermKey term;
                private long remaining = 0;

                @Override
                public TermRow read(DataInput in) throws IOException {
                    if (remaining == 0) {
                        term = new TermKey(NodeCodec.readNode(in)); // EOFException here ends the run
                        remaining = HugeIO.readVarLong(in);
                    }
                    remaining--;
                    return new TermRow(term, HugeIO.readVarLong(in));
                }
            };
        }
    }
}
