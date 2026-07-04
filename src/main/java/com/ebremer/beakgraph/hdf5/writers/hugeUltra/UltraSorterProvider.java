package com.ebremer.beakgraph.hdf5.writers.hugeUltra;

import com.ebremer.beakgraph.core.lib.NodeComparator;
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
import java.util.Comparator;
import java.util.concurrent.ForkJoinPool;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;

/**
 * The -method 4 sorter suite. Terms sort on a {@link ParallelSpillSorter}
 * with two accelerations: an order-preserving 8-byte prefix key that settles
 * most comparisons without touching the expensive NodeComparator, and a
 * grouped run format that writes each term's text once per run (with a
 * varint row list) instead of once per occurrence. Id records (row joins and
 * encoded quads) sort on bit-packed primitive sorters with no objects at all.
 */
public final class UltraSorterProvider implements SorterProvider {

    private final int termSpillBatch;
    private final int idSpillBatch;
    private final int fanIn;
    private final ForkJoinPool pool;

    public UltraSorterProvider(int termSpillBatch, int idSpillBatch, int fanIn, ForkJoinPool pool) {
        this.termSpillBatch = termSpillBatch;
        this.idSpillBatch = idSpillBatch;
        this.fanIn = fanIn;
        this.pool = pool;
    }

    @Override
    public RecordSorter<TermRow> termSorter(Path workDir, String tag) {
        // A PER-SORTER comparator: literal NodeValue conversions memoize
        // locally instead of contending on Jena's global cache (see
        // CachingNodeComparator). Cap ~= two spill batches of distinct nodes.
        return new ParallelSpillSorter<>(workDir, tag,
                fastTermOrder(new CachingNodeComparator(Math.max(1 << 16, termSpillBatch * 2))),
                new GroupedTermFormat(), termSpillBatch, fanIn, pool);
    }

    @Override
    public RecordSorter<RowId> rowIdSorter(Path workDir, String tag, long maxRow, long maxId) {
        return new PackedRowIdSorter(workDir, tag, maxRow, maxId, idSpillBatch, fanIn, pool, pool);
    }

    @Override
    public RecordSorter<HugeRecords.IdQuad> quadSorter(Path workDir, String tag, Index order,
                                                       long numEntities, long numPredicates, long numObjects) {
        return new PackedQuadSorter(workDir, tag, order, numEntities, numPredicates, numObjects,
                idSpillBatch, fanIn, pool, pool);
    }

    // ------------------------------------------------------------------
    // prefix-key acceleration
    // ------------------------------------------------------------------

    /**
     * An 8-byte key with the ONE property that matters: {@code key(a) != key(b)}
     * implies unsigned key order equals NodeComparator order; equal keys fall
     * back to the full comparator. Byte 7 is the macro rank (default-graph
     * sentinels 0, bnode 1, URI 2, literal 3 - NodeComparator's exact macro
     * order); bytes 6..0 are the first chars of the label/URI, ASCII-compared
     * exactly as {@code String.compareTo} does. Any char &ge; 0xFF becomes a
     * terminal 0xFF marker (chars past it are NOT encoded - encoding them
     * would disagree with UTF-16 order). Literals encode rank only: their
     * order is value-based and cannot be prefixed cheaply.
     */
    static long prefixKey(Node n) {
        int rank;
        String s;
        if (n == null || n.equals(Quad.defaultGraphIRI) || n.equals(Quad.defaultGraphNodeGenerated)) {
            rank = 0;
            s = (n == null ? Quad.defaultGraphIRI : n).getURI();
        } else if (n.isBlank()) {
            rank = 1;
            s = n.getBlankNodeLabel();
        } else if (n.isURI()) {
            rank = 2;
            s = n.getURI();
        } else {
            rank = 3;
            s = null;
        }
        long key = ((long) rank) << 56;
        if (s != null) {
            int len = Math.min(7, s.length());
            int shift = 48;
            for (int i = 0; i < len; i++, shift -= 8) {
                char c = s.charAt(i);
                if (c >= 0xFF) {
                    key |= 0xFFL << shift;
                    break;
                }
                key |= ((long) c) << shift;
            }
        }
        return key;
    }

    /**
     * TERM_ORDER, but with most comparisons settled by the prefix key and the
     * remainder (dominated by literal-vs-literal) running through the given
     * comparator - a {@link CachingNodeComparator} in production, so parallel
     * sorts never contend on Jena's global NodeValue cache.
     */
    static Comparator<TermRow> fastTermOrder(NodeComparator full) {
        return (a, b) -> {
            int c = Long.compareUnsigned(prefixKey(a.term()), prefixKey(b.term()));
            return (c != 0) ? c : full.compare(a.term(), b.term());
        };
    }

    /** The prefix-accelerated order on the SHARED NodeComparator (tests). */
    static final Comparator<TermRow> FAST_TERM_ORDER = fastTermOrder(NodeComparator.INSTANCE);

    // ------------------------------------------------------------------
    // grouped term run format
    // ------------------------------------------------------------------

    /**
     * Run layout: (term, count, row*count)* - each distinct term's text hits
     * the disk once per run. Runs are sorted, so equal terms are consecutive;
     * intermediate merges re-buffer and re-group, so repeats collapse further
     * at every merge level.
     */
    static final class GroupedTermFormat implements ParallelSpillSorter.RunFormat<TermRow> {

        @Override
        public void writeRun(DataOutput out, Object[] sorted, int n) throws IOException {
            int i = 0;
            while (i < n) {
                TermRow first = (TermRow) sorted[i];
                int j = i + 1;
                while (j < n && ((TermRow) sorted[j]).term().equals(first.term())) {
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

        @Override
        public RunStream<TermRow> newStream() {
            return new RunStream<>() {
                private Node term;
                private long remaining = 0;

                @Override
                public TermRow read(DataInput in) throws IOException {
                    if (remaining == 0) {
                        term = NodeCodec.readNode(in); // EOFException here ends the run
                        remaining = HugeIO.readVarLong(in);
                    }
                    remaining--;
                    return new TermRow(term, HugeIO.readVarLong(in));
                }
            };
        }
    }
}
