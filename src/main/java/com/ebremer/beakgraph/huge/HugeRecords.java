package com.ebremer.beakgraph.huge;

import com.ebremer.beakgraph.core.lib.NodeComparator;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import org.apache.jena.graph.Node;

/**
 * Record types flowing through the huge writer's spill files, with their
 * codecs and orderings.
 *
 * <p>{@code TermRow} carries one quad component (a term) plus the row number of
 * its quad; sorted by term it drives both dictionary construction (dedup) and
 * the sort-merge id join. {@code RowId} is the join output (row -> id), sorted
 * back into row order. {@code IdQuad} is a fully dictionary-encoded quad; its
 * two orderings are exactly the {@link com.ebremer.beakgraph.hdf5.Index}
 * comparators, because dictionary ids are rank-assigned from the same
 * {@link NodeComparator} order the RAM writer sorts quads with - numeric id
 * order and term order agree component-wise.
 *
 * @author Erich Bremer
 */
final class HugeRecords {

    private HugeRecords() {}

    record TermRow(Node term, long row) {}

    record RowId(long row, long id) {}

    record IdQuad(long g, long s, long p, long o) {}

    /** Term order only; equal terms may interleave rows arbitrarily (the join maps them to one id). */
    static final Comparator<TermRow> TERM_ORDER =
            (a, b) -> NodeComparator.INSTANCE.compare(a.term(), b.term());

    static final Comparator<RowId> ROW_ORDER = Comparator.comparingLong(RowId::row);

    static final Comparator<IdQuad> GSPO_ORDER = Comparator
            .comparingLong(IdQuad::g)
            .thenComparingLong(IdQuad::s)
            .thenComparingLong(IdQuad::p)
            .thenComparingLong(IdQuad::o);

    static final Comparator<IdQuad> GPOS_ORDER = Comparator
            .comparingLong(IdQuad::g)
            .thenComparingLong(IdQuad::p)
            .thenComparingLong(IdQuad::o)
            .thenComparingLong(IdQuad::s);

    static final ExternalSorter.Codec<TermRow> TERM_ROW_CODEC = new ExternalSorter.Codec<>() {
        @Override
        public void write(DataOutput out, TermRow record) throws IOException {
            NodeCodec.writeNode(out, record.term());
            HugeIO.writeVarLong(out, record.row());
        }

        @Override
        public TermRow read(DataInput in) throws IOException {
            Node term = NodeCodec.readNode(in);
            long row = HugeIO.readVarLong(in);
            return new TermRow(term, row);
        }
    };

    static final ExternalSorter.Codec<RowId> ROW_ID_CODEC = new ExternalSorter.Codec<>() {
        @Override
        public void write(DataOutput out, RowId record) throws IOException {
            HugeIO.writeVarLong(out, record.row());
            HugeIO.writeVarLong(out, record.id());
        }

        @Override
        public RowId read(DataInput in) throws IOException {
            long row = HugeIO.readVarLong(in);
            long id = HugeIO.readVarLong(in);
            return new RowId(row, id);
        }
    };

    /** Bare non-negative longs (the per-row predicate temp/final id files). */
    static final ExternalSorter.Codec<Long> VAR_LONG_CODEC = new ExternalSorter.Codec<>() {
        @Override
        public void write(DataOutput out, Long record) throws IOException {
            HugeIO.writeVarLong(out, record);
        }

        @Override
        public Long read(DataInput in) throws IOException {
            return HugeIO.readVarLong(in);
        }
    };

    static final ExternalSorter.Codec<IdQuad> ID_QUAD_CODEC = new ExternalSorter.Codec<>() {
        @Override
        public void write(DataOutput out, IdQuad record) throws IOException {
            HugeIO.writeVarLong(out, record.g());
            HugeIO.writeVarLong(out, record.s());
            HugeIO.writeVarLong(out, record.p());
            HugeIO.writeVarLong(out, record.o());
        }

        @Override
        public IdQuad read(DataInput in) throws IOException {
            long g = HugeIO.readVarLong(in);
            long s = HugeIO.readVarLong(in);
            long p = HugeIO.readVarLong(in);
            long o = HugeIO.readVarLong(in);
            return new IdQuad(g, s, p, o);
        }
    };
}
