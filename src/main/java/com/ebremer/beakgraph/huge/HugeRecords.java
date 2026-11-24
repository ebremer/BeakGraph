package com.ebremer.beakgraph.huge;

import com.ebremer.beakgraph.core.lib.NodeComparator;
import com.ebremer.beakgraph.core.lib.TermKey;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.function.ToLongFunction;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

/**
 * Record types flowing through the huge writer's spill files, with their
 * codecs and orderings.
 *
 * <p>{@code TermRow} carries one quad component (a term, as its sort key) plus
 * the row number of its quad; sorted by term it drives both dictionary
 * construction (dedup) and the sort-merge id join. {@code RowId} is the join output (row -> id), sorted
 * back into row order. {@code IdQuad} is a fully dictionary-encoded quad; its
 * two orderings are exactly the {@link com.ebremer.beakgraph.hdf5.Index}
 * comparators, because dictionary ids are rank-assigned from the same
 * {@link NodeComparator} order the RAM writer sorts quads with - numeric id
 * order and term order agree component-wise.
 *
 * @author Erich Bremer
 */
public final class HugeRecords {

    private HugeRecords() {}

    /**
     * A term and the row of its quad. The term is held as its {@link TermKey},
     * which a sort compares without working the term's place out again; the
     * rows of one term may share one key (the ingest's consecutive rows of a
     * subject, a run's rows of a term).
     */
    public record TermRow(TermKey key, long row) {

        public TermRow(Node term, long row) {
            this(new TermKey(term), row);
        }

        public Node term() {
            return key.node();
        }
    }

    public record RowId(long row, long id) {}

    public record IdQuad(long g, long s, long p, long o) {}

    /**
     * Term order only; equal terms may interleave rows arbitrarily (the join
     * maps them to one id). On the rows' keys: NodeComparator's order, with
     * each term's place in it worked out once and not for every comparison
     * (see {@link TermKey}).
     */
    public static final Comparator<TermRow> TERM_ORDER = (a, b) -> TermKey.compare(a.key(), b.key());

    /** The same order asked of {@code cmp} for every pair: what {@link #TERM_ORDER} is held to. */
    public static Comparator<TermRow> termOrder(NodeComparator cmp) {
        return (a, b) -> cmp.compare(a.term(), b.term());
    }

    public static final Comparator<RowId> ROW_ORDER = Comparator.comparingLong(RowId::row);

    /**
     * Retained-heap estimate of a term for the term sorters' byte budget:
     * object headers plus two bytes per char of every lexical part
     * (conservative for Latin-1 compact strings). Exactness is not the point -
     * the budget bounds the buffer to within a small factor of the truth,
     * which the record cap never did for multi-KB literals (BG-125).
     */
    public static long estimateBytes(Node n) {
        if (n == null) {
            return 8;
        }
        if (n.isLiteral()) {
            long b = 96 + 2L * n.getLiteralLexicalForm().length();
            String lang = n.getLiteralLanguage();
            if (lang != null && !lang.isEmpty()) {
                b += 2L * lang.length();
            }
            return b;
        }
        if (n.isURI()) {
            return 64 + 2L * n.getURI().length();
        }
        if (n.isBlank()) {
            return 64 + 2L * n.getBlankNodeLabel().length();
        }
        if (n.isTripleTerm()) {
            Triple t = n.getTriple();
            return 64 + estimateBytes(t.getSubject()) + estimateBytes(t.getPredicate()) + estimateBytes(t.getObject());
        }
        if (n.isNodeSeq()) {
            // The node, its element array, and each element (a slot is shared).
            long b = 64 + 16 + 8L * n.getSeq().size();
            for (Node e : n.getSeq()) {
                if (!e.isNodeUndef()) {
                    b += estimateBytes(e);
                }
            }
            return b;
        }
        return 64;
    }

    /** {@link #estimateBytes} of a term row: the record, its term, and what the term's key holds once it is sorted. */
    public static final ToLongFunction<TermRow> TERM_ROW_BYTES = r -> 32 + TermKey.HEAP_BYTES + estimateBytes(r.term());

    public static final Comparator<IdQuad> GSPO_ORDER = Comparator
            .comparingLong(IdQuad::g)
            .thenComparingLong(IdQuad::s)
            .thenComparingLong(IdQuad::p)
            .thenComparingLong(IdQuad::o);

    public static final Comparator<IdQuad> GPOS_ORDER = Comparator
            .comparingLong(IdQuad::g)
            .thenComparingLong(IdQuad::p)
            .thenComparingLong(IdQuad::o)
            .thenComparingLong(IdQuad::s);

    public static final ExternalSorter.Codec<TermRow> TERM_ROW_CODEC = new ExternalSorter.Codec<>() {
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

    public static final ExternalSorter.Codec<RowId> ROW_ID_CODEC = new ExternalSorter.Codec<>() {
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
    public static final ExternalSorter.Codec<Long> VAR_LONG_CODEC = new ExternalSorter.Codec<>() {
        @Override
        public void write(DataOutput out, Long record) throws IOException {
            HugeIO.writeVarLong(out, record);
        }

        @Override
        public Long read(DataInput in) throws IOException {
            return HugeIO.readVarLong(in);
        }
    };

    public static final ExternalSorter.Codec<IdQuad> ID_QUAD_CODEC = new ExternalSorter.Codec<>() {
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
