package com.ebremer.beakgraph.hdf5.jena;

import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.readers.IndexReader;
import com.ebremer.beakgraph.utils.HDTBitmapDirectory;

/**
 * Block-range resolution over the index bitmaps (one set bit per block start).
 *
 * <p>Every iterator needs "where does block {@code rank} start and end", which
 * used to cost two independent O(log n) directory descents. The end of a block
 * is just the next set bit after its start, and real blocks are usually short,
 * so {@link #blockEnd} answers with a bounded forward word-scan first and only
 * falls back to the directory when the block spans more than
 * {@value #SCAN_WORD_BUDGET} words (e.g. a giant graph's subject block) - the
 * scan-then-fallback never degrades to a linear walk.
 */
public final class RangeSelect {

    /** 16 words = 1024 bits: covers typical blocks, bounds the worst case. */
    private static final long SCAN_WORD_BUDGET = 16;

    private RangeSelect() {}

    /** Position of the {@code rank}-th set bit (block start), or -1 (also for rank &lt; 1). */
    public static long blockStart(HDTBitmapDirectory dir, BitPackedUnSignedLongBuffer bitmap, long rank) {
        if (rank < 1) return -1;
        return (dir != null) ? dir.select1(rank) : bitmap.select1(rank);
    }

    /**
     * Inclusive end of the block that starts at {@code start} (the {@code rank}-th
     * block): one before the next set bit, or the last entry when this block is
     * the final one. Prefers a bounded forward scan from {@code start + 1};
     * long blocks fall back to {@code select1(rank + 1)}.
     */
    public static long blockEnd(HDTBitmapDirectory dir, BitPackedUnSignedLongBuffer bitmap, long rank, long start) {
        long next = bitmap.nextSetBit(start + 1, SCAN_WORD_BUDGET);
        if (next == BitPackedUnSignedLongBuffer.SCAN_EXHAUSTED) {
            next = (dir != null) ? dir.select1(rank + 1) : bitmap.select1(rank + 1);
        }
        return (next == -1) ? bitmap.getNumEntries() - 1 : next - 1;
    }

    /**
     * The inclusive position range of graph {@code gi}'s block at the index's
     * first level, or null when the graph has no rows: an absent id, an empty
     * block, or a padding-only block (the writer pads each empty first-level
     * slot with a single all-zero dummy row, whose first id is 0). THE rule
     * for the COUNT and DISTINCT fast paths and the parallel scan planner,
     * which each carried a copy (BG-316).
     */
    public static long[] firstLevelRange(IndexReader ir, char component, long gi) {
        if (gi < 1) return null;
        BitPackedUnSignedLongBuffer bitmap = ir.getBitmapBuffer(component);
        HDTBitmapDirectory dir = ir.getDirectory(component);
        long start = blockStart(dir, bitmap, gi);
        if (start == -1) return null;
        long end = blockEnd(dir, bitmap, gi, start);
        if (start > end) return null;
        if (ir.getIDBuffer(component).get(start) == 0) return null;
        return new long[]{start, end};
    }

    /**
     * The inclusive position range at a child level spanned by parent positions
     * [{@code parentStart}..{@code parentEnd}]: parent position i's child block
     * starts at the (i+1)-th set bit (the addressing every BGIterator uses).
     * Null when the span is empty.
     */
    public static long[] childRange(IndexReader ir, char component, long parentStart, long parentEnd) {
        BitPackedUnSignedLongBuffer bitmap = ir.getBitmapBuffer(component);
        HDTBitmapDirectory dir = ir.getDirectory(component);
        long start = blockStart(dir, bitmap, parentStart + 1);
        if (start == -1) return null;
        long next = blockStart(dir, bitmap, parentEnd + 2);
        long end = (next == -1) ? ir.getIDBuffer(component).getNumEntries() - 1 : next - 1;
        if (start > end) return null;
        return new long[]{start, end};
    }
}
