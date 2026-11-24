package com.ebremer.beakgraph.store;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A {@link TypedArrayView} read a block of rows at a time - the rows one
 * decode yields, a compressed sub-chunk - keeping the most recently used
 * blocks decoded, so the rows of one sequence after another cost one decode
 * per block, not one per read. A read that spans blocks goes straight through.
 */
final class BlockCachedView implements TypedArrayView {

    /** About this many bytes of decoded blocks are kept. */
    static final long CACHE_BYTES = 16L << 20;

    private final TypedArrayView view;
    private final long blockRows;
    private final long rows;
    private final long rowElements;
    private final long maxBlocks;
    private final LinkedHashMap<Long, Object> blocks = new LinkedHashMap<>(16, 0.75f, true);

    BlockCachedView(TypedArrayView view, long blockRows) {
        this.view = view;
        this.blockRows = blockRows;
        long[] shape = view.shape();
        this.rows = shape[0];
        this.rowElements = TypedArray.rowElements(shape);
        long blockBytes = Math.max(1, blockRows * rowElements * view.type().bytes());
        this.maxBlocks = Math.max(2, CACHE_BYTES / blockBytes);
    }

    @Override
    public TypedArray.Type type() {
        return view.type();
    }

    @Override
    public long[] shape() {
        return view.shape();
    }

    @Override
    public Object readRows(long first, long count) {
        TypedArrayView.box(view.shape(), first, count);   // bounds
        if (count == 0) {
            return view.readRows(first, 0);
        }
        long b = first / blockRows;
        if ((first + count - 1) / blockRows != b) {
            return view.readRows(first, count);
        }
        Object block;
        synchronized (blocks) {
            block = blocks.get(b);
        }
        if (block == null) {
            long start = b * blockRows;
            block = view.readRows(start, Math.min(blockRows, rows - start));   // outside the lock: two loads at worst
            synchronized (blocks) {
                blocks.put(b, block);
                Iterator<Map.Entry<Long, Object>> it = blocks.entrySet().iterator();
                while (blocks.size() > maxBlocks && it.hasNext()) {
                    it.next();
                    it.remove();
                }
            }
        }
        int from = Math.toIntExact((first - b * blockRows) * rowElements);
        return TypedArray.slice(view.type(), block, from, Math.toIntExact(from + count * rowElements));
    }
}
