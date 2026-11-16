package com.ebremer.beakgraph.io;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Blocks fetched of a remote store, the one read longest ago dropped first:
 * ONE budget for everything read of the store, however many objects it is
 * kept in. An HDF5 file over HTTP is one object, and its channel has a cache
 * to itself; a Zarr store is an object an array (or more), and a cache each
 * would hold many times the memory, or far too little for the arrays a query
 * actually reads.
 *
 * <p>Each object reads and fills its own {@link Part} of the cache, by block
 * index. Safe for concurrent use: every operation is atomic, and none waits
 * on the network.
 *
 * @author Erich Bremer
 */
public final class BlockCache {

    /** A block index is kept under the bits of its part: 2^40 blocks an object, 2^23 objects. */
    private static final int INDEX_BITS = 40;
    private static final int MAX_PARTS = 1 << 23;

    private final int blockSize;
    private final int maxBlocks;
    private final LinkedHashMap<Long, byte[]> blocks;
    private int parts;

    /**
     * @param blockSize the bytes of a block: what is fetched at a time, and aligned to
     * @param maxBlocks the most blocks kept; the cache holds at most {@code blockSize * maxBlocks} bytes
     */
    public BlockCache(int blockSize, int maxBlocks) {
        if (blockSize <= 0 || maxBlocks <= 0) {
            throw new IllegalArgumentException("blockSize and maxBlocks must be positive: " + blockSize + ", " + maxBlocks);
        }
        this.blockSize = blockSize;
        this.maxBlocks = maxBlocks;
        this.blocks = new LinkedHashMap<>(16, 0.75f, true) {
            private static final long serialVersionUID = 1L;

            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, byte[]> eldest) {
                return size() > BlockCache.this.maxBlocks;
            }
        };
    }

    /** The bytes of a block. */
    public int blockSize() {
        return blockSize;
    }

    /** The most blocks kept. */
    public int maxBlocks() {
        return maxBlocks;
    }

    /** The blocks held now. */
    public synchronized int size() {
        return blocks.size();
    }

    /** A part of the cache for the blocks of one more object. */
    public synchronized Part part() {
        if (parts == MAX_PARTS) {
            throw new IllegalStateException("a block cache serves at most " + MAX_PARTS + " objects");
        }
        return new Part(parts++);
    }

    /** The blocks of one object, among those of the others. */
    public final class Part {

        private final long base;

        private Part(int id) {
            this.base = (long) id << INDEX_BITS;
        }

        private long key(long index) {
            if (index < 0 || index >>> INDEX_BITS != 0) {
                throw new IndexOutOfBoundsException("block " + index);
            }
            return base | index;
        }

        /** The block at {@code index}, now the most recently read; or null. */
        public byte[] get(long index) {
            long key = key(index);
            synchronized (BlockCache.this) {
                return blocks.get(key);
            }
        }

        /** Keeps the block at {@code index}; the block read longest ago goes if the cache is full. */
        public void put(long index, byte[] block) {
            long key = key(index);
            synchronized (BlockCache.this) {
                blocks.put(key, block);
            }
        }

        /** Drops this object's blocks, and no other's. */
        public void clear() {
            synchronized (BlockCache.this) {
                blocks.keySet().removeIf(k -> (k & -(1L << INDEX_BITS)) == base);
            }
        }
    }
}
