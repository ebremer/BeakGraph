package com.ebremer.beakgraph.utils;

import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
//import com.ebremer.beakgraph.HDTish.Handy.Pair;
import java.util.Iterator;

/**
 * Implements rank and select directory operations for an RRR-like bitmap structure.
 * Uses precomputed superblock and block rank vectors.
 */
public class RRR {

    private final BitPackedUnSignedLongBuffer superblockAbsRanks;
    private final BitPackedUnSignedLongBuffer blockRelRanks;
    private final int blockBitSize;
    private final int superblockBitSize;
    private final int blocksPerSuperblock;
    private final int blockRankStridePerSuperblock;
    private final int numSuperblocks;

    public RRR(
        BitPackedUnSignedLongBuffer superblockAbsoluteRanks,
        BitPackedUnSignedLongBuffer blockRelativeRanks,
        int superblockBitSize,
        int blockBitSize
    ) {
        if (superblockAbsoluteRanks == null) {
            throw new IllegalArgumentException("superblockAbsoluteRanks cannot be null.");
        }
        if (blockBitSize <= 0) {
            throw new IllegalArgumentException("blockBitSize must be positive.");
        }
        if (superblockBitSize <= 0) {
            throw new IllegalArgumentException("superblockBitSize must be positive.");
        }
        if (superblockBitSize % blockBitSize != 0) {
            throw new IllegalArgumentException("superblockBitSize must be a multiple of blockBitSize.");
        }
        this.blockBitSize = blockBitSize;
        this.superblockBitSize = superblockBitSize;
        this.blocksPerSuperblock = superblockBitSize / blockBitSize;
        this.blockRankStridePerSuperblock = this.blocksPerSuperblock + 1;

        long sbEntries = superblockAbsoluteRanks.getNumEntries();
        if (sbEntries == 1 && superblockAbsoluteRanks.get(0) == 0) {
            this.numSuperblocks = 0;
        } else if (sbEntries >= 2) {
            long nsb = sbEntries - 1;
            if (nsb > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Number of superblocks exceeds Integer.MAX_VALUE.");
            }
            this.numSuperblocks = (int) nsb;
            if (superblockAbsoluteRanks.get(0) != 0) {
                throw new IllegalArgumentException(
                    "superblockAbsoluteRanks.get(0) must be 0 for non-empty bitmaps.");
            }
        } else {
            throw new IllegalArgumentException(
                "superblockAbsoluteRanks must have exactly 1 entry [0] for empty bitmap, or N+1 entries (first 0) for non-empty.");
        }

        this.superblockAbsRanks = superblockAbsoluteRanks;
        this.blockRelRanks = blockRelativeRanks;

        long expectedBlockRanksLength = (long) this.numSuperblocks * this.blockRankStridePerSuperblock;
        if (this.numSuperblocks == 0) {
            if (blockRelativeRanks != null && blockRelativeRanks.getNumEntries() != 0) {
                throw new IllegalArgumentException(
                    "blockRelativeRanks must be null or empty for an empty bitmap.");
            }
        } else {
            if (blockRelativeRanks == null || blockRelativeRanks.getNumEntries() != expectedBlockRanksLength) {
                throw new IllegalArgumentException(
                    String.format(
                        "blockRelativeRanks has incorrect length. Expected %d (for %d superblocks with stride %d), got %s.",
                        expectedBlockRanksLength,
                        this.numSuperblocks,
                        this.blockRankStridePerSuperblock,
                        blockRelativeRanks == null ? "null" : blockRelativeRanks.getNumEntries()
                    )
                );
            }
        }
    }

    /**
     * Computes the number of set bits up to the beginning of the block containing `pos`.
     * @param pos
     * @return 
     */
    public long rank(long pos) {
        IO.println("rank("+pos+")");
        if (pos < 0) {
            throw new IllegalArgumentException("Position cannot be negative.");
        }
        if (numSuperblocks == 0) {
            return 0;
        }
        long totalBits = (long) numSuperblocks * superblockBitSize;
        if (pos >= totalBits) {
            return superblockAbsRanks.get(numSuperblocks);
        }
        long sbIdx = pos / superblockBitSize;
        long absRank = superblockAbsRanks.get(sbIdx);
        long localBlockIdx = (pos % superblockBitSize) / blockBitSize;
        long relRank = blockRelRanks.get(sbIdx * blockRankStridePerSuperblock + localBlockIdx);
        return absRank + relRank;
    }

    /**
     * Finds the starting bit-position of the block containing the j-th set bit (1-indexed).
     * @param j
     * @return 
     */
    public long select(long j) {
        if (j <= 0 || numSuperblocks == 0 || j > getTotalOnes()) {
            return -1;
        }
        int sb = findSuperblockIndex(j);
        if (sb < 0) {
            return -1;
        }
        long rankAtSB = superblockAbsRanks.get(sb);
        long target = j - rankAtSB;
        int blk = findBlockIndexInSuperblock(target, sb);
        if (blk < 0) {
            return -1;
        }
        long globalBlockIdx = (long) sb * blocksPerSuperblock + blk;
        return globalBlockIdx * blockBitSize;
    }

    private int findSuperblockIndex(long j) {
        int lo = 0, hi = numSuperblocks - 1;
        while (lo <= hi) {
            int mid = lo + (hi - lo) / 2;
            long before = superblockAbsRanks.get(mid);
            long after = superblockAbsRanks.get(mid + 1);
            if (before < j && j <= after) {
                return mid;
            } else if (j <= before) {
                hi = mid - 1;
            } else {
                lo = mid + 1;
            }
        }
        return -1;
    }

    private int findBlockIndexInSuperblock(long target, int sbIdx) {
        long base = (long) sbIdx * blockRankStridePerSuperblock;
        int lo = 0, hi = blocksPerSuperblock - 1;
        while (lo <= hi) {
            int mid = lo + (hi - lo) / 2;
            long before = blockRelRanks.get(base + mid);
            long after = blockRelRanks.get(base + mid + 1);
            if (before < target && target <= after) {
                return mid;
            } else if (target <= before) {
                hi = mid - 1;
            } else {
                lo = mid + 1;
            }
        }
        return -1;
    }

    /** Total number of bits in the bitmap (blocks-per-super * numSuperblocks).
     * @return  */
    public long getSizeInBits() {
        return (long) numSuperblocks * superblockBitSize;
    }

    /** Total number of 1-bits in the bitmap.
     * @return  */
    public long getTotalOnes() {
        return superblockAbsRanks.get(numSuperblocks);
    }

    /**
     * Returns the [start, end) range of bit-positions for the given 1-based ID.
     * Useful for mapping dictionary IDs to triple ranges.
     * @param id
     * @return 
     */
    public long[] selectRange(long id) {
        IO.println("selectRange="+id);
        long start = select(id);
        if (start < 0) return null;
        long end = select(id + 1);
        if (end < 0) end = getSizeInBits();
        return new long[]{start, end};
    }

    /** Number of zero-bits up to and including position pos.
     * @param pos
     * @return  */
    public long rank0(long pos) {
        if (pos < 0) throw new IllegalArgumentException("Position cannot be negative.");
        long upto = Math.min(pos, getSizeInBits() - 1);
        return (upto + 1) - rank(upto);
    }

    /** Position of the j-th zero-bit (1-indexed), or -1 if out of range.
     * @param j
     * @return  */
    public long select0(long j) {
        long totones = getTotalOnes();
        long totalZeros = getSizeInBits() - totones;
        if (j <= 0 || j > totalZeros) return -1;
        long lo = 0, hi = getSizeInBits() - 1;
        while (lo <= hi) {
            long mid = (lo + hi) >>> 1;
            long r0 = rank0(mid);
            if (r0 < j) lo = mid + 1;
            else if (r0 > j) hi = mid - 1;
            else return mid;
        }
        return -1;
    }

    /**
     * Iterate over the positions of all 1-bits in ascending order.
     * @return 
     */
    public Iterable<Long> ones() {
        return () -> new Iterator<>() {
            private long next = select(1);
            private long rank = 2;
            @Override public boolean hasNext()  { return next >= 0; }
            @Override public Long next() {
                long cur = next;
                next = select(rank++);
                return cur;
            }
        };
    }

/*
    public Pair getAssociatedIdsForN2(long n) {
        long[] span = selectRange(n);
        if (span == null) {
            return null;
        }
        long start = span[0];
        long end = span[1];  // exclusive
        long firstId = rank(start) + 1;
        long lastId = rank(end - 1);
        return new Pair(firstId, lastId);
    }    */
}
