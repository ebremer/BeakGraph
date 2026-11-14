package com.ebremer.beakgraph.utils;

import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import static com.ebremer.beakgraph.Params.BLOCKSIZE;
import static com.ebremer.beakgraph.Params.SUPERBLOCKSIZE;

/**
 * Rank/select directory over a bitmap: the superblock and block counters that
 * accelerate {@link #select1} (SPECIFICATIONS.md §8). The id sequence the
 * bitmap delimits is the caller's (IndexReader keeps it beside the directory;
 * the former {@code getIds()} pass-through had no reader, BG-310).
 *
 * @author Erich Bremer
 */
public class HDTBitmapDirectory {

    private final BitPackedUnSignedLongBuffer superblock;   // SBx
    private final BitPackedUnSignedLongBuffer block;        // BBx
    private final BitPackedUnSignedLongBuffer bitmap;       // Bx

    private final long superblockSize;
    private final long blockSize;
    private final long blocksPerSuperblock;

    private final long numBitmapEntries;
    private final long numSuperblockEntries;
    private final long numBlockEntries;
    /** Superblock counters kept on the heap up to this many entries (128 MiB of them). */
    static final long HEAP_SUPERBLOCKS_MAX = 1L << 24;
    private volatile long[] superblockHeap;

    public HDTBitmapDirectory(BitPackedUnSignedLongBuffer superblock,
                              BitPackedUnSignedLongBuffer block,
                              BitPackedUnSignedLongBuffer bitmap) {
        this(superblock, block, bitmap, SUPERBLOCKSIZE, BLOCKSIZE);
    }

    public HDTBitmapDirectory(BitPackedUnSignedLongBuffer superblock,
                              BitPackedUnSignedLongBuffer block,
                              BitPackedUnSignedLongBuffer bitmap,
                              long superblockSize,
                              long blockSize) {
        this.superblock = superblock;
        this.block = block;
        this.bitmap = bitmap;
        this.superblockSize = superblockSize;
        this.blockSize = blockSize;
        this.blocksPerSuperblock = superblockSize / blockSize;
        this.numBitmapEntries = bitmap.getNumEntries();
        this.numSuperblockEntries = superblock.getNumEntries();
        this.numBlockEntries = block.getNumEntries();
    }

    /**
     * The superblock counters, on the heap from the first select on: one cumulative
     * count per 512 bits of bitmap, so a store of a billion rows a level holds 16 MiB
     * of them. Finding a superblock then never touches the file - it was the part of
     * a select that probed all over a dataset - and what is left to read lies
     * together: the block counters of one superblock, and a word of the bitmap.
     * Beyond {@link #HEAP_SUPERBLOCKS_MAX} entries they are read where they lie, as
     * before. Loaded lazily, so opening a store reads none of it; two threads racing
     * here load the same content twice.
     */
    private long[] superblocksOnHeap() {
        long[] heap = superblockHeap;
        if (heap == null && numSuperblockEntries > 0 && numSuperblockEntries <= HEAP_SUPERBLOCKS_MAX) {
            heap = new long[(int) numSuperblockEntries];
            superblock.get(0, heap, 0, heap.length);
            superblockHeap = heap;
        }
        return heap;
    }

    private long superblockAt(long[] heap, long index) {
        return (heap != null) ? heap[(int) index] : superblock.get(index);
    }

    public long select1(long rank) {
        if (rank <= 0) return -1L;
        long[] heap = superblocksOnHeap();

        // Step 1: Find Superblock - the greatest index whose cumulative count is
        // still below the target rank. The cumulative counts of real index bitmaps
        // grow near-linearly, so ONE interpolated probe plus an exponential gallop
        // usually brackets the answer within a handful of reads (a plain binary
        // search costs ~22 probes on a PubMed-scale directory of ~5.6M
        // superblocks). The closing binary search keeps the O(log n) worst case
        // for skewed bitmaps (e.g. dense padding runs next to sparse data runs).
        // Stateless on purpose: directories are shared across concurrent queries.
        long low = 0;
        long high = numSuperblockEntries - 1;
        long sbIdx = 0; // Default to 0 if not found or first

        if (high > 8) {
            long last = superblockAt(heap, high);
            long guess = (last > 0) ? (long) ((double) rank / last * high) : 0;
            if (guess < 0) guess = 0;
            if (guess > high) guess = high;
            long step = 1;
            long cur = guess;
            if (superblockAt(heap, guess) < rank) {
                // Answer is at or to the right of the guess: gallop right.
                sbIdx = guess;
                while (cur + step <= high && superblockAt(heap, cur + step) < rank) {
                    cur += step;
                    sbIdx = cur;
                    step <<= 1;
                }
                low = cur + 1;
                high = Math.min(high, cur + step);
            } else {
                // Answer is strictly left of the guess: gallop left. Index 0 always
                // qualifies (the seeded count is 0 < rank), so the bracket is never empty.
                while (cur - step >= 0 && superblockAt(heap, cur - step) >= rank) {
                    cur -= step;
                    step <<= 1;
                }
                low = Math.max(0, cur - step);
                high = cur - 1;
            }
        }

        while (low <= high) {
            long mid = (low + high) >>> 1;
            long val = superblockAt(heap, mid);
            if (val < rank) {
                sbIdx = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        long baseRank = superblockAt(heap, sbIdx);
        // If the SB rank is strictly greater than target (shouldn't happen if 0 is 0), 
        // it means we need to look at previous SB.
        if (baseRank >= rank && sbIdx > 0) {
            sbIdx--;
            baseRank = superblockAt(heap, sbIdx);
        }
        
        long remaining = rank - baseRank;

        // Step 2: Find Block
        long blockStartIdx = sbIdx * blocksPerSuperblock;
        long blockEndIdx = Math.min(blockStartIdx + blocksPerSuperblock, numBlockEntries);

        low = blockStartIdx;
        high = blockEndIdx - 1;
        long bIdx = blockStartIdx; // Default to start of SB

        while (low <= high) {
            long mid = (low + high) >>> 1;
            long relRank = block.get(mid);
            if (baseRank + relRank < rank) {
                bIdx = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        
        // Ensure bIdx is within valid range for this SB
        if (bIdx < blockStartIdx) bIdx = blockStartIdx;
        
        long rankAtBlockStart = block.get(bIdx);
        long stillNeeded = remaining - rankAtBlockStart;

        // Step 3: Linear Scan
        long pos = bIdx * blockSize;
        
        // HARDENED: Scan until we find it or hit absolute end of bitmap
        while (pos < numBitmapEntries) {
            // Optimization: Process 64-bit words
            if (pos + 64 <= numBitmapEntries) {
                long word = bitmap.getWord64(pos);
                int count = Long.bitCount(word);
                if (stillNeeded <= count) {
                    return pos + findNthSetBitInWord(word, stillNeeded);
                }
                stillNeeded -= count;
                pos += 64;
            } else {
                // Bit-by-bit for the tail
                if (bitmap.get(pos) == 1) {
                    stillNeeded--;
                    if (stillNeeded <= 0) return pos;
                }
                pos++;
            }
        }

        return -1L;
    }

    private long findNthSetBitInWord(long word, long n) {
        // Delegates to the shared broadword implementation so the accelerated
        // select1 and the buffer's linear select1 can never disagree.
        return UTIL.selectInWord(word, n);
    }
}