package com.ebremer.beakgraph.utils;

import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import static com.ebremer.beakgraph.Params.BLOCKSIZE;
import static com.ebremer.beakgraph.Params.SUPERBLOCKSIZE;

/**
 * Accelerated rank/select operations on a bitmap.
 * * HARDENED: Select1 linear scan now robustly handles end-of-buffer edge cases.
 * * @author Erich Bremer
 */
public class HDTBitmapDirectory {

    private final BitPackedUnSignedLongBuffer superblock;   // SBx
    private final BitPackedUnSignedLongBuffer block;        // BBx
    private final BitPackedUnSignedLongBuffer bitmap;       // Bx
    private final BitPackedUnSignedLongBuffer ids;          // Sx

    private final long superblockSize;
    private final long blockSize;
    private final long blocksPerSuperblock;

    private final long numBitmapEntries;
    private final long numSuperblockEntries;
    private final long numBlockEntries;

    public HDTBitmapDirectory(BitPackedUnSignedLongBuffer superblock,
                              BitPackedUnSignedLongBuffer block,
                              BitPackedUnSignedLongBuffer bitmap,
                              BitPackedUnSignedLongBuffer ids) {
        this(superblock, block, bitmap, ids, SUPERBLOCKSIZE, BLOCKSIZE);
    }

    public HDTBitmapDirectory(BitPackedUnSignedLongBuffer superblock,
                              BitPackedUnSignedLongBuffer block,
                              BitPackedUnSignedLongBuffer bitmap,
                              BitPackedUnSignedLongBuffer ids,
                              long superblockSize,
                              long blockSize) {
        this.superblock = superblock;
        this.block = block;
        this.bitmap = bitmap;
        this.ids = ids;
        this.superblockSize = superblockSize;
        this.blockSize = blockSize;
        this.blocksPerSuperblock = superblockSize / blockSize;
        this.numBitmapEntries = bitmap.getNumEntries();
        this.numSuperblockEntries = superblock.getNumEntries();
        this.numBlockEntries = block.getNumEntries();
    }

    public long getNumBitmapEntries() { return numBitmapEntries; }

    public long select1(long rank) {
        if (rank <= 0) return -1L;

        // Step 1: Find Superblock
        long low = 0;
        long high = numSuperblockEntries - 1;
        long sbIdx = 0; // Default to 0 if not found or first

        while (low <= high) {
            long mid = (low + high) >>> 1;
            long val = superblock.get(mid);
            if (val < rank) {
                sbIdx = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        long baseRank = superblock.get(sbIdx);
        // If the SB rank is strictly greater than target (shouldn't happen if 0 is 0), 
        // it means we need to look at previous SB.
        if (baseRank >= rank && sbIdx > 0) {
            sbIdx--;
            baseRank = superblock.get(sbIdx);
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

    public BitPackedUnSignedLongBuffer getIds() { return ids; }
}