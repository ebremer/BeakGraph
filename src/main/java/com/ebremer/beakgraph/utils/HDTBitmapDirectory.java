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
    private final long numIdEntries;

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
        this.numIdEntries = ids.getNumEntries();
    }

    public long getNumBitmapEntries() { return numBitmapEntries; }

    public long rank1(long pos) {
        if (pos <= 0) return 0L;
        if (pos > numBitmapEntries) pos = numBitmapEntries;

        long targetBit = pos - 1;
        long sbIdx = targetBit / superblockSize;
        long bIdx  = targetBit / blockSize;

        long rank = (sbIdx < numSuperblockEntries) ? superblock.get(sbIdx) : superblock.get(numSuperblockEntries - 1);

        if (bIdx < numBlockEntries) {
            rank += block.get(bIdx);
        }

        long currentPos = bIdx * blockSize;
        while (currentPos + 64 <= pos) {
            long word = bitmap.getWord64(currentPos);
            rank += Long.bitCount(word);
            currentPos += 64;
        }
        while (currentPos < pos) {
            if (bitmap.get(currentPos) == 1) rank++;
            currentPos++;
        }
        return rank;
    }

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
        // Broadword selection: 0-based index (from MSB) of the n-th set bit (n >= 1),
        // O(1) via 6 popcount narrowing steps. Replaces an O(64) bit-by-bit loop; the
        // result is identical to the linear select1's selectInWordSafe.
        long result = 0;
        int cnt;
        cnt = Long.bitCount(word >>> 32); if (n > cnt) { word <<= 32; result += 32; n -= cnt; }
        cnt = Long.bitCount(word >>> 48); if (n > cnt) { word <<= 16; result += 16; n -= cnt; }
        cnt = Long.bitCount(word >>> 56); if (n > cnt) { word <<= 8;  result += 8;  n -= cnt; }
        cnt = Long.bitCount(word >>> 60); if (n > cnt) { word <<= 4;  result += 4;  n -= cnt; }
        cnt = Long.bitCount(word >>> 62); if (n > cnt) { word <<= 2;  result += 2;  n -= cnt; }
        cnt = Long.bitCount(word >>> 63); if (n > cnt) { result += 1; }
        return result;
    }

    public BitPackedUnSignedLongBuffer getIds() { return ids; }

    public long getId(long rank) {
        if (rank <= 0 || rank > numIdEntries) {
            throw new IndexOutOfBoundsException("Rank " + rank + " out of bounds [1.." + numIdEntries + "]");
        }
        return ids.get(rank - 1);
    }

    public long getBitCount() { return rank1(numBitmapEntries); }
}