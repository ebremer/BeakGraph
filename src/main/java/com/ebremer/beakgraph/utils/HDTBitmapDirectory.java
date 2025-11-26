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
        if (n == 1) return Long.numberOfLeadingZeros(word);
        for (int i = 0; i < 64; i++) {
            // MSB first check
            if ((word & (1L << (63 - i))) != 0) {
                n--;
                if (n == 0) return i;
            }
        }
        return -1;
    }

    public BitPackedUnSignedLongBuffer getIds() { return ids; }

    public long getId(long rank) {
        if (rank <= 0 || rank > numIdEntries) {
            throw new IndexOutOfBoundsException("Rank " + rank + " out of bounds [1.." + numIdEntries + "]");
        }
        return ids.get(rank - 1);
    }

    public long getBitCount() { return rank1(numBitmapEntries); }
    
    // ... dumpDirectories and printBitmapPrefix unchanged ...
    public void dumpDirectories() {
        System.out.println("=== HDTBitmapDirectory Dump ===");
        System.out.printf("Bitmap entries      : %,d%n", numBitmapEntries);
        System.out.printf("Superblock size     : %,d bits%n", superblockSize);
        System.out.printf("Block size          : %,d bits%n", blockSize);
        System.out.printf("Superblocks         : %,d%n", numSuperblockEntries);
        System.out.printf("Blocks              : %,d%n", numBlockEntries);
        System.out.printf("Total 1s            : %,d%n%n", getBitCount());

        System.out.println("Superblock Directory (SB – absolute rank at superblock boundary):");
        System.out.println("Idx    StartBit         EndBit           AbsRank");
        System.out.println("----------------------------------------------------------");
        for (long i = 0; i < numSuperblockEntries; i++) {
            long start = i * superblockSize;
            long end = Math.min((i + 1) * superblockSize, numBitmapEntries);
            long rank = superblock.get(i);
            System.out.printf("%3d    %,12d   -> %,12d     %,10d%n", i, start, end - 1, rank);
        }
        System.out.println();

        System.out.println("Block Directory (BB – relative rank within superblock):");
        System.out.println("BlkIdx  Superblock  StartBit         EndBit       RelRank AbsRank");
        System.out.println("---------------------------------------------------------------------");
        for (long i = 0; i < numBlockEntries; i++) {
            long sbIdx = i / blocksPerSuperblock;
            long absRankAtSB = (sbIdx < numSuperblockEntries) ? superblock.get(sbIdx) : superblock.get(numSuperblockEntries - 1);
            long relRank = block.get(i);
            long absRank = absRankAtSB + relRank;

            long start = i * blockSize;
            long end = Math.min((i + 1) * blockSize, numBitmapEntries);

            System.out.printf("%6d  %9d    %,12d   -> %,12d     %,8d %,10d%n",
                    i, sbIdx, start, end - 1, relRank, absRank);
        }
        System.out.println("=== End Dump ===\n");
    }

    public void printBitmapPrefix(long n) {
        long max = Math.min(n, numBitmapEntries);
        System.out.print("Bitmap prefix [" + max + "]: ");
        for (long i = 0; i < max; i++) {
            System.out.print(bitmap.get(i));
        }
        System.out.println();
    }
}