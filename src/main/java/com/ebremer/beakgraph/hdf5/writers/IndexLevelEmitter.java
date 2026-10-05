package com.ebremer.beakgraph.hdf5.writers;

import static com.ebremer.beakgraph.Params.BLOCKSIZE;
import static com.ebremer.beakgraph.Params.SUPERBLOCKSIZE;
import static com.ebremer.beakgraph.utils.UTIL.byteRoundedWidth;
import com.ebremer.beakgraph.hdf5.DictionarySinks.LongSink;

/**
 * The index level-emission rule, once: given id tuples in the index's
 * component order (L0 first), emits the three S/B levels and their SB/BB
 * rank/select directories (SPECIFICATIONS.md §8). {@code BGIndex},
 * {@code ParallelBGIndex} and the disk pipeline's {@code HugeIndexWriter}
 * used to carry the same duplicate check, level-change logic, L0 padding and
 * directory bookkeeping three times with a "keep in lockstep" comment
 * (BG-299); they now differ only in how they sort and which sink backs the
 * buffers. {@code UltraBGIndex} remains the vectorised twin of this loop.
 * <p>
 * Semantics: a row equal to the previous one is dropped; a row whose L0 id
 * skips ids gets one dummy row (id 0, bit 1) per skipped id at every level,
 * so {@code select1(id)} addresses every L0 id's slot; {@link #finish} pads
 * the tail up to the L0 dictionary's maximum. The directory entries follow
 * {@code HDTBitmapDirectory}'s layout: SB[k] = ones before superblock k,
 * BB[k] = ones before block k within its superblock, both seeded with 0.
 */
public final class IndexLevelEmitter {

    private static final class LevelState {
        long bitsProcessed = 0;
        long onesSoFar = 0;
        long onesInCurrentSuperblock = 0;
        // Start at 0 (not -1) and pair with the seeded BB[0]=0, so the first
        // real block-boundary write lands at BB[1].
        long lastSuperblockWritten = 0;
        long lastBlockWritten = 0;
    }

    private final LongSink S1, B1, SB1, BB1;
    private final LongSink S2, B2, SB2, BB2;
    private final LongSink S3, B3, SB3, BB3;
    private final LevelState l1 = new LevelState(), l2 = new LevelState(), l3 = new LevelState();
    private boolean first = true;
    private long last0, last1, last2, last3;
    private long currentL0 = 1;

    /** Seeds every SB/BB directory with its leading 0 entry. */
    public IndexLevelEmitter(LongSink S1, LongSink B1, LongSink SB1, LongSink BB1,
                             LongSink S2, LongSink B2, LongSink SB2, LongSink BB2,
                             LongSink S3, LongSink B3, LongSink SB3, LongSink BB3) {
        this.S1 = S1; this.B1 = B1; this.SB1 = SB1; this.BB1 = BB1;
        this.S2 = S2; this.B2 = B2; this.SB2 = SB2; this.BB2 = BB2;
        this.S3 = S3; this.B3 = B3; this.SB3 = SB3; this.BB3 = BB3;
        SB1.writeLong(0); SB2.writeLong(0); SB3.writeLong(0);
        BB1.writeLong(0); BB2.writeLong(0); BB3.writeLong(0);
    }

    /** Width of the superblock counters: the bitmap length bounds them ({@code rows + maxL0Id} padding rows). */
    public static int superblockBits(long rows, long maxL0Id) {
        return byteRoundedWidth(rows + maxL0Id + 128L);
    }

    /** Width of the block counters (ones within one superblock). */
    public static int blockBits() {
        return byteRoundedWidth(SUPERBLOCKSIZE);
    }

    /** Dataset name suffix of a component: {@code 'G'} -> "g". */
    public static String levelName(char component) {
        return switch (component) {
            case 'G' -> "g";
            case 'S' -> "s";
            case 'P' -> "p";
            case 'O' -> "o";
            default -> throw new IllegalStateException("Unknown component: " + component);
        };
    }

    /** One row in the index's component order; rows MUST arrive sorted. */
    public void emit(long k0, long k1, long k2, long k3) {
        // 1. Duplicate check (id equality == term equality)
        if (!first && k0 == last0 && k1 == last1 && k2 == last2 && k3 == last3) {
            return;
        }
        boolean changeL0 = first || k0 != last0;
        boolean changeL1 = first || changeL0 || k1 != last1;
        boolean changeL2 = first || changeL1 || k2 != last2;

        // 2. Pad missing L0 ids with empty lists: each skipped id gets a dummy
        // row at every level so all buffers stay in lockstep (B_i.len ==
        // S_i.len). The dummy id is 0; real ids are >= 1, so searches inside
        // an empty L0 range always miss.
        if (changeL0) {
            long skipped = first ? (k0 - 1) : (k0 - currentL0 - 1);
            padEmptyL0(skipped);
            currentL0 = k0;
        }

        // 3. Level 3
        S3.writeLong(k3);
        int bit3 = changeL2 ? 1 : 0;
        B3.writeInteger(bit3);
        advanceLevel(l3, bit3, SB3, BB3);

        // 4. Level 2
        if (changeL2) {
            S2.writeLong(k2);
            int bit2 = changeL1 ? 1 : 0;
            B2.writeInteger(bit2);
            advanceLevel(l2, bit2, SB2, BB2);
        }

        // 5. Level 1
        if (changeL1) {
            S1.writeLong(k1);
            int bit1 = changeL0 ? 1 : 0;
            B1.writeInteger(bit1);
            advanceLevel(l1, bit1, SB1, BB1);
        }

        first = false;
        last0 = k0;
        last1 = k1;
        last2 = k2;
        last3 = k3;
    }

    /** Pads the remaining L0 ids up to {@code maxL0Id}; call once, after the last {@link #emit}. */
    public void finish(long maxL0Id) {
        padEmptyL0(maxL0Id - currentL0);
    }

    /**
     * Emits {@code count} full dummy rows across all three levels: one slot in
     * every S/B buffer with value 0 and bit 1, which preserves B_i.length ==
     * S_i.length while still advancing the select1 rank at L1.
     */
    private void padEmptyL0(long count) {
        for (long k = 0; k < count; k++) {
            S1.writeLong(0);
            B1.writeInteger(1);
            advanceLevel(l1, 1, SB1, BB1);

            S2.writeLong(0);
            B2.writeInteger(1);
            advanceLevel(l2, 1, SB2, BB2);

            S3.writeLong(0);
            B3.writeInteger(1);
            advanceLevel(l3, 1, SB3, BB3);
        }
    }

    private static void advanceLevel(LevelState state, int bitValue, LongSink SB, LongSink BB) {
        if (bitValue == 1) {
            state.onesSoFar++;
            state.onesInCurrentSuperblock++;
        }
        state.bitsProcessed++;

        long currentSuperblock = state.bitsProcessed / SUPERBLOCKSIZE;
        if (currentSuperblock != state.lastSuperblockWritten) {
            SB.writeLong(state.onesSoFar);
            state.lastSuperblockWritten = currentSuperblock;
            state.onesInCurrentSuperblock = 0;
        }

        long currentBlock = state.bitsProcessed / BLOCKSIZE;
        if (currentBlock != state.lastBlockWritten) {
            BB.writeLong(state.onesInCurrentSuperblock);
            state.lastBlockWritten = currentBlock;
        }
    }
}
