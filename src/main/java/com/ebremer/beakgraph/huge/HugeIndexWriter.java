package com.ebremer.beakgraph.huge;

import static com.ebremer.beakgraph.utils.UTIL.byteRoundedWidth;
import static com.ebremer.beakgraph.Params.BLOCKSIZE;
import static com.ebremer.beakgraph.Params.SUPERBLOCKSIZE;
import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.huge.HugeRecords.IdQuad;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming twin of {@link com.ebremer.beakgraph.hdf5.writers.BGIndex}: builds
 * one quad index (GSPO or GPOS) - S/B id+bitmap levels plus the SB/BB
 * rank/select directory - from an externally sorted stream of
 * dictionary-encoded quads. Because dictionary ids are rank-assigned in
 * {@code NodeComparator} order, numeric id order equals the term order BGIndex
 * sorts with, and this writer's output is identical to BGIndex's for the same
 * data. The level/padding/dedup logic mirrors BGIndex line for line; keep the
 * two in lockstep.
 *
 * @author Erich Bremer
 */
final class HugeIndexWriter implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(HugeIndexWriter.class);

    private final SpillBitPackedBuffer B1, B2, B3;
    private final SpillBitPackedBuffer S1, S2, S3;
    private final SpillBitPackedBuffer SB1, SB2, SB3;
    private final SpillBitPackedBuffer BB1, BB2, BB3;
    private final Index type;
    private final char[] positions;
    private final String[] names;
    private final long numGraphs, numSubjects, numPredicates, numObjects;

    private static final class LevelState {
        long bitsProcessed = 0;
        long onesSoFar = 0;
        long onesInCurrentSuperblock = 0;
        // Same seeding as BGIndex: start at 0, paired with the seeded BB[0]=0,
        // so BB[k] = ones-before-block-k within its superblock.
        long lastSuperblockWritten = 0;
        long lastBlockWritten = 0;
    }

    /**
     * @param totalRows total encoded quads INCLUDING duplicates - BGIndex sizes
     *                  the superblock width from the full pre-dedup quad array
     */
    HugeIndexWriter(Path workDir, Index type, long numGraphs, long numSubjects,
                    long numPredicates, long numObjects, long totalRows) throws IOException {
        logger.info("Creating index {} (disk-backed)", type);
        this.type = type;
        this.numGraphs = numGraphs;
        this.numSubjects = numSubjects;
        this.numPredicates = numPredicates;
        this.numObjects = numObjects;
        String indexName = type.name();
        this.positions = new char[]{indexName.charAt(0), indexName.charAt(1), indexName.charAt(2), indexName.charAt(3)};
        this.names = new String[4];
        for (int i = 0; i < 4; i++) {
            names[i] = String.valueOf(Character.toLowerCase(positions[i]));
        }

        Path dir = Files.createDirectories(workDir.resolve("index." + indexName));

        long maxCumulativeOnes = totalRows + maxL0Id() + 128L;
        int sbBits = byteRoundedWidth(maxCumulativeOnes);
        int bbBits = byteRoundedWidth(SUPERBLOCKSIZE);

        B1 = new SpillBitPackedBuffer(dir.resolve("B" + names[1]), 1);
        B2 = new SpillBitPackedBuffer(dir.resolve("B" + names[2]), 1);
        B3 = new SpillBitPackedBuffer(dir.resolve("B" + names[3]), 1);

        S1 = new SpillBitPackedBuffer(dir.resolve("S" + names[1]), bitSize(positions[1]));
        S2 = new SpillBitPackedBuffer(dir.resolve("S" + names[2]), bitSize(positions[2]));
        S3 = new SpillBitPackedBuffer(dir.resolve("S" + names[3]), bitSize(positions[3]));

        SB1 = new SpillBitPackedBuffer(dir.resolve("SB" + names[1]), sbBits);
        SB2 = new SpillBitPackedBuffer(dir.resolve("SB" + names[2]), sbBits);
        SB3 = new SpillBitPackedBuffer(dir.resolve("SB" + names[3]), sbBits);

        BB1 = new SpillBitPackedBuffer(dir.resolve("BB" + names[1]), bbBits);
        BB2 = new SpillBitPackedBuffer(dir.resolve("BB" + names[2]), bbBits);
        BB3 = new SpillBitPackedBuffer(dir.resolve("BB" + names[3]), bbBits);

        // Seed the "ones before the first superblock/block" directory entries to 0.
        SB1.writeLong(0); SB2.writeLong(0); SB3.writeLong(0);
        BB1.writeLong(0); BB2.writeLong(0); BB3.writeLong(0);
    }

    private long count(char component) {
        return switch (component) {
            case 'G' -> numGraphs;
            case 'S' -> numSubjects;
            case 'P' -> numPredicates;
            case 'O' -> numObjects;
            default -> throw new IllegalStateException("Unknown component: " + component);
        };
    }

    /** Same width rule as BGIndex: UTIL.byteRoundedWidth(count + 1). */
    private int bitSize(char component) {
        return byteRoundedWidth(count(component) + 1);
    }

    private long maxL0Id() {
        return count(positions[0]);
    }

    private static long component(IdQuad q, char c) {
        return switch (c) {
            case 'G' -> q.g();
            case 'S' -> q.s();
            case 'P' -> q.p();
            case 'O' -> q.o();
            default -> throw new IllegalStateException();
        };
    }

    /** Consumes the id-quads, which MUST be sorted in this index's order. */
    void build(Iterator<IdQuad> sorted) throws IOException {
        LevelState l1 = new LevelState(), l2 = new LevelState(), l3 = new LevelState();
        IdQuad lastUnique = null;
        long count = 0;
        long maxL0Id = maxL0Id();
        long currentL0 = 1;

        while (sorted.hasNext()) {
            IdQuad curr = sorted.next();
            if (++count % 1_000_000 == 0) {
                logger.info("{} processed {} quads...", type.name(), count);
            }
            // 1. Duplicate check (numeric id equality == term equality)
            if (lastUnique != null
                    && component(lastUnique, positions[0]) == component(curr, positions[0])
                    && component(lastUnique, positions[1]) == component(curr, positions[1])
                    && component(lastUnique, positions[2]) == component(curr, positions[2])
                    && component(lastUnique, positions[3]) == component(curr, positions[3])) {
                continue;
            }

            boolean changeL0 = (lastUnique == null) || component(lastUnique, positions[0]) != component(curr, positions[0]);
            boolean changeL1 = (lastUnique == null) || changeL0 || component(lastUnique, positions[1]) != component(curr, positions[1]);
            boolean changeL2 = (lastUnique == null) || changeL1 || component(lastUnique, positions[2]) != component(curr, positions[2]);

            long thisL0 = component(curr, positions[0]);

            // 2. Pad missing L0 ids with empty lists (see BGIndex)
            if (changeL0) {
                long skipped = (lastUnique == null) ? (thisL0 - 1) : (thisL0 - currentL0 - 1);
                padEmptyL0(skipped, l1, l2, l3);
                currentL0 = thisL0;
            }

            // 3. Level 3
            S3.writeLong(component(curr, positions[3]));
            int bit3 = changeL2 ? 1 : 0;
            B3.writeInteger(bit3);
            advanceLevel(l3, bit3, SB3, BB3);

            // 4. Level 2
            if (changeL2) {
                S2.writeLong(component(curr, positions[2]));
                int bit2 = changeL1 ? 1 : 0;
                B2.writeInteger(bit2);
                advanceLevel(l2, bit2, SB2, BB2);
            }

            // 5. Level 1
            if (changeL1) {
                S1.writeLong(component(curr, positions[1]));
                int bit1 = changeL0 ? 1 : 0;
                B1.writeInteger(bit1);
                advanceLevel(l1, bit1, SB1, BB1);
            }

            lastUnique = curr;
        }

        // Pad remaining ids up to the dictionary's maximum
        long skipped = maxL0Id - currentL0;
        padEmptyL0(skipped, l1, l2, l3);

        completeAll();
    }

    private void padEmptyL0(long count, LevelState l1, LevelState l2, LevelState l3) {
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

    private void advanceLevel(LevelState state, int bitValue, SpillBitPackedBuffer SB, SpillBitPackedBuffer BB) {
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

    private void completeAll() throws IOException {
        B1.complete(); B2.complete(); B3.complete();
        S1.complete(); S2.complete(); S3.complete();
        SB1.complete(); SB2.complete(); SB3.complete();
        BB1.complete(); BB2.complete(); BB3.complete();
    }

    /** Writes the index group, mirroring BGIndex.add() (names and order). */
    void transferTo(StreamingHdf5Group hdt) throws IOException {
        StreamingHdf5Group index = hdt.putGroup(type.name());
        S1.transferTo(index, "S" + names[1]);
        S2.transferTo(index, "S" + names[2]);
        S3.transferTo(index, "S" + names[3]);
        B1.transferTo(index, "B" + names[1]);
        B2.transferTo(index, "B" + names[2]);
        B3.transferTo(index, "B" + names[3]);
        SB1.transferTo(index, "SB" + names[1]);
        SB2.transferTo(index, "SB" + names[2]);
        SB3.transferTo(index, "SB" + names[3]);
        BB1.transferTo(index, "BB" + names[1]);
        BB2.transferTo(index, "BB" + names[2]);
        BB3.transferTo(index, "BB" + names[3]);
    }

    @Override
    public void close() throws IOException {
        B1.close(); B2.close(); B3.close();
        S1.close(); S2.close(); S3.close();
        SB1.close(); SB2.close(); SB3.close();
        BB1.close(); BB2.close(); BB3.close();
    }
}
