package com.ebremer.beakgraph.hdf5.writers;

import static com.ebremer.beakgraph.Params.BLOCKSIZE;
import static com.ebremer.beakgraph.Params.SUPERBLOCKSIZE;
import static com.ebremer.beakgraph.utils.UTIL.byteRoundedWidth;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.Index;
import io.jhdf.api.WritableGroup;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import org.apache.jena.sparql.core.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GSPO / GPOS index builder of the default (method 0) writer.
 * <p>
 * Quads are indexed by DICTIONARY ID, not by node: {@link #resolveIds} looks
 * each quad's four ids up exactly once (four binary searches per quad), and
 * both orderings sort and scan those tuples with long compares and no further
 * lookups. The earlier version sorted the Quad array with NodeComparator - a
 * value comparison per node per comparison, through Jena's global NodeValue
 * cache - and then binary-searched the dictionaries again at every level of
 * every quad: eight lookups per quad per index (BG-241).
 * <p>
 * Sorting by id is exactly the order comparing nodes gives: an id is the
 * node's 1-based rank under NodeComparator within its section, and the object
 * id space keeps entities (1..maxEntityId) below literals, matching the
 * comparator's BNode &lt; URI &lt; Literal &lt; TripleTerm macro-order.
 * Distinct terms have distinct ids - MultiTypeDictionaryWriter fails the build
 * when the comparator answers 0 for two of them (BG-104) - so id equality is
 * node equality and the duplicate and level-change checks below are the
 * former node checks verbatim: the emitted buffers are byte-identical to the
 * node-sorted ones, which ParallelWriterParityTest and UltraWriterParityTest
 * pin against the other in-memory engines.
 */
public class BGIndex {
    private static final Logger logger = LoggerFactory.getLogger(BGIndex.class);

    private final BitPackedUnSignedLongBuffer B1, B2, B3;
    private final BitPackedUnSignedLongBuffer S1, S2, S3;
    private final BitPackedUnSignedLongBuffer SB1, SB2, SB3;
    private final BitPackedUnSignedLongBuffer BB1, BB2, BB3;
    private final Index type;
    /** This ordering's component per level, e.g. GPOS -> ['G','P','O','S']. */
    private final char[] comps;

    /** One quad's four dictionary ids; immutable, so both index builds share one resolution. */
    public record QuadIds(long g, long s, long p, long o) {}

    /**
     * Resolves every quad's ids once, sequentially. Hand the array to one
     * index build and a {@code clone()} to the other: each sorts its array in
     * place, so clone before either build starts.
     */
    public static QuadIds[] resolveIds(PositionalDictionaryWriter w, Quad[] quads) {
        logger.info("Resolving dictionary ids for {} quads...", quads.length);
        long start = System.nanoTime();
        QuadIds[] ids = new QuadIds[quads.length];
        for (int i = 0; i < quads.length; i++) {
            Quad q = quads[i];
            ids[i] = new QuadIds(
                    w.locateGraph(q.getGraph()),
                    w.locateSubject(q.getSubject()),
                    w.locatePredicate(q.getPredicate()),
                    w.locateObject(q.getObject()));
        }
        logger.info("Resolved ids for {} quads in {} s", quads.length, (System.nanoTime() - start) / 1_000_000_000L);
        return ids;
    }

    private static class LevelState {
        long bitsProcessed = 0;
        long onesSoFar = 0;
        long onesInCurrentSuperblock = 0;
        long onesInCurrentBlock = 0;
        long lastSuperblockWritten = 0;
        // Start at 0 (not -1) and pair with a seeded BB[0]=0 below, so the first real
        // block-boundary write lands at BB[1]. This keeps BB[k] = ones-before-block-k
        // (within its superblock) - the layout HDTBitmapDirectory.select1/rank1 assume.
        long lastBlockWritten = 0;
    }

    public BGIndex(PositionalDictionaryWriter dictWriter, Index type, QuadIds[] tuples) {
        logger.info("Creating index {}", type);
        this.type = type;
        this.comps = type.name().toCharArray();

        // Superblock entries store the cumulative count of set bits; the largest such
        // value is the bitmap length. Every level writes at most one row per unique quad
        // plus one padding row per L0 id, so (tuples.length + maxL0Id) bounds it. Sizing
        // from getNumberOfQuads() alone overflowed once the entity space - or the VOID /
        // spatial quads that numQuads does not count - exceeded the quad count, silently
        // corrupting the rank/select directory now used for query navigation.
        long maxCumulativeOnes = (long) tuples.length + computeMaxL0Id(dictWriter) + 128L;
        int sbBits = byteRoundedWidth(maxCumulativeOnes);
        int bbBits = byteRoundedWidth(SUPERBLOCKSIZE);

        String n1 = levelName(comps[1]);
        String n2 = levelName(comps[2]);
        String n3 = levelName(comps[3]);

        B1 = new BitPackedUnSignedLongBuffer(Path.of("B" + n1), null, 0, 1);
        B2 = new BitPackedUnSignedLongBuffer(Path.of("B" + n2), null, 0, 1);
        B3 = new BitPackedUnSignedLongBuffer(Path.of("B" + n3), null, 0, 1);

        S1 = new BitPackedUnSignedLongBuffer(Path.of("S" + n1), null, 0, getBitSize(dictWriter, comps[1]));
        S2 = new BitPackedUnSignedLongBuffer(Path.of("S" + n2), null, 0, getBitSize(dictWriter, comps[2]));
        S3 = new BitPackedUnSignedLongBuffer(Path.of("S" + n3), null, 0, getBitSize(dictWriter, comps[3]));

        SB1 = new BitPackedUnSignedLongBuffer(Path.of("SB" + n1), null, 0, sbBits);
        SB2 = new BitPackedUnSignedLongBuffer(Path.of("SB" + n2), null, 0, sbBits);
        SB3 = new BitPackedUnSignedLongBuffer(Path.of("SB" + n3), null, 0, sbBits);

        BB1 = new BitPackedUnSignedLongBuffer(Path.of("BB" + n1), null, 0, bbBits);
        BB2 = new BitPackedUnSignedLongBuffer(Path.of("BB" + n2), null, 0, bbBits);
        BB3 = new BitPackedUnSignedLongBuffer(Path.of("BB" + n3), null, 0, bbBits);

        // Seed the "ones before the first superblock/block" directory entries to 0.
        SB1.writeLong(0); SB2.writeLong(0); SB3.writeLong(0);
        BB1.writeLong(0); BB2.writeLong(0); BB3.writeLong(0);

        processTuples(dictWriter, tuples);
        prepareForReading();
    }

    private static String levelName(char component) {
        return switch (component) {
            case 'G' -> "g";
            case 'S' -> "s";
            case 'P' -> "p";
            case 'O' -> "o";
            default -> throw new IllegalStateException("Unknown component: " + component);
        };
    }

    private static long id(QuadIds t, char component) {
        return switch (component) {
            case 'G' -> t.g();
            case 'S' -> t.s();
            case 'P' -> t.p();
            case 'O' -> t.o();
            default -> throw new IllegalStateException("Unknown component: " + component);
        };
    }

    private static long count(PositionalDictionaryWriter w, char component) {
        return switch (component) {
            case 'G' -> w.getNumberOfGraphs();
            case 'S' -> w.getNumberOfSubjects();
            case 'P' -> w.getNumberOfPredicates();
            case 'O' -> w.getNumberOfObjects();
            default -> throw new IllegalStateException("Unknown component: " + component);
        };
    }

    private static int getBitSize(PositionalDictionaryWriter w, char component) {
        return byteRoundedWidth(count(w, component) + 1);
    }

    /**
     * Maximum id of this index's first (L0) component. The L0 dimension is padded up to
     * this value so that select1(id) addresses each L0 id's slot directly; it therefore
     * also bounds the cumulative set-bit count stored in the superblock directory.
     */
    private long computeMaxL0Id(PositionalDictionaryWriter w) {
        return count(w, comps[0]);
    }

    private Comparator<QuadIds> tupleOrder() {
        final char c0 = comps[0], c1 = comps[1], c2 = comps[2], c3 = comps[3];
        return (a, b) -> {
            int r = Long.compare(id(a, c0), id(b, c0));
            if (r != 0) return r;
            r = Long.compare(id(a, c1), id(b, c1));
            if (r != 0) return r;
            r = Long.compare(id(a, c2), id(b, c2));
            if (r != 0) return r;
            return Long.compare(id(a, c3), id(b, c3));
        };
    }

    private void processTuples(PositionalDictionaryWriter w, QuadIds[] tuples) {
        // INFO bracketing: sorting millions of quads takes minutes with no other output.
        logger.info("Sorting {} quads for {}...", tuples.length, type.name());
        long sortStart = System.nanoTime();
        Arrays.parallelSort(tuples, tupleOrder());
        logger.info("Sorted {} in {} s", type.name(), (System.nanoTime() - sortStart) / 1_000_000_000L);

        LevelState l1 = new LevelState(), l2 = new LevelState(), l3 = new LevelState();
        boolean first = true;
        long last0 = 0, last1 = 0, last2 = 0, last3 = 0;
        long count = 0;
        long totalQuads = tuples.length;

        // Establish the Maximum ID for Level 0 so we know how far to pad at the end
        long maxL0Id = computeMaxL0Id(w);

        long currentL0 = 1;

        for (QuadIds t : tuples) {
            if (++count % 1_000_000 == 0) {
                logger.info("{} processed {} / {} quads...", type.name(), count, totalQuads);
            }
            long k0 = id(t, comps[0]);
            long k1 = id(t, comps[1]);
            long k2 = id(t, comps[2]);
            long k3 = id(t, comps[3]);

            // 1. Duplicate Check
            if (!first && k0 == last0 && k1 == last1 && k2 == last2 && k3 == last3) {
                continue;
            }

            boolean changeL0 = first || k0 != last0;
            boolean changeL1 = first || changeL0 || k1 != last1;
            boolean changeL2 = first || changeL1 || k2 != last2;

            long thisL0 = k0;

            // Pad Missing L0 IDs with Empty Lists ---
            // Each skipped L0 ID gets a dummy row at every level so all buffers stay
            // in lockstep (B1.len == S1.len, B2.len == S2.len, B3.len == S3.len).
            // Dummy ID value is 0; since real dictionary IDs are >=1, searches for real
            // predicates/objects inside an empty graph's range always miss.
            if (changeL0) {
                long skipped = first ? (thisL0 - 1) : (thisL0 - currentL0 - 1);
                padEmptyL0(skipped, l1, l2, l3);
                currentL0 = thisL0;
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

        // Pad remaining IDs up to the Dictionary's maximum limit ---
        long skipped = maxL0Id - currentL0;
        padEmptyL0(skipped, l1, l2, l3);

        flushAllBuffers();
    }

    /**
     * Emit `count` full dummy rows across all three levels. Each dummy row occupies
     * one slot in every S/B buffer with value 0 and bit 1, which preserves the
     * invariant B_i.length == S_i.length while still advancing the select1 rank at
     * L1 (so `Bp.select1(gi)` correctly identifies empty graph gi's slot).
     */
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

    private void advanceLevel(LevelState state, int bitValue, BitPackedUnSignedLongBuffer SB, BitPackedUnSignedLongBuffer BB) {
        if (bitValue == 1) {
            state.onesSoFar++;
            state.onesInCurrentSuperblock++;
            state.onesInCurrentBlock++;
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
            state.onesInCurrentBlock = 0;
        }
    }

    private void flushAllBuffers() {
        B1.complete(); B2.complete(); B3.complete();
        S1.complete(); S2.complete(); S3.complete();
        SB1.complete(); SB2.complete(); SB3.complete();
        BB1.complete(); BB2.complete(); BB3.complete();
    }

    private void prepareForReading() {
        B1.prepareForReading(); B2.prepareForReading(); B3.prepareForReading();
        S1.prepareForReading(); S2.prepareForReading(); S3.prepareForReading();
        SB1.prepareForReading(); SB2.prepareForReading(); SB3.prepareForReading();
        BB1.prepareForReading(); BB2.prepareForReading(); BB3.prepareForReading();
    }

    public void add(WritableGroup hdt) {
        WritableGroup index = hdt.putGroup(type.name());
        S1.add(index); S2.add(index); S3.add(index);
        B1.add(index); B2.add(index); B3.add(index);
        SB1.add(index); SB2.add(index); SB3.add(index);
        BB1.add(index); BB2.add(index); BB3.add(index);
    }
}
