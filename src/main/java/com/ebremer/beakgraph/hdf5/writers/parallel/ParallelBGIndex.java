package com.ebremer.beakgraph.hdf5.writers.parallel;

import com.ebremer.beakgraph.core.Futures;
import com.ebremer.beakgraph.hdf5.writers.IndexLevelEmitter;
import static com.ebremer.beakgraph.utils.UTIL.byteRoundedWidth;
import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.Index;
import io.jhdf.api.WritableGroup;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;
import org.apache.jena.sparql.core.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Multi-threaded twin of {@link com.ebremer.beakgraph.hdf5.writers.BGIndex}.
 *
 * <p>The sequential index sorts the quad array by comparing NODES (an expensive
 * NodeComparator call per comparison) and then binary-searches the dictionaries
 * again for every level of every quad while scanning. Here the four dictionary
 * ids of each quad are resolved exactly once, in parallel, by
 * {@link #resolveIds}; the same tuples serve both orderings (GSPO reads them as
 * (g,s,p,o), GPOS as (g,p,o,s)), so two instances can be built concurrently on
 * clones of one tuple array. Sorting compares longs and the scan loop does no
 * lookups at all.
 *
 * <p>Sorting by id is exactly the order the sequential writer obtains by
 * comparing nodes: each id is the node's 1-based rank under NodeComparator
 * within its dictionary, and the object id space keeps entities
 * (1..maxEntityId) below literals (maxEntityId+1..), matching NodeComparator's
 * BNode &lt; URI &lt; Literal macro-order. Distinct terms always receive
 * distinct ids, so id equality is node equality and the duplicate/level-change
 * checks below are the sequential writer's checks verbatim - the emitted
 * buffers are identical.
 */
public class ParallelBGIndex {
    private static final Logger logger = LoggerFactory.getLogger(ParallelBGIndex.class);

    private final BitPackedUnSignedLongBuffer B1, B2, B3;
    private final BitPackedUnSignedLongBuffer S1, S2, S3;
    private final BitPackedUnSignedLongBuffer SB1, SB2, SB3;
    private final BitPackedUnSignedLongBuffer BB1, BB2, BB3;
    private final Index type;
    /** This ordering's component per level, e.g. GPOS -> ['G','P','O','S']. */
    private final char[] comps;

    /** One quad's four dictionary ids, immutable so both index builds can share it. */
    static final class QuadIds {
        final long g, s, p, o;

        QuadIds(long g, long s, long p, long o) {
            this.g = g;
            this.s = s;
            this.p = p;
            this.o = o;
        }
    }

    /**
     * Resolves the dictionary ids of every quad with lookups fanned out across
     * {@code pool}. Called once per write; the returned array is handed to the
     * GSPO build and a {@code clone()} of it to the GPOS build (each sorts its
     * array in place - cloning must happen before either build starts).
     */
    static QuadIds[] resolveIds(ParallelPositionalDictionaryWriter w, Quad[] quads, ForkJoinPool pool) throws IOException {
        logger.info("Resolving dictionary ids for {} quads...", quads.length);
        long start = System.nanoTime();
        final QuadIds[] ids = new QuadIds[quads.length];
        try {
            pool.submit(() -> IntStream.range(0, quads.length).parallel().forEach(i -> {
                Quad q = quads[i];
                ids[i] = new QuadIds(
                        w.locateGraph(q.getGraph()),
                        w.locateSubject(q.getSubject()),
                        w.locatePredicate(q.getPredicate()),
                        w.locateObject(q.getObject()));
            })).get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while resolving quad ids", ex);
        } catch (ExecutionException ex) {
            throw Futures.unwrap(ex.getCause(), "resolving quad ids");
        }
        logger.info("Resolved ids for {} quads in {} s", quads.length, (System.nanoTime() - start) / 1_000_000_000L);
        return ids;
    }

    ParallelBGIndex(ParallelPositionalDictionaryWriter dictWriter, Index type, QuadIds[] tuples) {
        logger.info("Creating index {}", type);
        this.type = type;
        this.comps = type.name().toCharArray();

        // Superblock entries store the cumulative count of set bits; the largest such
        // value is the bitmap length. Every level writes at most one row per unique quad
        // plus one padding row per L0 id, so (tuples.length + maxL0Id) bounds it - the
        // same sizing as the sequential BGIndex.
        int sbBits = IndexLevelEmitter.superblockBits(tuples.length, computeMaxL0Id(dictWriter));
        int bbBits = IndexLevelEmitter.blockBits();

        String n1 = IndexLevelEmitter.levelName(comps[1]);
        String n2 = IndexLevelEmitter.levelName(comps[2]);
        String n3 = IndexLevelEmitter.levelName(comps[3]);

        B1 = new BitPackedUnSignedLongBuffer(Path.of("B" + n1), 1);
        B2 = new BitPackedUnSignedLongBuffer(Path.of("B" + n2), 1);
        B3 = new BitPackedUnSignedLongBuffer(Path.of("B" + n3), 1);

        S1 = new BitPackedUnSignedLongBuffer(Path.of("S" + n1), getBitSize(dictWriter, comps[1]));
        S2 = new BitPackedUnSignedLongBuffer(Path.of("S" + n2), getBitSize(dictWriter, comps[2]));
        S3 = new BitPackedUnSignedLongBuffer(Path.of("S" + n3), getBitSize(dictWriter, comps[3]));

        SB1 = new BitPackedUnSignedLongBuffer(Path.of("SB" + n1), sbBits);
        SB2 = new BitPackedUnSignedLongBuffer(Path.of("SB" + n2), sbBits);
        SB3 = new BitPackedUnSignedLongBuffer(Path.of("SB" + n3), sbBits);

        BB1 = new BitPackedUnSignedLongBuffer(Path.of("BB" + n1), bbBits);
        BB2 = new BitPackedUnSignedLongBuffer(Path.of("BB" + n2), bbBits);
        BB3 = new BitPackedUnSignedLongBuffer(Path.of("BB" + n3), bbBits);

        processTuples(dictWriter, tuples);
        prepareForReading();
    }

    private static long id(QuadIds t, char component) {
        return switch (component) {
            case 'G' -> t.g;
            case 'S' -> t.s;
            case 'P' -> t.p;
            case 'O' -> t.o;
            default -> throw new IllegalStateException("Unknown component: " + component);
        };
    }

    private static long count(ParallelPositionalDictionaryWriter w, char component) {
        return switch (component) {
            case 'G' -> w.getNumberOfGraphs();
            case 'S' -> w.getNumberOfSubjects();
            case 'P' -> w.getNumberOfPredicates();
            case 'O' -> w.getNumberOfObjects();
            default -> throw new IllegalStateException("Unknown component: " + component);
        };
    }

    private static int getBitSize(ParallelPositionalDictionaryWriter w, char component) {
        return byteRoundedWidth(count(w, component) + 1);
    }

    /**
     * Maximum id of this index's first (L0) component. The L0 dimension is padded up to
     * this value so that select1(id) addresses each L0 id's slot directly; it therefore
     * also bounds the cumulative set-bit count stored in the superblock directory.
     */
    private long computeMaxL0Id(ParallelPositionalDictionaryWriter w) {
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

    private void processTuples(ParallelPositionalDictionaryWriter w, QuadIds[] tuples) {
        // INFO bracketing: sorting millions of quads takes minutes with no other output.
        // Invoked from a worker of the writer's pool, parallelSort forks into that same
        // pool, so the sort obeys the -cores bound.
        logger.info("Sorting {} quads for {}...", tuples.length, type.name());
        long sortStart = System.nanoTime();
        Arrays.parallelSort(tuples, tupleOrder());
        logger.info("Sorted {} in {} s", type.name(), (System.nanoTime() - sortStart) / 1_000_000_000L);

        // The level/padding/dedup rule lives in IndexLevelEmitter, shared with
        // BGIndex and the disk pipeline's HugeIndexWriter (BG-299).
        IndexLevelEmitter out = new IndexLevelEmitter(S1, B1, SB1, BB1, S2, B2, SB2, BB2, S3, B3, SB3, BB3);
        long count = 0;
        long totalQuads = tuples.length;
        for (QuadIds t : tuples) {
            if (++count % 1_000_000 == 0) {
                logger.info("{} processed {} / {} quads...", type.name(), count, totalQuads);
            }
            out.emit(id(t, comps[0]), id(t, comps[1]), id(t, comps[2]), id(t, comps[3]));
        }
        out.finish(computeMaxL0Id(w));
        flushAllBuffers();
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
