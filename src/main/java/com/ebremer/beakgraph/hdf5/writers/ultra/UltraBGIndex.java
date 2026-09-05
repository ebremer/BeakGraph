package com.ebremer.beakgraph.hdf5.writers.ultra;

import static com.ebremer.beakgraph.utils.UTIL.byteRoundedWidth;
import com.ebremer.beakgraph.hdf5.Index;
import static com.ebremer.beakgraph.Params.SUPERBLOCKSIZE;
import static com.ebremer.beakgraph.utils.UTIL.MinBits;
import io.jhdf.api.WritableGroup;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import org.apache.jena.sparql.core.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The ultra twin of the sequential BGIndex / ParallelBGIndex, rebuilt around
 * three ideas:
 *
 * <ol>
 * <li><b>Packed primitive keys.</b> A quad's four dictionary ids concatenate
 *     into ONE unsigned integer key per index ordering (single {@code long}
 *     when the id widths fit 63 bits, else a hi/lo pair). Sorting keys IS
 *     sorting id tuples IS the sequential writer's NodeComparator quad order -
 *     no objects, no comparator, no clone of a tuple array. Small inputs use
 *     {@code Arrays.parallelSort(long[])}; large or 2-word inputs use the
 *     O(n)-per-pass {@link ParallelRadixSort}.</li>
 * <li><b>Deduplicate once.</b> Duplicate quads collapse right after the GSPO
 *     sort in a parallel compaction; GPOS repacks its keys from the already
 *     deduplicated set (a bijection - distinct tuples stay distinct), sorts
 *     fewer rows, and both emission passes drop the per-row duplicate
 *     check.</li>
 * <li><b>Chunk-parallel emission.</b> The level-emission scan - the sequential
 *     writers' last O(n) single-thread tail - runs as two parallel passes:
 *     pass 1 counts each chunk's emitted rows per level (each row's behaviour
 *     depends only on its predecessor tuple, which a chunk reads directly at
 *     its seam), a prefix sum turns counts into absolute offsets, and pass 2
 *     writes S/B entries positionally ({@link UltraPackedBuffer} entries are
 *     byte-aligned; {@link UltraBitmap} merges seam words with atomic OR).
 *     The SB/BB rank directories are then derived from word popcounts -
 *     BLOCKSIZE is the machine word, so BB/SB values are pure prefix-sum
 *     lookups, reproducing the sequential scan's directory exactly.</li>
 * </ol>
 *
 * Output buffers are byte-identical to the sequential emission for the same
 * id tuples: same rows, same padding for absent L0 ids, same widths, same
 * directory values.
 */
final class UltraBGIndex {

    private static final Logger logger = LoggerFactory.getLogger(UltraBGIndex.class);
    private static final int WORD = 64; // == Params.BLOCKSIZE; the directory math relies on it

    private final Index type;
    private final UltraPackedBuffer S1, S2, S3;
    private final UltraBitmap B1, B2, B3;
    private final UltraPackedBuffer SB1, SB2, SB3;
    private final UltraPackedBuffer BB1, BB2, BB3;

    /** Bit layout of one packed key: components in index order, k0 highest. */
    record Layout(int b0, int b1, int b2, int b3) {
        int off3() { return 0; }
        int off2() { return b3; }
        int off1() { return b2 + b3; }
        int off0() { return b1 + b2 + b3; }
        int total() { return b0 + b1 + b2 + b3; }
    }

    // ------------------------------------------------------------------
    // orchestration
    // ------------------------------------------------------------------

    /**
     * Resolves every quad's ids once (O(1) map lookups), packs GSPO keys,
     * sorts, deduplicates, and builds both indexes concurrently. Releases the
     * ingest's quad array as soon as the keys are packed.
     */
    static UltraBGIndex[] buildBoth(UltraDictionary dict, UltraIngest ingest, ForkJoinPool pool) throws IOException {
        // Only the COUNT is kept here: the quad array, the packed key arrays
        // and the pre-dedup sorted pair live inside packSortDedup, so they are
        // collectable while both index builds and the GPOS repack allocate
        // (BG-115; ParallelRadixSort's contract asks callers to drop them).
        final int n = ingest.getQuads().length;
        final long e = dict.getNumberOfGraphs();
        final long p = dict.getNumberOfPredicates();
        final long o = dict.getNumberOfObjects();
        final int bG = MinBits(e), bS = MinBits(e), bP = MinBits(p), bO = MinBits(o);
        final Layout gspoLayout = new Layout(bG, bS, bP, bO);
        final Layout gposLayout = new Layout(bG, bP, bO, bS);
        final int totalBits = gspoLayout.total();
        if (totalBits > 126) {
            throw new IllegalArgumentException(
                    "Dictionary id widths total " + totalBits + " bits; too large for the in-memory ultra writer");
        }
        final boolean twoWords = totalBits > 63;

        long[][] deduped = packSortDedup(dict, ingest, gspoLayout, totalBits, twoWords, pool);
        final long[] dLo = deduped[0], dHi = deduped[1];

        ForkJoinTask<UltraBGIndex> gspoTask = pool.submit(() ->
                new UltraBGIndex(Index.GSPO, dLo, dHi, gspoLayout, dict, n, pool));
        ForkJoinTask<UltraBGIndex> gposTask = pool.submit(() -> {
            // Repack (g,s,p,o) -> (g,p,o,s) from the deduplicated keys and sort.
            int m = dLo.length;
            logger.info("Repacking and sorting {} GPOS keys...", m);
            long gposStart = System.nanoTime();
            long[] pLo = new long[m];
            long[] pHi = twoWords ? new long[m] : null;
            ParallelRadixSort.runChunks(pool, chunksFor(pool, m), m, (c, from, to) -> {
                for (int i = from; i < to; i++) {
                    long l = dLo[i], h = (dHi != null) ? dHi[i] : 0L;
                    pack(pLo, pHi, i,
                            extract(l, h, gspoLayout.off0(), gspoLayout.b0()),
                            extract(l, h, gspoLayout.off2(), gspoLayout.b2()),
                            extract(l, h, gspoLayout.off3(), gspoLayout.b3()),
                            extract(l, h, gspoLayout.off1(), gspoLayout.b1()),
                            gposLayout);
                }
            });
            long[][] s = sortKeys(pLo, pHi, totalBits, pool);
            logger.info("GPOS keys repacked and sorted in {} ms", (System.nanoTime() - gposStart) / 1_000_000L);
            return new UltraBGIndex(Index.GPOS, s[0], s[1], gposLayout, dict, n, pool);
        });
        return new UltraBGIndex[]{join(gspoTask, Index.GSPO), join(gposTask, Index.GPOS)};
    }

    /**
     * Resolves every quad's ids, packs GSPO keys, sorts and deduplicates; the
     * ONLY arrays that survive the call are the returned deduplicated pair.
     */
    private static long[][] packSortDedup(UltraDictionary dict, UltraIngest ingest, Layout gspoLayout,
                                          int totalBits, boolean twoWords, ForkJoinPool pool) throws IOException {
        final Quad[] quads = ingest.getQuads();
        final int n = quads.length;
        logger.info("Packing {} quad keys ({} bits, {} words)...", n, totalBits, twoWords ? 2 : 1);
        long start = System.nanoTime();
        final long[] lo = new long[n];
        final long[] hi = twoWords ? new long[n] : null;
        ParallelRadixSort.runChunks(pool, chunksFor(pool, n), n, (c, from, to) -> {
            for (int i = from; i < to; i++) {
                Quad q = quads[i];
                pack(lo, hi, i,
                        dict.locateGraph(q.getGraph()),
                        dict.locateSubject(q.getSubject()),
                        dict.locatePredicate(q.getPredicate()),
                        dict.locateObject(q.getObject()),
                        gspoLayout);
            }
        });
        ingest.releaseQuads();
        logger.info("Packed ids for {} quads in {} ms", n, (System.nanoTime() - start) / 1_000_000L);

        logger.info("Sorting {} GSPO keys ({})...", n,
                (hi == null && n < (1 << 20)) ? "JDK parallel sort" : "parallel radix sort");
        start = System.nanoTime();
        long[][] sorted = sortKeys(lo, hi, totalBits, pool);
        logger.info("GSPO keys sorted in {} ms", (System.nanoTime() - start) / 1_000_000L);
        start = System.nanoTime();
        long[][] deduped = dedup(sorted[0], sorted[1], pool);
        logger.info("Deduplicated in {} ms: {} unique quads of {} (GPOS reuses the deduplicated set)",
                (System.nanoTime() - start) / 1_000_000L, deduped[0].length, n);
        return deduped;
    }

    /**
     * Packs one key: values in this layout's component order, {@code v0}
     * ending up in the highest bits. Each component width is at most 34 bits,
     * so the running 128-bit left-shift-and-or below never shifts by >= 64.
     */
    static void pack(long[] lo, long[] hi, int i, long v0, long v1, long v2, long v3, Layout lay) {
        long l = v0, h = 0;
        h = (h << lay.b1()) | (l >>> (64 - lay.b1())); l = (l << lay.b1()) | v1;
        h = (h << lay.b2()) | (l >>> (64 - lay.b2())); l = (l << lay.b2()) | v2;
        h = (h << lay.b3()) | (l >>> (64 - lay.b3())); l = (l << lay.b3()) | v3;
        lo[i] = l;
        if (hi != null) {
            hi[i] = h;
        }
    }

    static long extract(long lo, long hi, int off, int bits) {
        long mask = (1L << bits) - 1; // bits <= 34, never 64
        if (off >= 64) return (hi >>> (off - 64)) & mask;
        if (off + bits <= 64) return (lo >>> off) & mask;
        return ((lo >>> off) | (hi << (64 - off))) & mask;
    }

    private static long[][] sortKeys(long[] lo, long[] hi, int totalBits, ForkJoinPool pool) throws IOException {
        if (hi == null && lo.length < (1 << 20)) {
            // Small single-word inputs: the JDK dual-pivot sort has lower
            // constant overhead than four radix passes. Signed order equals
            // unsigned order here - keys are at most 63 bits, never negative.
            try {
                pool.submit(() -> Arrays.parallelSort(lo)).get();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while sorting index keys", ex);
            } catch (ExecutionException ex) {
                Throwable cause = ex.getCause();
                if (cause instanceof RuntimeException re) throw re;
                if (cause instanceof Error err) throw err;
                throw new IOException("Failed to sort index keys", cause);
            }
            return new long[][]{lo, null};
        }
        return ParallelRadixSort.sort(lo, hi, totalBits, pool);
    }

    /** Parallel compaction of adjacent duplicate keys (arrays already sorted). */
    private static long[][] dedup(long[] lo, long[] hi, ForkJoinPool pool) {
        final int n = lo.length;
        if (n == 0) {
            return new long[][]{lo, hi};
        }
        final int chunks = chunksFor(pool, n);
        final int[] kept = new int[chunks];
        ParallelRadixSort.runChunks(pool, chunks, n, (c, from, to) -> {
            int k = 0;
            for (int i = from; i < to; i++) {
                if (i == 0 || lo[i] != lo[i - 1] || (hi != null && hi[i] != hi[i - 1])) {
                    k++;
                }
            }
            kept[c] = k;
        });
        final int[] starts = new int[chunks];
        int total = 0;
        for (int c = 0; c < chunks; c++) {
            starts[c] = total;
            total += kept[c];
        }
        if (total == n) {
            return new long[][]{lo, hi}; // nothing to drop
        }
        final long[] outLo = new long[total];
        final long[] outHi = (hi != null) ? new long[total] : null;
        ParallelRadixSort.runChunks(pool, chunks, n, (c, from, to) -> {
            int at = starts[c];
            for (int i = from; i < to; i++) {
                if (i == 0 || lo[i] != lo[i - 1] || (hi != null && hi[i] != hi[i - 1])) {
                    outLo[at] = lo[i];
                    if (outHi != null) {
                        outHi[at] = hi[i];
                    }
                    at++;
                }
            }
        });
        return new long[][]{outLo, outHi};
    }

    private static int chunksFor(ForkJoinPool pool, int n) {
        return (int) Math.max(1, Math.min(pool.getParallelism() * 4L, Math.max(1, n / 32_768)));
    }

    // ------------------------------------------------------------------
    // one index build: two-pass parallel emission
    // ------------------------------------------------------------------

    private UltraBGIndex(Index type, long[] keyLo, long[] keyHi, Layout lay, UltraDictionary dict,
                         long originalQuadCount, ForkJoinPool pool) {
        logger.info("Creating index {} (ultra, chunk-parallel emission)", type);
        final long indexStart = System.nanoTime();
        this.type = type;
        final char[] comps = type.name().toCharArray();
        final int m = keyLo.length;
        final long maxL0Id = count(dict, comps[0]);

        // Identical width sizing to the sequential/parallel index builders.
        long maxCumulativeOnes = originalQuadCount + maxL0Id + 128L;
        int sbBits = byteRoundedWidth(maxCumulativeOnes);
        int bbBits = byteRoundedWidth(SUPERBLOCKSIZE);
        final int w1 = getBitSize(dict, comps[1]);
        final int w2 = getBitSize(dict, comps[2]);
        final int w3 = getBitSize(dict, comps[3]);
        final String n1 = levelName(comps[1]);
        final String n2 = levelName(comps[2]);
        final String n3 = levelName(comps[3]);

        // ---- pass 1: rows emitted per level, per chunk ----
        final int chunks = (m == 0) ? 1 : chunksFor(pool, m);
        final long[] rows1 = new long[chunks], rows2 = new long[chunks], rows3 = new long[chunks];
        if (m > 0) {
            ParallelRadixSort.runChunks(pool, chunks, m, (c, from, to) -> {
                long r1 = 0, r2 = 0, r3 = 0;
                boolean first = (from == 0);
                long p0 = 0, p1 = 0, p2 = 0;
                if (!first) {
                    long l = keyLo[from - 1], h = (keyHi != null) ? keyHi[from - 1] : 0L;
                    p0 = extract(l, h, lay.off0(), lay.b0());
                    p1 = extract(l, h, lay.off1(), lay.b1());
                    p2 = extract(l, h, lay.off2(), lay.b2());
                }
                for (int r = from; r < to; r++) {
                    long l = keyLo[r], h = (keyHi != null) ? keyHi[r] : 0L;
                    long k0 = extract(l, h, lay.off0(), lay.b0());
                    long k1 = extract(l, h, lay.off1(), lay.b1());
                    long k2 = extract(l, h, lay.off2(), lay.b2());
                    boolean ch0 = first || k0 != p0;
                    boolean ch1 = ch0 || k1 != p1;
                    boolean ch2 = ch1 || k2 != p2;
                    long pads = ch0 ? (first ? k0 - 1 : k0 - p0 - 1) : 0;
                    r3 += 1 + pads;
                    r2 += (ch2 ? 1 : 0) + pads;
                    r1 += (ch1 ? 1 : 0) + pads;
                    p0 = k0; p1 = k1; p2 = k2; first = false;
                }
                if (to == m) {
                    long tail = maxL0Id - extract(keyLo[m - 1], (keyHi != null) ? keyHi[m - 1] : 0L, lay.off0(), lay.b0());
                    r1 += tail; r2 += tail; r3 += tail;
                }
                rows1[c] = r1; rows2[c] = r2; rows3[c] = r3;
            });
        } else {
            // No tuples at all (theoretical: VoID always contributes some).
            // The sequential loop pads maxL0Id-1 empty rows in this case.
            long pads = Math.max(0, maxL0Id - 1);
            rows1[0] = pads; rows2[0] = pads; rows3[0] = pads;
        }
        final long[] start1 = prefix(rows1), start2 = prefix(rows2), start3 = prefix(rows3);
        final long t1 = start1[chunks], t2 = start2[chunks], t3 = start3[chunks];
        logger.info("{}: counted rows in {} chunk(s): L1={}, L2={}, L3={}", type, chunks, t1, t2, t3);

        S1 = new UltraPackedBuffer("S" + n1, t1, w1);
        S2 = new UltraPackedBuffer("S" + n2, t2, w2);
        S3 = new UltraPackedBuffer("S" + n3, t3, w3);
        B1 = new UltraBitmap("B" + n1, t1);
        B2 = new UltraBitmap("B" + n2, t2);
        B3 = new UltraBitmap("B" + n3, t3);

        // ---- pass 2: positional emission ----
        long phase = System.nanoTime();
        if (m > 0) {
            ParallelRadixSort.runChunks(pool, chunks, m, (c, from, to) -> {
                long c1 = start1[c], c2 = start2[c], c3 = start3[c];
                boolean first = (from == 0);
                long p0 = 0, p1 = 0, p2 = 0;
                if (!first) {
                    long l = keyLo[from - 1], h = (keyHi != null) ? keyHi[from - 1] : 0L;
                    p0 = extract(l, h, lay.off0(), lay.b0());
                    p1 = extract(l, h, lay.off1(), lay.b1());
                    p2 = extract(l, h, lay.off2(), lay.b2());
                }
                for (int r = from; r < to; r++) {
                    long l = keyLo[r], h = (keyHi != null) ? keyHi[r] : 0L;
                    long k0 = extract(l, h, lay.off0(), lay.b0());
                    long k1 = extract(l, h, lay.off1(), lay.b1());
                    long k2 = extract(l, h, lay.off2(), lay.b2());
                    long k3 = extract(l, h, lay.off3(), lay.b3());
                    boolean ch0 = first || k0 != p0;
                    boolean ch1 = ch0 || k1 != p1;
                    boolean ch2 = ch1 || k2 != p2;
                    if (ch0) {
                        long pads = first ? k0 - 1 : k0 - p0 - 1;
                        for (long k = 0; k < pads; k++) {
                            S1.set(c1, 0); B1.set(c1); c1++;
                            S2.set(c2, 0); B2.set(c2); c2++;
                            S3.set(c3, 0); B3.set(c3); c3++;
                        }
                    }
                    S3.set(c3, k3);
                    if (ch2) B3.set(c3);
                    c3++;
                    if (ch2) {
                        S2.set(c2, k2);
                        if (ch1) B2.set(c2);
                        c2++;
                    }
                    if (ch1) {
                        S1.set(c1, k1);
                        if (ch0) B1.set(c1);
                        c1++;
                    }
                    p0 = k0; p1 = k1; p2 = k2; first = false;
                }
                if (to == m) {
                    long tail = maxL0Id - p0;
                    for (long k = 0; k < tail; k++) {
                        S1.set(c1, 0); B1.set(c1); c1++;
                        S2.set(c2, 0); B2.set(c2); c2++;
                        S3.set(c3, 0); B3.set(c3); c3++;
                    }
                }
            });
        } else {
            long pads = Math.max(0, maxL0Id - 1);
            for (long k = 0; k < pads; k++) {
                S1.set(k, 0); B1.set(k);
                S2.set(k, 0); B2.set(k);
                S3.set(k, 0); B3.set(k);
            }
        }

        logger.info("{}: emitted S/B buffers in {} ms", type, (System.nanoTime() - phase) / 1_000_000L);

        // ---- rank/select directories from word popcounts ----
        phase = System.nanoTime();
        SB1 = new UltraPackedBuffer("SB" + n1, t1 / SUPERBLOCKSIZE + 1, sbBits);
        SB2 = new UltraPackedBuffer("SB" + n2, t2 / SUPERBLOCKSIZE + 1, sbBits);
        SB3 = new UltraPackedBuffer("SB" + n3, t3 / SUPERBLOCKSIZE + 1, sbBits);
        BB1 = new UltraPackedBuffer("BB" + n1, t1 / WORD + 1, bbBits);
        BB2 = new UltraPackedBuffer("BB" + n2, t2 / WORD + 1, bbBits);
        BB3 = new UltraPackedBuffer("BB" + n3, t3 / WORD + 1, bbBits);
        ParallelRadixSort.runChunks(pool, 3, 3, (c, from, to) -> {
            for (int i = from; i < to; i++) {
                switch (i) {
                    case 0 -> buildDirectory(B1, SB1, BB1);
                    case 1 -> buildDirectory(B2, SB2, BB2);
                    default -> buildDirectory(B3, SB3, BB3);
                }
            }
        });
        logger.info("{}: rank/select directories built in {} ms", type, (System.nanoTime() - phase) / 1_000_000L);
        logger.info("Index {} built in {} ms: {} L3 rows", type, (System.nanoTime() - indexStart) / 1_000_000L, t3);
    }

    /**
     * SB[j] = set bits in the first j superblocks; BB[k] = set bits between
     * block k's superblock start and block k (0 when k opens its superblock).
     * Exactly the values the sequential advanceLevel scan writes, but read off
     * a word-popcount prefix array. BLOCKSIZE == 64 == word size makes every
     * boundary a word boundary.
     */
    private static void buildDirectory(UltraBitmap B, UltraPackedBuffer SB, UltraPackedBuffer BB) {
        final int words = B.numWords();
        final long bits = B.getNumBits();
        final long[] cum = new long[words + 1];
        for (int w = 0; w < words; w++) {
            cum[w + 1] = cum[w] + Long.bitCount(B.word(w));
        }
        final int wordsPerSuper = SUPERBLOCKSIZE / WORD;
        SB.set(0, 0);
        long nSB = bits / SUPERBLOCKSIZE;
        for (long j = 1; j <= nSB; j++) {
            SB.set(j, cum[(int) (j * wordsPerSuper)]);
        }
        BB.set(0, 0);
        long nBB = bits / WORD;
        for (long k = 1; k <= nBB; k++) {
            BB.set(k, cum[(int) k] - cum[(int) ((k / wordsPerSuper) * wordsPerSuper)]);
        }
    }

    private static long[] prefix(long[] counts) {
        long[] out = new long[counts.length + 1];
        for (int i = 0; i < counts.length; i++) {
            out[i + 1] = out[i] + counts[i];
        }
        return out;
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

    private static long count(UltraDictionary w, char component) {
        return switch (component) {
            case 'G' -> w.getNumberOfGraphs();
            case 'S' -> w.getNumberOfSubjects();
            case 'P' -> w.getNumberOfPredicates();
            case 'O' -> w.getNumberOfObjects();
            default -> throw new IllegalStateException("Unknown component: " + component);
        };
    }

    private static int getBitSize(UltraDictionary w, char component) {
        return byteRoundedWidth(count(w, component) + 1);
    }

    private static UltraBGIndex join(ForkJoinTask<UltraBGIndex> task, Index which) throws IOException {
        try {
            return task.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while building index " + which, ex);
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof IOException io) throw io;
            if (cause instanceof RuntimeException re) throw re;
            if (cause instanceof Error err) throw err;
            throw new IOException("Failed to build index " + which, cause);
        }
    }

    void add(WritableGroup hdt) {
        WritableGroup index = hdt.putGroup(type.name());
        S1.add(index); S2.add(index); S3.add(index);
        B1.add(index); B2.add(index); B3.add(index);
        SB1.add(index); SB2.add(index); SB3.add(index);
        BB1.add(index); BB2.add(index); BB3.add(index);
    }
}
