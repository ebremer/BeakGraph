package com.ebremer.beakgraph.hdf5.writers.ultra;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;

/**
 * Parallel LSD radix sort for the ultra index's packed tuple keys: rows are
 * unsigned keys of up to 128 bits held in a {@code lo} array and an optional
 * {@code hi} array ({@code null} when the whole key fits 64 bits). O(n) per
 * 16-bit digit pass instead of the O(n log n) comparisons of a comparator
 * sort, and every pass parallelizes as per-chunk histogram, per-bucket prefix
 * offsets, then per-chunk stable scatter.
 *
 * <p>16 divides 64, so a digit never straddles the lo/hi boundary; digits at
 * or above {@code totalBits} are identically zero and their passes are never
 * run. LSD with a stable scatter is deterministic: equal keys keep their
 * relative order, and since the key IS the entire tuple, the sorted output is
 * a pure function of the input multiset.
 */
public final class ParallelRadixSort {

    private static final int RADIX_BITS = 16;
    private static final int BUCKETS = 1 << RADIX_BITS;

    private ParallelRadixSort() {}

    /**
     * Sorts rows by unsigned (hi,lo) key using {@code pool} for the parallel
     * phases. The arrays are permuted via an internal double buffer; callers
     * MUST use the returned pair {@code {lo, hi}} (either the original or the
     * scratch instances, depending on pass parity) and drop their own
     * references.
     */
    public static long[][] sort(long[] lo, long[] hi, int totalBits, ForkJoinPool pool) {
        int n = lo.length;
        if (hi != null && hi.length != n) {
            throw new IllegalArgumentException("hi/lo length mismatch: " + hi.length + " vs " + n);
        }
        if (totalBits < 1 || totalBits > 128 || (totalBits > 64 && hi == null)) {
            throw new IllegalArgumentException("Bad key width " + totalBits + " for " + (hi == null ? 1 : 2) + "-word keys");
        }
        if (n < 2) {
            return new long[][]{lo, hi};
        }
        final int passes = (totalBits + RADIX_BITS - 1) / RADIX_BITS;
        final int chunks = (n >= BUCKETS) ? Math.max(1, Math.min(pool.getParallelism(), 32)) : 1;
        long[] srcLo = lo, srcHi = hi;
        long[] dstLo = new long[n];
        long[] dstHi = (hi != null) ? new long[n] : null;

        final int[][] counts = new int[chunks][BUCKETS];
        for (int pass = 0; pass < passes; pass++) {
            final int shift = pass * RADIX_BITS;
            final long[] sLo = srcLo, sHi = srcHi, dLo = dstLo, dHi = dstHi;

            for (int[] c : counts) {
                java.util.Arrays.fill(c, 0);
            }
            runChunks(pool, chunks, n, (c, from, to) -> {
                int[] cnt = counts[c];
                for (int i = from; i < to; i++) {
                    cnt[digit(sLo, sHi, i, shift)]++;
                }
            });

            // Per-bucket, per-chunk start offsets; the scatter below is stable
            // because each chunk walks its rows in order from its own offsets.
            int running = 0;
            int nonZeroBuckets = 0;
            for (int d = 0; d < BUCKETS && running < n; d++) {
                boolean any = false;
                for (int c = 0; c < chunks; c++) {
                    int t = counts[c][d];
                    counts[c][d] = running;
                    running += t;
                    any |= t != 0;
                }
                if (any) {
                    nonZeroBuckets++;
                }
            }
            if (nonZeroBuckets <= 1) {
                continue; // every row shares this digit: the pass is a no-op permutation
            }

            runChunks(pool, chunks, n, (c, from, to) -> {
                int[] off = counts[c];
                for (int i = from; i < to; i++) {
                    int pos = off[digit(sLo, sHi, i, shift)]++;
                    dLo[pos] = sLo[i];
                    if (dHi != null) {
                        dHi[pos] = sHi[i];
                    }
                }
            });

            long[] t = srcLo; srcLo = dstLo; dstLo = t;
            if (hi != null) {
                t = srcHi; srcHi = dstHi; dstHi = t;
            }
        }
        return new long[][]{srcLo, srcHi};
    }

    private static int digit(long[] lo, long[] hi, int i, int shift) {
        return (shift < 64)
                ? (int) ((lo[i] >>> shift) & 0xFFFF)
                : (int) ((hi[i] >>> (shift - 64)) & 0xFFFF);
    }

    @FunctionalInterface
    public interface ChunkTask {
        void run(int chunk, int from, int to);
    }

    /** Runs {@code task} over [0,n) split into {@code chunks} ranges, inside {@code pool}. */
    public static void runChunks(ForkJoinPool pool, int chunks, int n, ChunkTask task) {
        if (chunks == 1) {
            task.run(0, 0, n);
            return;
        }
        int per = (n + chunks - 1) / chunks;
        try {
            pool.submit(() -> IntStream.range(0, chunks).parallel().forEach(c -> {
                int from = c * per;
                int to = Math.min(n, from + per);
                if (from < to) {
                    task.run(c, from, to);
                }
            })).get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted during parallel radix phase", ex);
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof RuntimeException re) throw re;
            if (cause instanceof Error err) throw err;
            throw new IllegalStateException("Parallel radix phase failed", cause);
        }
    }
}

