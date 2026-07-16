package com.ebremer.beakgraph.core.lib;

import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * A minimal, thread-safe HyperLogLog cardinality sketch: 2^14 registers
 * (64 KiB as ints), standard error about 0.81%. Registers only ever move up
 * (CAS-max), so concurrent adds from any number of threads are safe and the
 * final state - and therefore the estimate - is a pure, order-independent
 * function of the input set: two builds that feed the same distinct values
 * report the same number, regardless of thread interleaving.
 *
 * <p>Callers supply well-mixed 64-bit hashes; the top {@code P} bits pick the
 * register and the remainder's leading-zero count is the rank. Includes the
 * linear-counting small-range correction, which makes estimates for
 * cardinalities far below the register count exact in practice.
 */
public final class HyperLogLog {

    private static final int P = 14;
    private static final int M = 1 << P;
    private static final double ALPHA = 0.7213 / (1 + 1.079 / M);

    private final AtomicIntegerArray registers = new AtomicIntegerArray(M);

    /** Records one 64-bit hash. Thread-safe. */
    public void add(long hash) {
        int idx = (int) (hash >>> (64 - P));
        long w = hash << P;
        int rank = (w == 0) ? (64 - P + 1) : Long.numberOfLeadingZeros(w) + 1;
        int cur;
        while ((cur = registers.get(idx)) < rank) {
            if (registers.compareAndSet(idx, cur, rank)) {
                break;
            }
        }
    }

    /** The cardinality estimate (rounded). */
    public long estimate() {
        double sum = 0;
        int zeros = 0;
        for (int i = 0; i < M; i++) {
            int r = registers.get(i);
            sum += 1.0 / (1L << r);
            if (r == 0) {
                zeros++;
            }
        }
        double e = ALPHA * M * (double) M / sum;
        if (e <= 2.5 * M && zeros > 0) {
            e = M * Math.log((double) M / zeros); // small-range (linear counting)
        }
        return Math.round(e);
    }

    /** splitmix64 finalizer: turns any 64-bit value into a well-mixed hash. */
    public static long mix64(long z) {
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }
}
