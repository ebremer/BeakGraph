package com.ebremer.beakgraph.hdf5.writers.ultra;

import io.jhdf.api.WritableDataset;
import io.jhdf.api.WritableGroup;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * Concurrently-settable 1-bit buffer for the index B bitmaps. Bits live
 * MSB-first inside big-endian 64-bit words, which serializes to exactly the
 * byte stream the sequential writer's bit accumulator emits (byte {@code k}
 * holds bits {@code 8k..8k+7}, most significant first).
 *
 * <p>Words start all-zero and writers only ever SET bits, so concurrent
 * emission chunks combine with an atomic bitwise OR: two chunks may share the
 * word at their seam, and OR is the one update that is correct regardless of
 * arrival order. Interior words are touched by a single chunk, where the
 * atomic costs nothing measurable (uncontended {@code lock or}).
 *
 * <p>Word granularity is deliberately the same as {@code Params.BLOCKSIZE}
 * (64): the rank directory's block boundaries are word boundaries, so
 * {@link #word} + {@code Long.bitCount} is all the SB/BB builder needs.
 */
final class UltraBitmap {

    private static final VarHandle WORDS = MethodHandles.arrayElementVarHandle(long[].class);

    private final String name;
    private final long numBits;
    private final long[] words;

    UltraBitmap(String name, long numBits) {
        if (numBits < 0) {
            throw new IllegalArgumentException("numBits must be >= 0, got " + numBits);
        }
        long w = (numBits + 63) >>> 6;
        if (w > Integer.MAX_VALUE - 16 || ((numBits + 7) >>> 3) > Integer.MAX_VALUE - 16) {
            throw new IllegalArgumentException(
                    "Bitmap '" + name + "' of " + numBits + " bits is too large for the in-memory ultra writer");
        }
        this.name = name;
        this.numBits = numBits;
        this.words = new long[(int) w];
    }

    /** Sets bit {@code bitIndex} to 1. Thread-safe (atomic OR). */
    void set(long bitIndex) {
        if (bitIndex < 0 || bitIndex >= numBits) {
            throw new IndexOutOfBoundsException("Bit " + bitIndex + " out of bounds [0, " + numBits + ")");
        }
        long mask = 1L << (63 - (bitIndex & 63));
        WORDS.getAndBitwiseOr(words, (int) (bitIndex >>> 6), mask);
    }

    long getNumBits() {
        return numBits;
    }

    int numWords() {
        return words.length;
    }

    /** Word {@code w} (bits {@code 64w..64w+63}, MSB first). */
    long word(int w) {
        return words[w];
    }

    /** Test access: bit value at {@code bitIndex}. */
    int get(long bitIndex) {
        if (bitIndex < 0 || bitIndex >= numBits) {
            throw new IndexOutOfBoundsException("Bit " + bitIndex + " out of bounds [0, " + numBits + ")");
        }
        return (int) ((words[(int) (bitIndex >>> 6)] >>> (63 - (bitIndex & 63))) & 1L);
    }

    /**
     * The exact byte stream the sequential 1-bit buffer produces: bits
     * MSB-first, final partial byte zero-padded (word tails are already zero).
     */
    byte[] toBytes() {
        int nBytes = (int) ((numBits + 7) >>> 3);
        byte[] out = new byte[nBytes];
        for (int b = 0; b < nBytes; b++) {
            out[b] = (byte) (words[b >>> 3] >>> (56 - ((b & 7) << 3)));
        }
        return out;
    }

    void add(WritableGroup group) {
        byte[] bytes = toBytes();
        if (bytes.length > 0) {
            WritableDataset ds = group.putDataset(name, bytes);
            ds.putAttribute("width", 1);
            ds.putAttribute("numEntries", numBits);
        }
    }
}
