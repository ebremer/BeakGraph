package com.ebremer.beakgraph.benchmarks;

import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import java.nio.file.Path;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * The read path's innermost primitive: bit-packed value decode.
 *
 * <p>Every id fetch, bitmap probe, and binary-search step in the query engine
 * funnels through {@link BitPackedUnSignedLongBuffer#get(long)}, so this is the
 * baseline to beat for any decode-level optimization (e.g. replacing the
 * byte-at-a-time loop with one unaligned long read). {@code sequentialSumViaGet}
 * vs {@code sequentialSumViaStream} shows the gap between random-access decode
 * and the streaming accumulator for range scans.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsAppend = {
        "--enable-native-access=ALL-UNNAMED",
        "--sun-misc-unsafe-memory-access=allow",
        "-Xmx4g"})
@State(Scope.Benchmark)
public class BitPackedBufferBench {

    private static final int ENTRIES = 1 << 20;
    private static final int PROBES = 1 << 16;
    private static final int MASK = PROBES - 1;

    /** Bit widths spanning the shapes real stores use (ids, offsets, counts). */
    @Param({"7", "13", "29", "41"})
    public int width;

    private BitPackedUnSignedLongBuffer random;
    private BitPackedUnSignedLongBuffer sorted;
    private long[] probeIndexes;
    private long[] probeValues;
    private int cursor;

    @Setup
    public void setup() {
        SplittableRandom rnd = new SplittableRandom(42);
        long maxVal = (width == 64) ? Long.MAX_VALUE : (1L << width) - 1;

        random = new BitPackedUnSignedLongBuffer(Path.of("bench-random"), null, 0, width);
        for (int i = 0; i < ENTRIES; i++) {
            random.writeLong(rnd.nextLong(maxVal + 1));
        }
        random.prepareForReading();

        sorted = new BitPackedUnSignedLongBuffer(Path.of("bench-sorted"), null, 0, width);
        long[] values = new long[ENTRIES];
        long v = 0;
        long step = Math.max(1, maxVal / ENTRIES);
        for (int i = 0; i < ENTRIES; i++) {
            v = Math.min(maxVal, v + rnd.nextLong(step + 1));
            values[i] = v;
            sorted.writeLong(v);
        }
        sorted.prepareForReading();

        probeIndexes = new long[PROBES];
        probeValues = new long[PROBES];
        for (int i = 0; i < PROBES; i++) {
            probeIndexes[i] = rnd.nextInt(ENTRIES);
            probeValues[i] = values[rnd.nextInt(ENTRIES)];
        }
    }

    @Benchmark
    public long randomGet() {
        return random.get(probeIndexes[cursor++ & MASK]);
    }

    @Benchmark
    public long binarySearch() {
        return sorted.binarySearch(0, ENTRIES - 1, probeValues[cursor++ & MASK]);
    }

    @Benchmark
    @OperationsPerInvocation(ENTRIES)
    public long sequentialSumViaGet() {
        long sum = 0;
        for (long i = 0; i < ENTRIES; i++) {
            sum += random.get(i);
        }
        return sum;
    }

    @Benchmark
    @OperationsPerInvocation(ENTRIES)
    public long sequentialSumViaStream() {
        return random.stream().sum();
    }
}
