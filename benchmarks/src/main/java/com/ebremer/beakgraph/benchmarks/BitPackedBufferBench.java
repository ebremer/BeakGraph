package com.ebremer.beakgraph.benchmarks;

import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.io.ByteBufferBytes;
import com.ebremer.beakgraph.io.ChannelBytes;
import com.ebremer.beakgraph.io.MemorySegmentBytes;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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
 *
 * <p>{@code profile} is the receiver-type history of {@code get()}'s one
 * {@code RandomAccessBytes.getLong} call site: {@code mono} has only ever seen
 * a {@code ByteBufferBytes}; {@code mixed} has been driven through all three
 * implementations (ByteBufferBytes, MemorySegmentBytes, ChannelBytes) before
 * the measurement, as a JVM that opened small, FFM-mapped and remote datasets
 * has. Both measure the ByteBufferBytes view, so the difference is the cost
 * of the megamorphic call itself (BG-260).
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

    /** Receiver-type profile at the get() call site before measuring: see the class comment. */
    @Param({"mono", "mixed"})
    public String profile;

    private BitPackedUnSignedLongBuffer random;
    private BitPackedUnSignedLongBuffer sorted;
    private long[] probeIndexes;
    private long[] probeValues;
    private int cursor;

    @Setup
    public void setup() throws IOException {
        SplittableRandom rnd = new SplittableRandom(42);
        long maxVal = (width == 64) ? Long.MAX_VALUE : (1L << width) - 1;

        long[] randomValues = new long[ENTRIES];
        for (int i = 0; i < ENTRIES; i++) {
            randomValues[i] = rnd.nextLong(maxVal + 1);
        }
        byte[] packed = pack(randomValues, width);
        // The same read view the HDF5 readers build over a jHDF buffer.
        random = BitPackedUnSignedLongBuffer.readView(new ByteBufferBytes(ByteBuffer.wrap(packed)), ENTRIES, width);

        sorted = new BitPackedUnSignedLongBuffer(Path.of("bench-sorted"), width);
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
        if ("mixed".equals(profile)) {
            polluteReceiverProfile(packed);
        }
    }

    /**
     * Drives get() through a MemorySegmentBytes and a ChannelBytes view of the
     * same packed bytes, interleaved with the ByteBufferBytes view, long enough
     * for the JIT to compile the call site against all three receivers.
     */
    private void polluteReceiverProfile(byte[] packed) throws IOException {
        Path file = Files.createTempFile("bench-packed", ".bin");
        file.toFile().deleteOnExit();
        Files.write(file, packed);
        BitPackedUnSignedLongBuffer segment = BitPackedUnSignedLongBuffer.readView(
                MemorySegmentBytes.map(file, 0, packed.length), ENTRIES, width);
        FileChannel channel = FileChannel.open(file, StandardOpenOption.READ);   // lives as long as the fork
        BitPackedUnSignedLongBuffer remote = BitPackedUnSignedLongBuffer.readView(
                new ChannelBytes(channel, 0, packed.length), ENTRIES, width);
        long sink = 0;
        for (int round = 0; round < 2_000_000; round++) {
            long idx = probeIndexes[round & MASK];
            sink += random.get(idx) + segment.get(idx) + remote.get(idx);
        }
        if (sink == 0x5EED) {
            System.out.println("sink");   // keep the loop observable
        }
    }

    /** Big-endian MSB-first bit packing, exactly the buffer's own layout. */
    private static byte[] pack(long[] values, int width) {
        long bits = (long) values.length * width;
        byte[] out = new byte[(int) ((bits + 7) / 8)];
        long acc = 0;
        int have = 0;
        int p = 0;
        for (long v : values) {
            acc = (acc << width) | v;
            have += width;
            while (have >= 8) {
                out[p++] = (byte) (acc >>> (have - 8));
                have -= 8;
                acc &= (1L << have) - 1;
            }
        }
        if (have > 0) {
            out[p] = (byte) (acc << (8 - have));
        }
        return out;
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
