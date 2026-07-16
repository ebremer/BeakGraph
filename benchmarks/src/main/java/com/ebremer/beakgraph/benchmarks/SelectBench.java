package com.ebremer.beakgraph.benchmarks;

import com.ebremer.beakgraph.hdf5.BitPackedUnSignedLongBuffer;
import com.ebremer.beakgraph.hdf5.Index;
import com.ebremer.beakgraph.hdf5.readers.HDF5Reader;
import com.ebremer.beakgraph.hdf5.readers.IndexReader;
import com.ebremer.beakgraph.utils.HDTBitmapDirectory;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * select1 on a real store's GSPO object-level bitmap: the accelerated
 * superblock/block directory ({@link HDTBitmapDirectory}) vs the linear
 * word-scan fallback. Every iterator construction performs several of these,
 * and the GPOS nested scan performs two per object group, so this is the
 * navigation primitive behind index traversal.
 *
 * <p>{@code adjacentPair} mirrors the iterators' actual access pattern -
 * {@code select1(rank)} then {@code select1(rank + 1)} to close the range -
 * which today costs two full directory descents.
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
public class SelectBench {

    private static final int PROBES = 1 << 16;
    private static final int MASK = PROBES - 1;

    @Param({"50000"})
    public int subjects;

    private HDF5Reader reader;
    private BitPackedUnSignedLongBuffer bitmap;
    private HDTBitmapDirectory directory;
    private long[] ranks;
    private int cursor;

    @Setup
    public void setup() {
        reader = new HDF5Reader(SyntheticStore.get(subjects).toFile());
        IndexReader gspo = reader.getIndexReader(Index.GSPO);
        bitmap = gspo.getBitmapBuffer('O');
        directory = gspo.getDirectory('O');
        if (directory == null) {
            throw new IllegalStateException("Store has no rank/select directory (format version too old?)");
        }
        long ones = bitmap.stream().sum();
        SplittableRandom rnd = new SplittableRandom(42);
        ranks = new long[PROBES];
        for (int i = 0; i < PROBES; i++) {
            // Leave headroom of 1 so adjacentPair's rank+1 stays a valid query.
            ranks[i] = 1 + rnd.nextLong(Math.max(1, ones - 1));
        }
    }

    @TearDown
    public void tearDown() {
        reader.close();
    }

    @Benchmark
    public long directorySelect1() {
        return directory.select1(ranks[cursor++ & MASK]);
    }

    @Benchmark
    public long linearSelect1() {
        return bitmap.select1(ranks[cursor++ & MASK]);
    }

    @Benchmark
    public long adjacentPair() {
        long rank = ranks[cursor++ & MASK];
        return directory.select1(rank) + directory.select1(rank + 1);
    }
}
