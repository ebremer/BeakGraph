package com.ebremer.beakgraph.huge;

import com.ebremer.beakgraph.core.AbstractGraphBuilder;
import java.nio.file.Path;

/**
 * The settings every disk-based engine's builder carries - the spill
 * workspace and the sorter sizing knobs the CLI exposes as {@code -workdir},
 * {@code -termSpillBatch}, {@code -idSpillBatch}, {@code -mergeFanIn} and
 * {@code -spillMB} - with one validation and one javadoc instead of three
 * copies (BG-314). Each engine passes its own defaults to the constructor
 * and documents them there:
 * <ul>
 *   <li>the sequential huge writer ({@code -method 1}) spills smaller runs
 *       ({@code 1 << 18} term records, {@code 1 << 21} id records, fan-in 64)
 *       with a byte budget of heap / 8 - one batch is live per sorter and
 *       merges run one at a time;</li>
 *   <li>hugeUltra and plaid ({@code -method 4/5}) spill in the background
 *       and merge concurrently, so two batches are in flight per sorter
 *       (budget heap / 16) and they take bigger runs ({@code 1 << 19} /
 *       {@code 1 << 22}) with fan-in 128: fewer merge levels, less disk churn,
 *       the open-file peak bounded by {@code setMergeConcurrency}.</li>
 * </ul>
 */
public abstract class AbstractDiskWriterBuilder<T extends AbstractDiskWriterBuilder<T>> extends AbstractGraphBuilder<T> {

    protected Path workDir;
    protected int termSpillBatch;
    protected int idSpillBatch;
    protected int mergeFanIn;
    protected long termSpillBytes;

    protected AbstractDiskWriterBuilder(int termSpillBatch, int idSpillBatch, int mergeFanIn, long termSpillBytes) {
        this.termSpillBatch = termSpillBatch;
        this.idSpillBatch = idSpillBatch;
        this.mergeFanIn = mergeFanIn;
        this.termSpillBytes = termSpillBytes;
    }

    /**
     * Directory for the build workspace (spill runs, staged buffers).
     * Defaults to the destination's directory - the workspace transiently
     * needs disk on the order of a few times the (uncompressed) source.
     */
    public T setWorkDirectory(Path dir) {
        this.workDir = dir;
        return self();
    }

    /** Records buffered in RAM per term column before spilling a sorted run. */
    public T setTermSpillBatch(int records) {
        if (records < 1) throw new IllegalArgumentException("termSpillBatch must be >= 1");
        this.termSpillBatch = records;
        return self();
    }

    /** Records buffered in RAM per numeric (id/quad) sorter before spilling. */
    public T setIdSpillBatch(int records) {
        if (records < 1) throw new IllegalArgumentException("idSpillBatch must be >= 1");
        this.idSpillBatch = records;
        return self();
    }

    /** Maximum spill runs merged in one pass (each running merge holds {@code fanIn + 1} open files). */
    public T setMergeFanIn(int fanIn) {
        if (fanIn < 2) throw new IllegalArgumentException("mergeFanIn must be >= 2");
        this.mergeFanIn = fanIn;
        return self();
    }

    /**
     * Estimated heap of parsed terms one term sorter buffers before a run
     * spills, whatever the record count - the bound that keeps multi-KB WKT
     * literals from exhausting the heap (BG-125). See the class comment for
     * each engine's default.
     */
    public T setTermSpillBytes(long bytes) {
        if (bytes < 1) throw new IllegalArgumentException("termSpillBytes must be >= 1");
        this.termSpillBytes = bytes;
        return self();
    }

    public Path getWorkDirectory() { return workDir; }
    public int getTermSpillBatch() { return termSpillBatch; }
    public int getIdSpillBatch() { return idSpillBatch; }
    public int getMergeFanIn() { return mergeFanIn; }
    public long getTermSpillBytes() { return termSpillBytes; }

    /** Where the workspace is created: the configured directory, else the destination's. */
    public Path workspaceBase(Path dest) {
        if (workDir != null) {
            return workDir;
        }
        Path parent = dest.toAbsolutePath().getParent();
        return parent != null ? parent : Path.of(".");
    }
}
