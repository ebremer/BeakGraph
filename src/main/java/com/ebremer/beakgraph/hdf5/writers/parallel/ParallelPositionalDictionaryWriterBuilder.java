package com.ebremer.beakgraph.hdf5.writers.parallel;

import com.ebremer.beakgraph.hdf5.writers.PositionalDictionaryWriterBuilder;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

/**
 * Builder for {@link ParallelPositionalDictionaryWriter}. The whole ingest
 * pipeline - parse, bnode alignment, numeric canonicalization, spatial /
 * feature augmentation, VoID statistics - is inherited unchanged from the
 * sequential {@link PositionalDictionaryWriterBuilder} (it is stream-driven
 * and inherently single-pass); only the writer that consumes the collected
 * state differs.
 */
public class ParallelPositionalDictionaryWriterBuilder extends PositionalDictionaryWriterBuilder {
    private ForkJoinPool pool;

    /** Worker pool every parallel build stage runs in; defaults to the common pool. */
    public ParallelPositionalDictionaryWriterBuilder setPool(ForkJoinPool pool) {
        this.pool = pool;
        return this;
    }

    // Covariant overrides so fluent chains keep this builder's type.

    @Override
    public ParallelPositionalDictionaryWriterBuilder setSource(File src) {
        super.setSource(src);
        return this;
    }

    @Override
    public ParallelPositionalDictionaryWriterBuilder setSources(List<File> files) {
        super.setSources(files);
        return this;
    }

    @Override
    public ParallelPositionalDictionaryWriterBuilder setDestination(File dest) {
        super.setDestination(dest);
        return this;
    }

    @Override
    public ParallelPositionalDictionaryWriterBuilder setSpatial(boolean flag) {
        super.setSpatial(flag);
        return this;
    }

    @Override
    public ParallelPositionalDictionaryWriterBuilder setFeatures(boolean flag) {
        super.setFeatures(flag);
        return this;
    }

    @Override
    public ParallelPositionalDictionaryWriterBuilder setName(String name) {
        super.setName(name);
        return this;
    }

    /**
     * Same ingest as {@link #build()}, but the collected state feeds the
     * parallel writer. Deliberately NOT an override of build():
     * ParallelPositionalDictionaryWriter is not a PositionalDictionaryWriter
     * subtype, because subclassing would run the sequential constructor's
     * entire single-threaded build before the parallel one could start.
     */
    public ParallelPositionalDictionaryWriter buildParallel() throws IOException {
        parse();
        return new ParallelPositionalDictionaryWriter(this, pool != null ? pool : ForkJoinPool.commonPool());
    }
}
