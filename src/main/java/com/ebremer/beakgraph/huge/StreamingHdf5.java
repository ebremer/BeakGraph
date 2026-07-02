package com.ebremer.beakgraph.huge;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Factory for {@link StreamingHdf5File} backends.
 *
 * <p>The default provider is {@link NativeHdf5File} (the HDF Group's native
 * hdf5 library, the only backend that currently supports writing datasets in
 * chunks). A different backend - e.g. jHDF once it supports chunked/streaming
 * writes - is installed with {@link #setProvider}.
 *
 * @author Erich Bremer
 */
public final class StreamingHdf5 {

    /** Creates a new writable HDF5 file at the given path (truncating). */
    @FunctionalInterface
    public interface Provider {
        StreamingHdf5File create(Path path) throws IOException;
    }

    private static volatile Provider provider = NativeHdf5File::create;

    private StreamingHdf5() {}

    /** Replaces the backend used for all subsequently created files. */
    public static void setProvider(Provider p) {
        provider = Objects.requireNonNull(p, "provider");
    }

    /** Creates a new writable HDF5 file at {@code path} using the current backend. */
    public static StreamingHdf5File create(Path path) throws IOException {
        return provider.create(path);
    }
}
