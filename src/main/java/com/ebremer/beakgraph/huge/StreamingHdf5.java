package com.ebremer.beakgraph.huge;

import java.io.IOException;
import java.nio.file.Files;
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

    private static final Provider DEFAULT = NativeHdf5File::create;
    private static volatile Provider provider = DEFAULT;

    private StreamingHdf5() {}

    /** Replaces the backend used for all subsequently created files. */
    public static void setProvider(Provider p) {
        provider = Objects.requireNonNull(p, "provider");
    }

    /** Creates a new writable HDF5 file at {@code path} using the current backend. */
    public static StreamingHdf5File create(Path path) throws IOException {
        return provider.create(path);
    }

    /** Reinstalls the native backend (tests that swapped it). */
    public static void resetProvider() {
        provider = DEFAULT;
    }

    /** True while the native {@link NativeHdf5File} backend is installed. */
    public static boolean isDefaultProvider() {
        return provider == DEFAULT;
    }

    /**
     * Fails fast, before any parsing, when the installed backend cannot work
     * at all: for the default backend that is the native library check
     * ({@link NativeHdf5File#requireAvailable}); a replaced provider is
     * trusted here and probed by {@link #requireWritable} (BG-441, BG-135).
     */
    public static void requireBackend() throws IOException {
        if (isDefaultProvider()) {
            NativeHdf5File.requireAvailable();
        }
    }

    /**
     * Proves the installed backend can create, write and close a file in
     * {@code dir} - whatever provider is installed - before the build spends
     * hours parsing and sorting only to fail at HDF5 assembly (BG-135). The
     * probe file is removed again.
     */
    public static void requireWritable(Path dir) throws IOException {
        probeFile(dir.resolve(".probe-" + System.nanoTime() + ".h5"));
    }

    /**
     * Proves the installed backend can create, write and close EXACTLY
     * {@code file}, then removes it. The disk writers probe their output's
     * temp path with it before any parsing, so a path the native library
     * cannot spell (non-BMP characters cross JNI as CESU-8, MAX_PATH) or
     * cannot write (ACLs) fails in the first second of the build, not after
     * the whole sort (BG-419).
     */
    public static void probeFile(Path file) throws IOException {
        try {
            try (StreamingHdf5File f = create(file)) {
                f.rootGroup().putAttribute("probe", 1);
            }
            if (!Files.exists(file)) {
                throw new IOException("the file was created under a different name");
            }
        } catch (IOException | RuntimeException | Error e) {
            throw new IOException("The streaming HDF5 backend (" + (isDefaultProvider() ? "native library" : provider)
                    + ") cannot write a file in " + file.toAbsolutePath().getParent() + ": " + e.getMessage(), e);
        } finally {
            try {
                Files.deleteIfExists(file);
            } catch (IOException ignored) {
                // a leftover probe is harmless; the workspace is deleted after the build
            }
        }
    }
}
