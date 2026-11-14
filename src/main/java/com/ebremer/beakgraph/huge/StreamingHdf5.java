package com.ebremer.beakgraph.huge;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Factory for {@link StreamingHdf5File} backends.
 *
 * <p>The default provider is {@link FalconHdf5File}: Falcon
 * (https://github.com/ebremer/falcon), the pure-Java HDF5 library of the
 * readers and the in-memory writers, which writes a dataset a piece at a time.
 * It needs nothing installed. A different backend is
 * installed with {@link #setProvider} (the tests fail a build through one).
 *
 * @author Erich Bremer
 */
public final class StreamingHdf5 {

    /** Creates a new writable HDF5 file at the given path (replacing a file there). */
    @FunctionalInterface
    public interface Provider {
        StreamingHdf5File create(Path path) throws IOException;
    }

    private static final Provider DEFAULT = FalconHdf5File::create;
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

    /** Reinstalls the Falcon backend (tests that swapped it). */
    public static void resetProvider() {
        provider = DEFAULT;
    }

    /** True while the {@link FalconHdf5File} backend is installed. */
    public static boolean isDefaultProvider() {
        return provider == DEFAULT;
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
     * temp path with it before any parsing, so a path that cannot be written
     * (ACLs, a read-only volume, a name the file system refuses) fails in the
     * first second of the build, not after the whole sort (BG-419).
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
            throw new IOException("The streaming HDF5 backend (" + (isDefaultProvider() ? "Falcon" : provider)
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
