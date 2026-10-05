package com.ebremer.beakgraph.huge;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;

/**
 * {@link StreamingHdf5File} backed by the HDF Group's native HDF5 library
 * (https://github.com/HDFGroup/hdf5) through the official {@code hdf.hdf5lib}
 * Java API. This is the only backend that can write a dataset piecewise today;
 * jHDF's writer needs the whole dataset in memory.
 *
 * <p>This class is written against the official {@code hdf.hdf5lib.H5} API and
 * is binding-agnostic: which jar supplies that API is a build-time choice (the
 * {@code hdf5-backend-*} profiles in the POM). The default is the JavaCPP
 * preset ({@code org.bytedeco:hdf5-platform}), which bundles the classes AND
 * the native library for the common platforms. Those bundled natives only
 * load when JavaCPP's {@code Loader} extracts them: the stock
 * {@code H5.loadH5Lib()} knows nothing of JavaCPP and resolves
 * {@code hdf5_java} from {@code java.library.path} alone, so without
 * {@link #loadNatives()} the ~40 MB of packaged natives were dead weight and
 * the disk-based writers silently required a system HDF5 install.
 * {@link #loadNatives()} prefers a system install on the library path (or an
 * explicit {@code hdf.hdf5lib.H5.hdf5lib} / {@code hdf.hdf5lib.H5.loadLibraryName}
 * property), then bootstraps the bundled natives, then lets the stock loader
 * report the failure. Caveat: the preset's Windows jar ships a JNI glue that
 * imports {@code hdf5.dll} without shipping it, so on Windows the bundled
 * natives are only usable next to a system {@code hdf5.dll}; Linux and macOS
 * jars are self-contained. Building
 * with {@code -Dhdf5.ffm=true} swaps in the HDF Group's own
 * {@code org.hdfgroup:hdf5-java-ffm} bindings (HDF5 2.1.x, Java 25 FFM, no
 * JNI), which come from GitHub Packages (authenticated) and require a system
 * HDF5 install; see the profile comments in pom.xml. The two provide identical
 * class names, so they are mutually exclusive on the classpath - this is a
 * dependency swap, not a runtime switch. The two do NOT share an
 * implementation, though: the FFM binding ships the typed helpers
 * ({@code H5Awrite_int}, {@code H5Awrite_long}, {@code H5Dwrite_int}, ...) as
 * throwing "not implemented yet" stubs, so this class uses only the
 * {@code byte[]} entry points ({@code H5Awrite} / {@code H5Dwrite} with a
 * {@code byte[]}), which both bindings implement (BG-439;
 * {@code NativeHdf5PortabilityTest} pins the rule).
 *
 * <p>Reader compatibility (see {@link StreamingHdf5File}): datasets are created
 * with a fixed 1-D dataspace and the library-default CONTIGUOUS layout, then
 * filled by sequential hyperslab {@code H5Dwrite} calls; attributes are scalar
 * {@code H5T_STD_I32LE} / {@code H5T_STD_I64LE}. jHDF maps these to
 * {@code ContiguousDataset} and {@link Integer}/{@link Long} attribute values,
 * exactly what the existing BeakGraph readers expect.
 *
 * <p>Not thread-safe; the huge writer drives it from a single thread.
 *
 * @author Erich Bremer
 */
public final class NativeHdf5File implements StreamingHdf5File {

    private static volatile Throwable unavailableCause;
    private static final String PROP_LIBRARY_NAME = "hdf.hdf5lib.H5.loadLibraryName";
    private static final String PROP_LIBRARY_PATH = "hdf.hdf5lib.H5.hdf5lib";
    private static final String GLUE = "hdf5_java";
    private static boolean loaded;                    // guarded by the class lock
    private static volatile String nativeSource;      // how the natives were found, once loaded
    private static volatile Throwable bundledFailure; // why the bundled natives were not used, if so

    /**
     * Loads the HDF5 JNI bindings exactly once, choosing in order: an explicit
     * {@code hdf.hdf5lib.H5.loadLibraryName} / {@code hdf.hdf5lib.H5.hdf5lib}
     * property; a system install whose {@code hdf5_java} library sits on
     * {@code java.library.path}; the natives bundled in the JavaCPP preset
     * (extracted through {@code org.bytedeco.javacpp.Loader}, reached
     * reflectively so the {@code -Dhdf5.ffm=true} profile compiles without it);
     * and finally the stock loader's own error. The bundled glue is
     * test-loaded before it is trusted: on Windows the preset's glue imports
     * a {@code hdf5.dll} the jar does not carry.
     */
    static synchronized void loadNatives() {
        if (loaded) {
            return;
        }
        String source;
        if (System.getProperty(PROP_LIBRARY_NAME) != null || System.getProperty(PROP_LIBRARY_PATH) != null) {
            source = "configured by system property";
        } else if (onLibraryPath()) {
            source = "system install on java.library.path";
        } else {
            String glue = bundledGlue();
            if (glue != null) {
                System.setProperty(PROP_LIBRARY_PATH, glue);
                source = "bundled JavaCPP natives (" + glue + ")";
            } else {
                source = "java.library.path (bundled natives not usable here"
                       + (bundledFailure != null ? ": " + bundledFailure : "") + ")";
            }
        }
        nativeSource = source;   // recorded before loading so a failure still says what was tried
        H5.loadH5Lib();
        loaded = true;
    }

    /** How the natives were found, or were last sought ({@code null} before any attempt); for logs, errors and tests. */
    public static String nativeSource() {
        return nativeSource;
    }

    private static boolean onLibraryPath() {
        String path = System.getProperty("java.library.path", "");
        String file = System.mapLibraryName(GLUE);
        for (String dir : path.split(java.util.regex.Pattern.quote(java.io.File.pathSeparator))) {
            if (!dir.isEmpty() && new java.io.File(dir, file).isFile()) {
                return true;
            }
        }
        return false;
    }

    /** Path of the bundled, proven-loadable {@code hdf5_java} glue, or null. */
    private static String bundledGlue() {
        try {
            Class<?> loader = Class.forName("org.bytedeco.javacpp.Loader");
            Class<?> glueClass = Class.forName("org.bytedeco.hdf5." + GLUE);
            // Extracts the preset's natives for this platform into JavaCPP's
            // cache and returns the path of the class's own JNI library; the
            // HDF Group glue sits beside it.
            String jni = (String) loader.getMethod("load", Class.class).invoke(null, glueClass);
            if (jni == null) {
                return null;
            }
            java.io.File glue = new java.io.File(new java.io.File(jni).getParentFile(), System.mapLibraryName(GLUE));
            if (!glue.isFile()) {
                return null;
            }
            System.load(glue.getPath());   // proves its dependencies resolve; the stock loader re-loads it as a no-op
            return glue.getPath();
        } catch (ReflectiveOperationException | LinkageError | RuntimeException e) {
            bundledFailure = e instanceof java.lang.reflect.InvocationTargetException ite && ite.getCause() != null
                    ? ite.getCause() : e;
            return null;
        }
    }

    private final long fileId;
    private final NativeGroup root;
    // Every open native handle (groups, datasets) is tracked so close() can
    // release them all before H5Fclose even if a caller leaked one.
    private final Deque<Runnable> openHandles = new ArrayDeque<>();
    private boolean closed = false;

    private NativeHdf5File(long fileId) {
        this.fileId = fileId;
        this.root = new NativeGroup(fileId, "/");
    }

    /**
     * True when the native HDF5 library can be loaded on this platform. Tests
     * use this to skip rather than fail where no natives exist.
     */
    public static boolean isAvailable() {
        try {
            loadNatives();
            H5.H5open();
            return true;
        } catch (Throwable t) {
            unavailableCause = t;
            return false;
        }
    }

    /** The reason {@link #isAvailable()} answered false, if it did. */
    public static Throwable getUnavailableCause() {
        return unavailableCause;
    }

    /**
     * Fails fast when the native library cannot be loaded. The disk-based
     * writers call this BEFORE parsing: the library used to be first touched
     * when the output file was created, after every parse, spill and sort
     * stage had already run (BG-441).
     *
     * @throws IOException naming the platform, what was tried and why it failed
     */
    public static void requireAvailable() throws IOException {
        try {
            loadNatives();
            H5.H5open();
        } catch (Throwable t) {
            throw new IOException(unavailableMessage(), t);
        }
    }

    private static String unavailableMessage() {
        return "Native HDF5 library unavailable on " + System.getProperty("os.name") + "/"
              + System.getProperty("os.arch") + " (needed by the disk-based writers, -method 1/4/5; sought: "
              + nativeSource + "). The bundled natives cover linux-x86_64 and macos-x86_64 (windows-x86_64 needs "
              + "an HDF5 1.14 install's bin directory, hdf5.dll and hdf5_java.dll, on PATH); no arm64 natives "
              + "are bundled - install HDF5 and put " + System.mapLibraryName(GLUE) + " on java.library.path, "
              + "or set -D" + PROP_LIBRARY_PATH + "=<path to " + System.mapLibraryName(GLUE) + ">."
              + (bundledFailure != null ? " Bundled natives: " + bundledFailure : "");
    }

    /** Creates a new HDF5 file at {@code path}, truncating any existing file. */
    public static NativeHdf5File create(Path path) throws IOException {
        try {
            loadNatives();
        } catch (Throwable t) {
            throw new IOException(unavailableMessage(), t);
        }
        // Only a path the JNI marshalling can garble (supplementary-plane
        // characters, or one at the Windows MAX_PATH edge) pays for a listing
        // of its directory, so a stray created under another name can be
        // removed again on the mismatch path below.
        boolean risky = path.toString().codePoints().anyMatch(c -> c > 0xFFFF) || path.toAbsolutePath().toString().length() > 240;
        Path parent = path.toAbsolutePath().getParent();
        java.util.Set<Path> before = (risky && parent != null && Files.isDirectory(parent)) ? listQuietly(parent) : null;
        long fid;
        try {
            fid = H5.H5Fcreate(path.toAbsolutePath().toString(), HDF5Constants.H5F_ACC_TRUNC,
                    HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
        } catch (Exception e) {
            throw new IOException("H5Fcreate failed for " + path, e);
        }
        if (!Files.exists(path)) {
            try { H5.H5Fclose(fid); } catch (Exception ignored) { }
            if (before != null) {
                for (Path stray : listQuietly(parent)) {
                    if (!before.contains(stray)) {
                        try { Files.deleteIfExists(stray); } catch (IOException ignored) { }
                    }
                }
            }
            // The path crosses JNI as modified UTF-8: supplementary-plane
            // characters (emoji, CJK Ext-B) arrive as CESU-8 surrogate pairs
            // and the library creates a differently named file, which the
            // final Files.move then cannot find - the finished store was
            // orphaned under a mojibake name (BG-419). Report it instead.
            throw new IOException("The native HDF5 library created " + path + " under a different name "
                    + "(non-BMP characters in the path, or a path beyond MAX_PATH?); use a path of BMP "
                    + "characters shorter than 260 characters for the destination and -workdir");
        }
        return new NativeHdf5File(fid);
    }

    private static java.util.Set<Path> listQuietly(Path dir) {
        try (java.util.stream.Stream<Path> s = Files.list(dir)) {
            return s.collect(java.util.stream.Collectors.toSet());
        } catch (IOException e) {
            return java.util.Set.of();
        }
    }

    @Override
    public StreamingHdf5Group rootGroup() {
        return root;
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        // Release children before the file: HDF5 keeps a file alive while any
        // object handle in it is open, and a leaked handle would strand the file.
        while (!openHandles.isEmpty()) {
            openHandles.pop().run();
        }
        try {
            H5.H5Fclose(fileId);
        } catch (Exception e) {
            throw new IOException("H5Fclose failed", e);
        }
    }

    private static void quietly(Runnable r) {
        try { r.run(); } catch (RuntimeException ignored) {}
    }

    // Attribute values go through H5Awrite(long, long, byte[]) ONLY: the HDF
    // Group's 2.1.x FFM binding implements that overload (and the byte[]
    // H5Dwrite) but stubs H5Awrite_int / H5Awrite_long with a throwing
    // "not implemented yet", so under -Dhdf5.ffm=true every huge build used
    // to fail at its FIRST attribute - after all the parsing, spilling and
    // sorting (BG-439). Memory type = file type (I32LE / I64LE) with bytes
    // laid out little-endian explicitly, so no host-order assumption either.
    private static byte[] littleEndian(int value) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
    }

    private static byte[] littleEndian(long value) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
    }

    /** Scalar int attribute: file type I32LE, so jHDF reads an Integer. */
    private static void writeIntAttribute(long objId, String name, int value) throws IOException {
        try {
            long space = H5.H5Screate(HDF5Constants.H5S_SCALAR);
            try {
                long attr = H5.H5Acreate(objId, name, HDF5Constants.H5T_STD_I32LE, space,
                        HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
                try {
                    H5.H5Awrite(attr, HDF5Constants.H5T_STD_I32LE, littleEndian(value));
                } finally {
                    H5.H5Aclose(attr);
                }
            } finally {
                H5.H5Sclose(space);
            }
        } catch (Exception e) {
            throw new IOException("Failed to write int attribute '" + name + "'", e);
        }
    }

    /** Scalar long attribute: file type I64LE, so jHDF reads a Long. */
    private static void writeLongAttribute(long objId, String name, long value) throws IOException {
        try {
            long space = H5.H5Screate(HDF5Constants.H5S_SCALAR);
            try {
                long attr = H5.H5Acreate(objId, name, HDF5Constants.H5T_STD_I64LE, space,
                        HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
                try {
                    H5.H5Awrite(attr, HDF5Constants.H5T_STD_I64LE, littleEndian(value));
                } finally {
                    H5.H5Aclose(attr);
                }
            } finally {
                H5.H5Sclose(space);
            }
        } catch (Exception e) {
            throw new IOException("Failed to write long attribute '" + name + "'", e);
        }
    }

    private final class NativeGroup implements StreamingHdf5Group {
        private final long groupId;
        /** Absolute HDF5 path, for error messages only: handle release is the openHandles deque's job alone (BG-132). */
        private final String path;

        NativeGroup(long groupId, String path) {
            this.groupId = groupId;
            this.path = path;
        }

        private String childPath(String name) {
            return path.endsWith("/") ? path + name : path + "/" + name;
        }

        @Override
        public StreamingHdf5Group putGroup(String name) throws IOException {
            try {
                long gid = H5.H5Gcreate(groupId, name, HDF5Constants.H5P_DEFAULT,
                        HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
                NativeGroup g = new NativeGroup(gid, childPath(name));
                openHandles.push(() -> quietly(() -> {
                    try { H5.H5Gclose(gid); } catch (Exception e) { throw new RuntimeException(e); }
                }));
                return g;
            } catch (Exception e) {
                throw new IOException("H5Gcreate failed for group '" + childPath(name) + "'", e);
            }
        }

        @Override
        public void putAttribute(String name, int value) throws IOException {
            writeIntAttribute(groupId, name, value);
        }

        @Override
        public void putAttribute(String name, long value) throws IOException {
            writeLongAttribute(groupId, name, value);
        }

        @Override
        public StreamingHdf5Dataset createByteDataset(String name, long length) throws IOException {
            if (length <= 0) {
                // The RAM writer never emits empty datasets (BitPacked/DataOutput
                // buffers skip them), and jHDF's getBuffer() cannot map one.
                throw new IOException("Refusing to create empty dataset '" + childPath(name) + "'");
            }
            try {
                long space = H5.H5Screate_simple(1, new long[]{length}, null);
                long dset = H5.H5Dcreate(groupId, name, HDF5Constants.H5T_STD_I8LE, space,
                        HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
                NativeDataset d = new NativeDataset(dset, space, childPath(name), length);
                openHandles.push(() -> quietly(d::releaseHandles));
                return d;
            } catch (Exception e) {
                throw new IOException("H5Dcreate failed for dataset '" + childPath(name) + "'", e);
            }
        }
    }

    private static final class NativeDataset implements StreamingHdf5Dataset {
        private final long datasetId;
        private final long fileSpaceId;
        private final String name;
        private final long length;
        private long position = 0;
        private boolean released = false;

        NativeDataset(long datasetId, long fileSpaceId, String name, long length) {
            this.datasetId = datasetId;
            this.fileSpaceId = fileSpaceId;
            this.name = name;
            this.length = length;
        }

        @Override
        public void write(byte[] buf, int off, int len) throws IOException {
            if (len == 0) return;
            if (position + len > length) {
                throw new IOException("Dataset '" + name + "' overflow: " + (position + len)
                        + " > declared length " + length);
            }
            // H5Dwrite reads elements from the START of the Java array, so a
            // non-zero offset needs a compact copy of just the slice.
            byte[] data = (off == 0) ? buf : Arrays.copyOfRange(buf, off, off + len);
            try {
                H5.H5Sselect_hyperslab(fileSpaceId, HDF5Constants.H5S_SELECT_SET,
                        new long[]{position}, null, new long[]{len}, null);
                long memSpace = H5.H5Screate_simple(1, new long[]{len}, null);
                try {
                    H5.H5Dwrite(datasetId, HDF5Constants.H5T_NATIVE_INT8, memSpace, fileSpaceId,
                            HDF5Constants.H5P_DEFAULT, data);
                } finally {
                    H5.H5Sclose(memSpace);
                }
            } catch (Exception e) {
                throw new IOException("H5Dwrite failed for dataset '" + name + "' at offset " + position, e);
            }
            position += len;
        }

        @Override
        public void putAttribute(String name, int value) throws IOException {
            writeIntAttribute(datasetId, name, value);
        }

        @Override
        public void putAttribute(String name, long value) throws IOException {
            writeLongAttribute(datasetId, name, value);
        }

        @Override
        public void close() throws IOException {
            if (released) return;
            boolean complete = (position == length);
            releaseHandles();
            if (!complete) {
                throw new IOException("Dataset '" + name + "' closed after " + position
                        + " of " + length + " bytes - refusing to leave undefined data");
            }
        }

        private void releaseHandles() {
            if (released) return;
            released = true;
            try { H5.H5Sclose(fileSpaceId); } catch (Exception ignored) {}
            try { H5.H5Dclose(datasetId); } catch (Exception ignored) {}
        }
    }
}
