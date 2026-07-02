package com.ebremer.beakgraph.huge;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import java.io.IOException;
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
 * the native library for the common platforms - nothing to install. Building
 * with {@code -Dhdf5.ffm} swaps in the HDF Group's own
 * {@code org.hdfgroup:hdf5-java-ffm} bindings (HDF5 2.1.x, Java 25 FFM, no
 * JNI), which come from GitHub Packages (authenticated) and require a system
 * HDF5 install; see the profile comments in pom.xml. The two provide identical
 * class names, so they are mutually exclusive on the classpath - this is a
 * dependency swap, not a runtime switch.
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

    private final long fileId;
    private final NativeGroup root;
    // Every open native handle (groups, datasets) is tracked so close() can
    // release them all before H5Fclose even if a caller leaked one.
    private final Deque<Runnable> openHandles = new ArrayDeque<>();
    private boolean closed = false;

    private NativeHdf5File(long fileId) {
        this.fileId = fileId;
        this.root = new NativeGroup(fileId, "/", false);
    }

    /**
     * True when the native HDF5 library can be loaded on this platform. Tests
     * use this to skip rather than fail where no natives exist.
     */
    public static boolean isAvailable() {
        try {
            H5.loadH5Lib();
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

    /** Creates a new HDF5 file at {@code path}, truncating any existing file. */
    public static NativeHdf5File create(Path path) throws IOException {
        try {
            H5.loadH5Lib();
        } catch (Throwable t) {
            throw new IOException(
                "Native HDF5 library unavailable (needed by the huge writer backend). "
              + "Ensure org.bytedeco:hdf5-platform natives are on the classpath for this platform.", t);
        }
        try {
            long fid = H5.H5Fcreate(path.toString(), HDF5Constants.H5F_ACC_TRUNC,
                    HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
            return new NativeHdf5File(fid);
        } catch (Exception e) {
            throw new IOException("H5Fcreate failed for " + path, e);
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

    /** Scalar int attribute: file type I32LE, so jHDF reads an Integer. */
    private static void writeIntAttribute(long objId, String name, int value) throws IOException {
        try {
            long space = H5.H5Screate(HDF5Constants.H5S_SCALAR);
            try {
                long attr = H5.H5Acreate(objId, name, HDF5Constants.H5T_STD_I32LE, space,
                        HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
                try {
                    H5.H5Awrite_int(attr, HDF5Constants.H5T_NATIVE_INT, new int[]{value});
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
                    H5.H5Awrite_long(attr, HDF5Constants.H5T_NATIVE_INT64, new long[]{value});
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
        private final boolean ownsHandle;

        NativeGroup(long groupId, String name, boolean ownsHandle) {
            this.groupId = groupId;
            this.ownsHandle = ownsHandle;
        }

        @Override
        public StreamingHdf5Group putGroup(String name) throws IOException {
            try {
                long gid = H5.H5Gcreate(groupId, name, HDF5Constants.H5P_DEFAULT,
                        HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
                NativeGroup g = new NativeGroup(gid, name, true);
                openHandles.push(() -> quietly(() -> {
                    try { H5.H5Gclose(gid); } catch (Exception e) { throw new RuntimeException(e); }
                }));
                return g;
            } catch (Exception e) {
                throw new IOException("H5Gcreate failed for group '" + name + "'", e);
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
                throw new IOException("Refusing to create empty dataset '" + name + "'");
            }
            try {
                long space = H5.H5Screate_simple(1, new long[]{length}, null);
                long dset = H5.H5Dcreate(groupId, name, HDF5Constants.H5T_STD_I8LE, space,
                        HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
                NativeDataset d = new NativeDataset(dset, space, name, length);
                openHandles.push(() -> quietly(d::releaseHandles));
                return d;
            } catch (Exception e) {
                throw new IOException("H5Dcreate failed for dataset '" + name + "'", e);
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
