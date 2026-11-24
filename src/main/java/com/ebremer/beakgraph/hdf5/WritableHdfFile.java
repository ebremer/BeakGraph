package com.ebremer.beakgraph.hdf5;

import com.ebremer.beakgraph.store.Container;
import com.ebremer.beakgraph.store.WritableStore;
import com.ebremer.falcon.hdf5.Hdf5Writer;
import com.ebremer.falcon.hdf5.datatype.Datatype;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;

/**
 * A store being written into one HDF5 file, in BeakGraph's HDF5 container
 * profile (SPECIFICATIONS.md §3): groups, 1-D datasets of 8-bit signed
 * integers in contiguous layout and without filters, and scalar 32-bit /
 * 64-bit integer attributes ({@code int} is 32-bit, {@code long} 64-bit). The
 * HDF5 implementation of {@link WritableStore}, on Falcon
 * (https://github.com/ebremer/falcon).
 *
 * <p>Falcon writes to a temporary file beside {@code path} - a dataset given
 * whole once the next object is added, so its bytes are held only until then;
 * a piece of a dataset as it is given - and moves the finished file to
 * {@code path} on {@link #commit()}, forced to disk first. A build that fails
 * leaves nothing behind: {@link #close()} without a commit discards the
 * temporary file. So write inside try-with-resources, and commit as its last
 * statement:
 *
 * <pre>{@code
 * try (WritableHdfFile file = WritableHdfFile.write(tmp)) {
 *     WritableGroup store = file.putGroup(".BG");
 *     ...
 *     file.commit();
 * }
 * }</pre>
 *
 * @author Erich Bremer
 */
public final class WritableHdfFile implements WritableStore {

    private static final long[] SCALAR = new long[0];

    private final Hdf5Writer writer;
    private boolean finished;

    private WritableHdfFile(Hdf5Writer writer) {
        this.writer = writer;
    }

    /** Begins writing the file at {@code path}; a file already there is replaced by {@link #commit()}. */
    public static WritableHdfFile write(Path path) {
        return new WritableHdfFile(Hdf5Writer.create(path));
    }

    @Override
    public Container container() {
        return Container.HDF5;
    }

    /** The file's root group. */
    @Override
    public WritableGroup root() {
        return new Hdf5Group(writer.root());
    }

    /** Creates (and returns) a group in the file's root group. */
    @Override
    public WritableGroup putGroup(String name) {
        return new Hdf5Group(writer.group(name));
    }

    /**
     * Completes the file: writes what is still pending and the file's
     * metadata, forces it to disk, and moves it to its path. A failure
     * discards the temporary file before it propagates.
     */
    @Override
    public void commit() throws IOException {
        finished = true;
        try {
            writer.close();
        } catch (IOException | RuntimeException | Error e) {
            try {
                writer.abort();
            } catch (RuntimeException | Error cleanup) {
                e.addSuppressed(cleanup);
            }
            throw e;
        }
    }

    /** Discards the file unless it was {@linkplain #commit() committed}; never fails. */
    @Override
    public void close() {
        if (!finished) {
            finished = true;
            try {
                writer.abort();
            } catch (RuntimeException ignored) {
                // the build has already failed; its own exception is the one to report
            }
        }
    }

    private record Hdf5Group(Hdf5Writer.GroupWriter group) implements WritableGroup {

        @Override
        public WritableGroup putGroup(String name) {
            return new Hdf5Group(group.group(name));
        }

        /** The array is kept until the next object is added to the file. */
        @Override
        public WritableDataset putDataset(String name, byte[] data) {
            return new Hdf5Dataset(group.byteDataset(name, data, new long[]{data.length}), data.length, data.length);
        }

        /** Falcon puts each piece in the file as it is given, at its place in the dataset. */
        @Override
        public WritableDataset createDataset(String name, long length) {
            return new Hdf5Dataset(group.createDataset(name, Datatype.int8(), length), length, 0);
        }

        @Override
        public void putAttribute(String name, int value) {
            group.intAttribute(name, new int[]{value}, SCALAR);
        }

        @Override
        public void putAttribute(String name, long value) {
            group.attribute(name, Datatype.int64(), SCALAR, new long[]{value});
        }

        @Override
        public void putAttribute(String name, String value) {
            group.stringAttribute(name, value);
        }

        @Override
        public void putTypedArray(String name, com.ebremer.beakgraph.store.TypedArray array, boolean compress) {
            long[] shape = array.shape();
            Datatype type = switch (array.type()) {
                case F64 -> Datatype.float64();
                case F32 -> Datatype.float32();
                case I64 -> Datatype.int64();
                case I32 -> Datatype.int32();
                case BOOL -> Datatype.bool();   // the enum {FALSE, TRUE} over int8, which h5py reads as bool
            };
            Hdf5Writer.DatasetWriter ds = group.createDataset(name, type, shape);
            if (compress && array.elementCount() > 0) {
                ds.chunked(rowChunk(shape, array.rowBytes())).shuffle().deflate(6);
            }
            ds.write(array.values());
            for (java.util.Map.Entry<String, Object> a : array.attributes().entrySet()) {
                if (a.getValue() instanceof String s) {
                    ds.stringAttribute(a.getKey(), s);
                } else {
                    ds.attribute(a.getKey(), Datatype.int64(), SCALAR, new long[]{(Long) a.getValue()});
                }
            }
        }
    }

    /** Whole rows, at most about 1 MiB a chunk (HDF5 chunks are 4 GiB at most, and a reader decodes one at a time). */
    static long[] rowChunk(long[] shape, long rowBytes) {
        long[] chunk = new long[shape.length];
        for (int i = 1; i < shape.length; i++) {
            chunk[i] = Math.max(1, shape[i]);
        }
        chunk[0] = Math.max(1, Math.min(Math.max(1, shape[0]), (1L << 20) / Math.max(1, rowBytes)));
        return chunk;
    }

    private static final class Hdf5Dataset implements WritableDataset {

        private final Hdf5Writer.DatasetWriter dataset;
        /** How many bytes the dataset holds. */
        private final long length;
        /** How many of them have been written: all of them, for a dataset given its bytes whole. */
        private long written;

        Hdf5Dataset(Hdf5Writer.DatasetWriter dataset, long length, long written) {
            this.dataset = dataset;
            this.length = length;
            this.written = written;
        }

        @Override
        public void write(byte[] buf, int off, int len) {
            Objects.checkFromIndexSize(off, len, buf.length);
            if (len > length - written) {
                throw new IllegalStateException((written + len) + " bytes written to a dataset of " + length);
            }
            if (len == 0) {
                return;
            }
            // Falcon takes a piece as an array of exactly its bytes.
            byte[] piece = (off == 0 && len == buf.length) ? buf : Arrays.copyOfRange(buf, off, off + len);
            dataset.writeRaw(new long[]{written}, new long[]{len}, piece);
            written += len;
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public long written() {
            return written;
        }

        @Override
        public void putAttribute(String name, int value) {
            dataset.intAttribute(name, new int[]{value}, SCALAR);
        }

        @Override
        public void putAttribute(String name, long value) {
            dataset.attribute(name, Datatype.int64(), SCALAR, new long[]{value});
        }
    }
}
