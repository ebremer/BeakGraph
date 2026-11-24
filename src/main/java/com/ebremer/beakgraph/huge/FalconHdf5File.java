package com.ebremer.beakgraph.huge;

import com.ebremer.beakgraph.hdf5.WritableDataset;
import com.ebremer.beakgraph.hdf5.WritableGroup;
import com.ebremer.beakgraph.hdf5.WritableHdfFile;
import com.ebremer.beakgraph.store.Container;
import com.ebremer.beakgraph.store.WritableStore;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link StreamingHdf5File} written through Falcon
 * (https://github.com/ebremer/falcon), the pure-Java HDF5 library every reader
 * and every other writer of BeakGraph uses: the backend of the disk-based
 * writers. Falcon puts each piece of a dataset in the file as it is given, so
 * a store larger than the heap is assembled through one transfer buffer, with
 * no native HDF5 library, on every platform Java runs on and inside the
 * GraalVM native binary too.
 *
 * <p>It goes through the same door as the in-memory writers
 * ({@link WritableHdfFile}), so the two families write one container: 1-D
 * datasets of 8-bit integers in contiguous layout, and scalar 32-bit / 64-bit
 * integer attributes (SPECIFICATIONS.md §3).
 *
 * <p>The file is written beside its path and moved there by {@link #close()},
 * forced to disk first. A file closed with a dataset short of its length is
 * not moved there at all: nothing is left at the path or beside it, and the
 * close fails. So a build that fails while a dataset is being transferred
 * publishes nothing, whatever its caller then does with the path.
 *
 * <p>The same class writes the Zarr container ({@link #create(Path, Container)}):
 * the door is {@link WritableStore}, which is either, and a Zarr store is as
 * all-or-nothing as an HDF5 file - a directory written beside its path and
 * moved there complete.
 *
 * <p>Not thread-safe; the disk-based writers assemble a file from one thread.
 *
 * @author Erich Bremer
 */
public final class FalconHdf5File implements StreamingHdf5File {

    private final Path path;
    /** "HDF5 file", "Zarr store" or "Zarr store in a ZIP archive": what the messages call it. */
    private final String what;
    private final WritableStore file;
    private final FalconGroup root;
    private final List<FalconDataset> datasets = new ArrayList<>();
    private boolean closed;

    private FalconHdf5File(Path path, WritableStore file) {
        this.path = path;
        this.what = file.container().noun();
        this.file = file;
        this.root = new FalconGroup(file.root(), "/");
    }

    /** Begins a new file at {@code path}; a file already there is replaced when this one is closed complete. */
    public static StreamingHdf5File create(Path path) throws IOException {
        return create(path, Container.HDF5);
    }

    /** Begins a new store at {@code path} in {@code container}; what is already there is replaced when this one is closed complete. */
    public static StreamingHdf5File create(Path path, Container container) throws IOException {
        try {
            return new FalconHdf5File(path, WritableStore.create(path, container));
        } catch (RuntimeException e) {
            throw new IOException("Cannot create " + container.noun() + " " + path
                    + ": " + e.getMessage(), e);
        }
    }

    @Override
    public StreamingHdf5Group rootGroup() {
        return root;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        try (file) {    // discards the file beside the path unless it is committed below
            for (FalconDataset d : datasets) {
                if (!d.complete()) {
                    throw new IOException(what + " " + path + " closed with dataset '" + d.name + "' at "
                            + d.dataset.written() + " of " + d.dataset.length() + " bytes: nothing was written to the path");
                }
            }
            try {
                file.commit();
            } catch (RuntimeException e) {
                throw new IOException("Cannot complete " + what + " " + path + ": " + e.getMessage(), e);
            }
        }
    }

    private final class FalconGroup implements StreamingHdf5Group {

        private final WritableGroup group;
        /** Absolute HDF5 path, for messages. */
        private final String name;

        FalconGroup(WritableGroup group, String name) {
            this.group = group;
            this.name = name;
        }

        private String child(String childName) {
            return name.endsWith("/") ? name + childName : name + "/" + childName;
        }

        @Override
        public WritableGroup writable() {
            return group;
        }

        @Override
        public StreamingHdf5Group putGroup(String childName) throws IOException {
            try {
                return new FalconGroup(group.putGroup(childName), child(childName));
            } catch (RuntimeException e) {
                throw new IOException("Cannot create group '" + child(childName) + "': " + e.getMessage(), e);
            }
        }

        @Override
        public void putAttribute(String attribute, int value) throws IOException {
            try {
                group.putAttribute(attribute, value);
            } catch (RuntimeException e) {
                throw new IOException("Cannot write attribute '" + attribute + "' of '" + name + "': " + e.getMessage(), e);
            }
        }

        @Override
        public void putAttribute(String attribute, long value) throws IOException {
            try {
                group.putAttribute(attribute, value);
            } catch (RuntimeException e) {
                throw new IOException("Cannot write attribute '" + attribute + "' of '" + name + "': " + e.getMessage(), e);
            }
        }

        @Override
        public StreamingHdf5Dataset createByteDataset(String childName, long length) throws IOException {
            if (length <= 0) {
                // The RAM writers never emit an empty dataset (the bit-packed and data
                // buffers skip one), and a reader has nothing to map of one.
                throw new IOException("Refusing to create empty dataset '" + child(childName) + "'");
            }
            try {
                FalconDataset d = new FalconDataset(group.createDataset(childName, length), child(childName));
                datasets.add(d);
                return d;
            } catch (RuntimeException e) {
                throw new IOException("Cannot create dataset '" + child(childName) + "': " + e.getMessage(), e);
            }
        }
    }

    private static final class FalconDataset implements StreamingHdf5Dataset {

        private final WritableDataset dataset;
        private final String name;
        private boolean closed;

        FalconDataset(WritableDataset dataset, String name) {
            this.dataset = dataset;
            this.name = name;
        }

        boolean complete() {
            return dataset.written() == dataset.length();
        }

        @Override
        public void write(byte[] buf, int off, int len) throws IOException {
            if (len == 0) {
                return;
            }
            if (len > dataset.length() - dataset.written()) {
                throw new IOException("Dataset '" + name + "' overflow: " + (dataset.written() + len)
                        + " > declared length " + dataset.length());
            }
            long at = dataset.written();
            try {
                dataset.write(buf, off, len);
            } catch (RuntimeException e) {
                throw new IOException("Cannot write dataset '" + name + "' at offset " + at + ": " + e.getMessage(), e);
            }
        }

        @Override
        public void putAttribute(String attribute, int value) throws IOException {
            try {
                dataset.putAttribute(attribute, value);
            } catch (RuntimeException e) {
                throw new IOException("Cannot write attribute '" + attribute + "' of '" + name + "': " + e.getMessage(), e);
            }
        }

        @Override
        public void putAttribute(String attribute, long value) throws IOException {
            try {
                dataset.putAttribute(attribute, value);
            } catch (RuntimeException e) {
                throw new IOException("Cannot write attribute '" + attribute + "' of '" + name + "': " + e.getMessage(), e);
            }
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            if (!complete()) {
                throw new IOException("Dataset '" + name + "' closed after " + dataset.written()
                        + " of " + dataset.length() + " bytes - refusing to leave undefined data");
            }
        }
    }
}
