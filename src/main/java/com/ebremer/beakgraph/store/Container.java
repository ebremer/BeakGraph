package com.ebremer.beakgraph.store;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

/**
 * What a BeakGraph store is kept in. The store itself - its groups, its named
 * byte arrays, their integer attributes, and every byte of every array - is
 * the same in each (SPECIFICATIONS.md §3): only where the bytes lie differs.
 *
 * <ul>
 *   <li>{@link #HDF5}: one HDF5 file, each array a contiguous dataset
 *       (SPECIFICATIONS.md §3). The default.</li>
 *   <li>{@link #ZARR}: one Zarr v3 directory store, each array a Zarr array
 *       of uncompressed bytes (SPECIFICATIONS.md §3a).</li>
 * </ul>
 *
 * Both are read and written through Falcon (https://github.com/ebremer/falcon).
 * A store's name says which it is: a writer given a destination that ends in
 * {@code .zarr} writes the Zarr container, any other name the HDF5 one; a
 * reader opens a directory as Zarr and a file as HDF5.
 *
 * @author Erich Bremer
 */
public enum Container {

    /** One HDF5 file. */
    HDF5("h5"),
    /** One Zarr v3 directory store. */
    ZARR("zarr");

    private final String extension;

    Container(String extension) {
        this.extension = extension;
    }

    /** The extension a store in this container is given: {@code h5} or {@code zarr}. */
    public String extension() {
        return extension;
    }

    /** The container a store NAMED {@code name} is written in: Zarr for {@code *.zarr}, else HDF5. */
    public static Container ofName(String name) {
        return (name != null && name.toLowerCase(Locale.ROOT).endsWith(".zarr")) ? ZARR : HDF5;
    }

    /** The container a destination is written in, by its name ({@link #ofName}). */
    public static Container ofDestination(Path destination) {
        Path name = destination.getFileName();
        return ofName(name == null ? null : name.toString());
    }

    /** The container of the store that IS at {@code path}: a directory is Zarr, anything else HDF5. */
    public static Container ofExisting(Path path) {
        return Files.isDirectory(path) ? ZARR : HDF5;
    }

    /**
     * Whether a build may take the place of what is at {@code path}: nothing,
     * a file, a Zarr store, or an empty directory. Never a directory of
     * anything else - a store that is a directory is replaced whole, and a
     * mistyped destination must not cost a directory of other files.
     */
    public static boolean isReplaceable(Path path) {
        if (!Files.isDirectory(path, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            return true;
        }
        if (isZarrStore(path)) {
            return true;
        }
        try (java.util.stream.Stream<Path> entries = Files.list(path)) {
            return entries.findAny().isEmpty();
        } catch (java.io.IOException | RuntimeException e) {
            return false; // a directory that cannot be listed is not one to replace
        }
    }

    /**
     * Whether the directory {@code dir} is a Zarr store's root: it holds the
     * {@code zarr.json} of a group. What the commands that walk a tree for
     * stores go by, so that a store's own directories are not walked into.
     */
    public static boolean isZarrStore(Path dir) {
        return Files.isDirectory(dir) && Files.isRegularFile(dir.resolve("zarr.json"));
    }
}
