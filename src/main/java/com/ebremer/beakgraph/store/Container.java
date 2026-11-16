package com.ebremer.beakgraph.store;

import java.io.IOException;
import java.io.InputStream;
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
 *   <li>{@link #ZARR_ZIP}: that Zarr store in ONE file, a ZIP archive whose
 *       entries are the store's keys, none compressed (SPECIFICATIONS.md
 *       §3a, item 8).</li>
 * </ul>
 *
 * All are read and written through Falcon (https://github.com/ebremer/falcon).
 * A store's name says which it is: a writer given a destination that ends in
 * {@code .zarr} writes the Zarr directory store, one that ends in
 * {@code .zip} the Zarr store in a ZIP archive, any other name the HDF5
 * file; a reader opens a directory as Zarr, a file that begins as a ZIP
 * archive does as Zarr in an archive, and any other file as HDF5.
 *
 * @author Erich Bremer
 */
public enum Container {

    /** One HDF5 file. */
    HDF5("h5"),
    /** One Zarr v3 directory store. */
    ZARR("zarr", "Zarr store"),
    /** One ZIP archive that holds a Zarr v3 store, its entries uncompressed. */
    ZARR_ZIP("zarr.zip", "Zarr store in a ZIP archive");

    /** How a ZIP archive begins: the signature of its first entry's local header (PKWARE APPNOTE 4.3.7). */
    private static final byte[] ZIP_SIGNATURE = {'P', 'K', 3, 4};

    private final String extension;
    private final String noun;

    Container(String extension) {
        this(extension, "HDF5 file");
    }

    Container(String extension, String noun) {
        this.extension = extension;
        this.noun = noun;
    }

    /** The extension a store in this container is given: {@code h5}, {@code zarr} or {@code zarr.zip}. */
    public String extension() {
        return extension;
    }

    /** What a store in this container is called in messages: "HDF5 file", "Zarr store", "Zarr store in a ZIP archive". */
    public String noun() {
        return noun;
    }

    /** Whether a store in this container is a directory, where the others are one file. */
    public boolean isDirectory() {
        return this == ZARR;
    }

    /**
     * The container a store NAMED {@code name} is written in: the Zarr
     * directory store for {@code *.zarr}, Zarr in a ZIP archive for
     * {@code *.zip}, else HDF5.
     */
    public static Container ofName(String name) {
        String lower = name == null ? "" : name.toLowerCase(Locale.ROOT);
        if (lower.endsWith(".zarr")) {
            return ZARR;
        }
        return lower.endsWith(".zip") ? ZARR_ZIP : HDF5;
    }

    /**
     * The container of the store an {@code http(s)} URL names, by the last
     * name of its path ({@link #ofName}): nothing of it has been read yet. A
     * trailing slash is not part of the name.
     */
    public static Container ofRemote(java.net.URI uri) {
        String path = uri.getPath() == null ? "" : uri.getPath();
        while (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        return ofName(path.substring(path.lastIndexOf('/') + 1));
    }

    /** The container a destination is written in, by its name ({@link #ofName}). */
    public static Container ofDestination(Path destination) {
        Path name = destination.getFileName();
        return ofName(name == null ? null : name.toString());
    }

    /**
     * The container of the store that IS at {@code path}: a directory is
     * Zarr, a file that begins as a ZIP archive does is Zarr in an archive,
     * and anything else HDF5 - by what is there, whatever it is named.
     */
    public static Container ofExisting(Path path) {
        if (Files.isDirectory(path)) {
            return ZARR;
        }
        return isZipArchive(path) ? ZARR_ZIP : HDF5;
    }

    /**
     * Whether the file at {@code path} begins with a ZIP entry. An HDF5 file
     * begins with its own signature, so the two are never mistaken; an empty
     * archive (no entry) is no store, and reads as not one.
     */
    public static boolean isZipArchive(Path path) {
        byte[] head = new byte[ZIP_SIGNATURE.length];
        try (InputStream in = Files.newInputStream(path)) {
            return in.readNBytes(head, 0, head.length) == head.length && java.util.Arrays.equals(head, ZIP_SIGNATURE);
        } catch (IOException | RuntimeException e) {
            return false; // nothing to read there: whatever opens it says why
        }
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
