package com.ebremer.beakgraph.core;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * The ONE rule for "is this file name a BeakGraph store": {@code .h5} or
 * {@code .hdf5}, case-insensitively. {@code -verify} accepted both while
 * {@code -export} and the LWS servlet accepted {@code .h5} only, so a tree
 * that verified as fully OK exported a subset with no message (BG-159,
 * BG-319).
 *
 * <p>And the one rule for a store in the Zarr container, which is a
 * directory: its name ends in {@code .zarr}, and it holds a
 * {@code zarr.json}. The commands that walk a tree for stores
 * ({@code -verify}, {@code -export}) take such a directory as ONE store and
 * do not walk into it.
 *
 * <p>A Zarr store in one file, a ZIP archive, is found in a tree by the name
 * {@code *.zarr.zip}: a tree holds other archives, and they are not stores.
 * Named outright, a store is opened by what it is, whatever it is called.
 *
 * @author Erich Bremer
 */
public final class BeakGraphFiles {

    private BeakGraphFiles() {}

    public static boolean isBeakGraphFileName(String name) {
        if (name == null) {
            return false;
        }
        String lower = name.toLowerCase(Locale.ROOT);
        return lower.endsWith(".h5") || lower.endsWith(".hdf5");
    }

    /** Whether {@code name} is a Zarr store's: it ends in {@code .zarr}, case-insensitively. */
    public static boolean isZarrStoreName(String name) {
        return name != null && name.toLowerCase(Locale.ROOT).endsWith(".zarr");
    }

    /** Whether {@code name} is that of a Zarr store in a ZIP archive: it ends in {@code .zarr.zip}, case-insensitively. */
    public static boolean isZarrArchiveName(String name) {
        return name != null && name.toLowerCase(Locale.ROOT).endsWith(".zarr.zip");
    }

    /** Whether {@code name} names a store in any container: {@code .h5}, {@code .hdf5}, {@code .zarr} or {@code .zarr.zip}. */
    public static boolean isStoreName(String name) {
        return isBeakGraphFileName(name) || isZarrStoreName(name) || isZarrArchiveName(name);
    }

    /**
     * Whether a destination NAMED {@code name} is one store file, and not a
     * directory to put stores in: {@code .h5}, {@code .hdf5}, or {@code .zip}
     * (a Zarr store in a ZIP archive, which a destination may name without
     * the {@code .zarr} before it).
     */
    public static boolean isStoreFileName(String name) {
        return isBeakGraphFileName(name) || (name != null && name.toLowerCase(Locale.ROOT).endsWith(".zip"));
    }

    /** Whether {@code dir} is a Zarr store by the rule above: a directory named {@code *.zarr} holding a {@code zarr.json}. */
    public static boolean isZarrStoreDirectory(Path dir) {
        Path name = dir.getFileName();
        return name != null && isZarrStoreName(name.toString())
                && Files.isDirectory(dir) && Files.isRegularFile(dir.resolve("zarr.json"));
    }

    /**
     * Whether a store is already at {@code path}: a file that is not empty,
     * or a directory with a {@code zarr.json} (a Zarr store is moved into
     * place whole, so one that is there is complete).
     */
    public static boolean storeExists(Path path) {
        if (Files.isDirectory(path)) {
            return Files.isRegularFile(path.resolve("zarr.json"));
        }
        try {
            return Files.isRegularFile(path) && Files.size(path) > 0;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Every store under {@code root}, in path order: the files named
     * {@code .h5} / {@code .hdf5} / {@code .zarr.zip} and the Zarr store
     * directories, which are not walked into.
     *
     * @throws IOException if a directory or file of the tree cannot be read
     */
    public static List<Path> findStores(Path root) throws IOException {
        List<Path> found = new ArrayList<>();
        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                if (isZarrStoreDirectory(dir)) {
                    found.add(dir);
                    return FileVisitResult.SKIP_SUBTREE;
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                String name = file.getFileName().toString();
                if (attrs.isRegularFile() && (isBeakGraphFileName(name) || isZarrArchiveName(name))) {
                    found.add(file);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                throw exc; // a tree that cannot be read whole is not one to call verified
            }
        });
        Collections.sort(found);
        return found;
    }

    /** The name without its {@code .h5} / {@code .hdf5} / {@code .zarr} / {@code .zarr.zip} / {@code .zip} suffix; unchanged when it has none. */
    public static String stripExtension(String name) {
        String lower = name.toLowerCase(Locale.ROOT);
        if (lower.endsWith(".zarr.zip")) {
            return name.substring(0, name.length() - 9);
        }
        if (lower.endsWith(".zip")) {
            return name.substring(0, name.length() - 4);
        }
        if (lower.endsWith(".zarr")) {
            return name.substring(0, name.length() - 5);
        }
        if (lower.endsWith(".hdf5")) {
            return name.substring(0, name.length() - 5);
        }
        if (lower.endsWith(".h5")) {
            return name.substring(0, name.length() - 3);
        }
        return name;
    }
}
