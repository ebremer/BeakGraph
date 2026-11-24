package com.ebremer.beakgraph.store;

/**
 * A group of an open store, as the readers see it in either container
 * (SPECIFICATIONS.md §3): child groups, named byte arrays, and integer
 * attributes. An HDF5 group or a Zarr group; the readers cannot tell which,
 * and do not ask.
 *
 * <p>A child that is absent is {@code null}: most of a store's parts are
 * optional. A child that is there but breaks the container profile - a
 * dataset where a group belongs, a chunked HDF5 dataset, a compressed Zarr
 * array - is an {@link IllegalStateException} that names the child and the
 * rule, as is an attribute that is missing or not an integer.
 *
 * @author Erich Bremer
 */
public interface StoreGroup {

    /** The group's name in its parent, e.g. {@code dictionary}. */
    String name();

    /** The group's path in the store, e.g. {@code /.BG/dictionary}. */
    String path();

    /** The child group {@code name}, or null when absent. */
    StoreGroup group(String name);

    /** The byte array {@code name}, or null when absent. */
    StoreArray array(String name);

    /**
     * Makes every byte array below this group ready to read now, so that
     * nothing is looked for in the container later. A Zarr store maps every
     * chunk file below the group: a store is many files, and one replaced or
     * removed while it is open must find its reader holding all of the old
     * one, as the reader of an HDF5 file holds its one mapping - not looking
     * for a file that has gone. Nothing to do in HDF5. An array that breaks
     * the container profile is left for the read that asks for it to report.
     */
    default void load() {
    }

    /** Whether the group carries attribute {@code name}. */
    boolean hasAttribute(String name);

    /** Integer attribute {@code name}, whatever width the container stores it in. */
    long attribute(String name);

    /** A string attribute of this group, or null when it has none of that name (format v7, {@code /.BG/arrays}). */
    String stringAttribute(String name);

    /** The names of this group's child groups, in name order. */
    java.util.List<String> groupNames();

    /**
     * The typed array {@code name} of {@code /.BG/arrays} (format v7, RDFARRY.md
     * §3.5), read whole - its type, shape, values, dimension names where the
     * container keeps them, and attributes - or null when absent. Unlike
     * {@link #array}, any layout and codec the container's library reads is
     * accepted.
     */
    TypedArray typedArray(String name);

    /**
     * The typed array {@code name} of {@code /.BG/arrays} to be read a box of
     * rows at a time ({@link TypedArrayView}), or null when absent: what
     * BeakGraph reads packed sequences through (RDFARRY.md §3.6).
     */
    TypedArrayView typedArrayView(String name);
}
