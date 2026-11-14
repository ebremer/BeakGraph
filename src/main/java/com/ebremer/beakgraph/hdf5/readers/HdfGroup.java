package com.ebremer.beakgraph.hdf5.readers;

import com.ebremer.falcon.hdf5.Group;
import com.ebremer.falcon.hdf5.RangeReader;

/**
 * A group of an open store, as the readers are handed it: the HDF5 group, and
 * how the bytes of the datasets under it are reached.
 *
 * <p>A store opened from a local file is memory-mapped whole, and its
 * datasets are views of that mapping: {@code channel} is null. A store read
 * through a caller-supplied channel (an HTTP range channel, typically) cannot
 * be mapped: its datasets are read a range at a time through {@code channel},
 * the same reader the HDF5 metadata came through, so {@code HdfProfile} hands
 * it on to every child group.
 *
 * @param group   the HDF5 group
 * @param channel what the store's bytes are read through when it is not
 *                mapped from a local file, else null
 * @author Erich Bremer
 */
public record HdfGroup(Group group, RangeReader channel) {

    /** A group of a store mapped from a local file. */
    public HdfGroup(Group group) {
        this(group, null);
    }

    /** The group's name in its parent, e.g. {@code dictionary}. */
    public String name() {
        return group.name();
    }

    /** The group's path in the file, e.g. {@code /.BG/dictionary}. */
    public String path() {
        return group.path();
    }
}
