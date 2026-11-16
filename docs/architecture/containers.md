---
title: Containers
parent: Architecture
nav_order: 3
permalink: /architecture/containers/
---

# One store, two containers

HDF5 or Zarr — the same arrays, byte for byte
{: .fs-6 .fw-300 }

| | HDF5 | Zarr directory | Zarr in one ZIP archive |
|---|---|---|---|
| On disk | `name.h5`: one file | `name.zarr`: a `zarr.json` and one chunk file per array, about 130 files | `name.zarr.zip`: the directory's files as entries, none compressed |
| An array | a contiguous dataset | a `uint8` array, `bytes` codec only, one chunk up to 1 GiB | the same array, its chunk an entry |
| Local reads | the file memory-mapped once | every chunk mapped when the store opens | every chunk mapped where it lies in the archive |
| Remote reads | ranges of the one file | ranges of each chunk, through one block cache for the store | not read over HTTP |

- **The code above the container never sees it.** Readers get groups and arrays from an
  `OpenStore`; writers put groups, arrays and attributes through a `WritableStore`
  (`com.ebremer.beakgraph.store`). The same calls make the same store in either container, and the
  format version does not depend on the container
- **Bytes are read where they lie.** Every array is a `RandomAccessBytes`: a memory-mapped FFM
  segment (or a ByteBuffer view of it, the native binary's default), several mapped chunks of one Zarr
  array (`SegmentedBytes`, each value read from the chunk it lies in), or positional reads through a
  remote channel (`ChannelBytes`, over a block-cached HTTP range channel). Offsets are `long`, reads
  big-endian, and one array serves concurrent queries
- **Pure Java, through [Falcon](https://github.com/ebremer/falcon).** No native HDF5 library on any
  platform, the GraalVM native binary included
- **A store is found by what it is**, not by its name: a directory is a Zarr store, a file that begins
  with a ZIP header a Zarr archive, any other file HDF5. Tree walks (`-verify`, `-export`, LWS storage)
  take `*.h5` / `*.hdf5` and `*.zarr.zip` files and `*.zarr` directories, each as one store, and never
  walk into one
- **Replaced whole.** A store is built beside its destination — a `.tmp` file, a directory, a directory
  packed into an archive — forced to disk and moved into place complete
- **Why both:** HDF5 is one file to copy, publish and serve; a Zarr store opens in Zarr tools and can
  sit inside a larger Zarr hierarchy, and its ZIP form is one file again

---

[← Previous: File layout](../file-layout/){: .btn } [Next: Dictionary design →](../dictionary-design/){: .btn .btn-primary }
