---
title: What a BeakGraph store is
parent: Architecture
nav_order: 1
permalink: /architecture/what-a-beakgraph-file-is/
---

# What a BeakGraph store is

- **One store = one immutable, self-contained RDF dataset** (a default graph and named graphs)
  - Kept in one `.h5` file, a `.zarr` directory, or a `.zarr.zip` archive — the same arrays, byte
    for byte, in each ([Containers](../containers/))
  - Queried in place through Apache Jena — memory-mapped when local, by HTTP range requests when
    remote; the graph is never loaded into RAM
- **HDT-inspired design, generalized to quads**
  - Terms live once in sorted dictionaries, and a term's id is its rank: quads become tuples of
    integer ids
  - Two orderings of the id tuples (GSPO, GPOS) answer every access pattern
- **Everything is integers, packed tight**
  - Bitmaps, dictionary columns and value stores are bit-packed at the width their values need; the
    id sequences are block-structured (format v6): every block of about a thousand ids takes a base
    and a width of its own
- **A tiny container profile**
  - Groups, one-dimensional byte arrays and integer attributes, nothing else: no compression or
    filters at the container level, and an HDF5 dataset is contiguous, a Zarr array one chunk up to
    1 GiB — so readers read the bytes where they lie. Compression lives inside BeakGraph's own
    string encoding
  - Any HDF5 tool (HDFView, h5py) or Zarr v3 tool (zarr-python) opens a store as arrays of bytes;
    BeakGraph reads and writes both containers in pure Java, through
    [Falcon](https://github.com/ebremer/falcon)
- **Written once, atomically** — built beside the destination and moved into place complete;
  format version 6

---

[← Overview](../){: .btn } [Next: File layout →](../file-layout/){: .btn .btn-primary }
