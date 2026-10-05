---
title: What a BeakGraph file is
parent: Architecture
nav_order: 1
permalink: /architecture/what-a-beakgraph-file-is/
---

# What a BeakGraph file is

- **One `.h5` file = one immutable, self-contained RDF quad store**
  - Queried in place through Apache Jena — locally memory-mapped or remotely via HTTP range
    requests; the graph is never loaded into RAM
- **HDT-inspired design, generalized to quads (named graphs)**
  - Terms live once in dictionaries; triples/quads become tuples of integer ids
  - Two orderings of the id tuples (GSPO, GPOS) answer all access patterns
- **Everything is bit-packed**
  - Every integer sequence is stored at the minimum byte-aligned width its value range needs, as
    raw big-endian bit-packed buffers
- **HDF5 as the container**
  - Groups/datasets/attributes give the file self-describing structure, tooling (h5dump, HDFView),
    and partial-read access for free
- **Written once, atomically (build to `.tmp`, rename); format version 3**

---

[← Overview](../){: .btn } [Next: File layout →](../file-layout/){: .btn .btn-primary }
