---
title: Query path
parent: Architecture
nav_order: 8
permalink: /architecture/query-path/
---

# Query path

From SPARQL triple pattern to matching quads
{: .fs-6 .fw-300 }

For a pattern `(g, s, p, ?)`:

1. Locate ids in the dictionaries (binary search, FCD-decoded)
2. Pick the index: `s` bound? GSPO : GPOS
3. Level 1: `select1(Bs, g-rank)` delimits g's subjects; binary-search s
4. Level 2: `select1(Bp, ...)` delimits (g,s)'s predicates; binary-search p
5. Level 3: `select1(Bo, ...)` delimits (g,s,p)'s objects; stream them
6. `extract(id)` turns each id back into a term (rank lookup)

- Every step is a handful of range reads over packed buffers — ideal for memory maps and HTTP range
  requests alike
- Value-ordered literal ids make range filters (x > 5) pushdown-able as contiguous id ranges
  (ValueCluster)
- Optional spatial layer: geometries are Hilbert-indexed into cell ids stored as ordinary quads in
  graph `urn:x-beakgraph:Spatial`, so spatial queries ride the same index machinery

---

[← Previous: Bit-packed buffers](../bit-packed-buffers/){: .btn } [Next: Five writers, one format →](../writers/){: .btn .btn-primary }
