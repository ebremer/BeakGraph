---
title: Query path
parent: Architecture
nav_order: 9
permalink: /architecture/query-path/
---

# Query path

From SPARQL to matching quads, on ids
{: .fs-6 .fw-300 }

For a pattern `(g, s, p, ?)`:

1. Locate ids in the dictionaries (binary search, FCD-decoded)
2. Pick the index: `s` bound? GSPO : GPOS (only `o` bound: one GPOS probe per predicate of the graph)
3. Level 1: `select1(Bs, g)` delimits g's subjects; binary-search s
4. Level 2: `select1(Bp, ...)` delimits (g,s)'s predicates; binary-search p
5. Level 3: `select1(Bo, ...)` delimits (g,s,p)'s objects; stream them
6. `extract(id)` turns an id back into a term only where a row needs one (the node table: pages of
   16 consecutive ids for id → term, a Caffeine cache for term → id)

- **Rows stay ids.** `OpExecutorBG` takes over basic graph patterns, bare or under `FILTER`: it
  reorders them by the VoID statistics (a fixed heuristic without them), places the filters, and
  joins on ids. Range filters are pushed into the runs as id bounds, widened to whole value-equal
  clusters (`ValueCluster`)
- **`GRAPH ?g { ... }`** over patterns, `FILTER`, `OPTIONAL` and `UNION` is one walk over the named
  graphs on ids (`GraphVarBG`), handed to parallel workers once it has produced 65,536 rows; other
  blocks run graph by graph through Jena's operator
- **Aggregates on ids.** `COUNT`s of every quad — whole, or by graph or predicate — and
  `DISTINCT ?p` / `?s` are read off the index levels; any other `GROUP BY`, `COUNT`, `SUM`, `AVG`,
  `MIN` or `MAX` over patterns the engine reads is computed on ids by the threads that read the rows
  (`GroupOnIds`)
- **Parallel scans.** A scan-shaped first pattern (`?s ?p ?o`, `?s <p> ?o`) of at least 65,536
  positions is cut into position chunks read on virtual threads (`ScanChunks`, `ParallelScan`); a
  store read over HTTP is read on one thread
- Every step is a handful of range reads over packed arrays — ideal for memory maps and HTTP range
  requests alike
- RDF 1.2 triple-term patterns with variables are unified on ids against `tripleTerms`
  (`TripleTermMatcher`)
- A concrete RDF-array sequence, such as `?s :p ( 1 2 3 )`, is a constant like any other: it is
  located on ids
- A basic graph pattern holding a sequence pattern (a variable, gap or nested pattern inside
  `( … )`) goes whole to jena-array's solver over the store's `find`, in written order, with its
  filters after it: a variable at an empty slot stays unbound, which the id-level planner does not
  know yet. `UNNEST`, `SEQLEN`, indexes and slices are Jena's, over lazily read stored sequences
  (`RDFARRY.md` §3.7)
- Optional spatial layer: geometries are Hilbert-indexed into cell ids stored as ordinary quads in
  graph `urn:x-beakgraph:Spatial`; `geof:sfIntersects` takes its candidates from GPOS range scans over
  the query region's cover and verifies each with JTS, so results are exact

---

[← Previous: Integer sequences](../bit-packed-buffers/){: .btn } [Next: Six writers, one format →](../writers/){: .btn .btn-primary }
