---
title: Six writers, one format
parent: Architecture
nav_order: 10
permalink: /architecture/writers/
---

# Six writers, one format

`-method 0..5`: same store, different build machinery
{: .fs-6 .fw-300 }

| `-method` | Engine | Memory | Parallelism | Notes |
|---|---|---|---|---|
| 0 | sequential in-memory | whole input on heap | — | |
| 1 | huge: disk-based pipeline | bounded (spills) | — | |
| 2 | parallel in-memory | whole input on heap | `-cores` | |
| 3 | ultra in-memory | whole input on heap | `-cores` | packed-key radix sorts, O(1) id maps, parallel emission |
| 4 | hugeUltra: disk-based | bounded (spills) | `-cores` | background radix-sorted spills, grouped term runs, concurrent stages |
| 5 | plaid: disk-based | bounded (spills) | `-cores` | method 4 plus parallel multi-file ingest: up to `-cores` documents parse at once |

- Shared invariant: ids are `NodeComparator` ranks, so id-order == term-order — every engine
  provably emits the same index content
- Shared code where hand-mirrored copies once diverged: one parser setup (`RdfSources.parser`), one
  relative-IRI rule (`RelativeIris`), one spatial augmenter, one comparator, one id-sequence encoder
  (`BlockedEncoder`)
- Disk engines (1, 4, 5): external merge sort of (term,row) records → one merge of the sorted
  columns into the distinct dictionaries and every row's ids → two id-quad sorts → the store written
  a piece of an array at a time. Engines 4 and 5 run that merge a range of the term order at a time,
  as many ranges at once as `-cores`, and decode a dictionary's terms and compress its strings ahead
  of the thread that encodes it. RAM bounded by spill batches (records and bytes) at ANY quad count,
  JSON-LD documents excepted (they are not streamed)
- In-memory outputs are byte-identical per source (0, 2, 3); disk outputs are isomorphic
  (rank-derived bnode labels)
- Every engine writes both containers (`-zarr`, `-zip`, or a destination named `*.zarr` / `*.zip`)
  through the same `WritableStore` calls, and through Falcon: no native HDF5 library
- All engines publish atomically: build beside the destination (a `.tmp` file, or a directory for
  Zarr, packed into an archive for ZIP), force it to disk, move it into place; a failed build leaves
  the previous store untouched

---

[← Previous: Query path](../query-path/){: .btn } [Back to the overview](../){: .btn .btn-primary }
