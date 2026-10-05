---
title: Five writers, one format
parent: Architecture
nav_order: 9
permalink: /architecture/writers/
---

# Five writers, one format

`-method 0..4`: same file, different build machinery
{: .fs-6 .fw-300 }

| `-method` | Engine | Memory | Parallelism | Notes |
|---|---|---|---|---|
| 0 | sequential in-memory | whole input on heap | — | |
| 1 | huge: disk-based pipeline | bounded (spills) | — | |
| 2 | parallel in-memory | whole input on heap | `-cores` | |
| 3 | ultra in-memory | whole input on heap | `-cores` | packed-key radix sorts, O(1) id maps, parallel emission |
| 4 | hugeUltra: disk-based | bounded (spills) | `-cores` | background radix-sorted spills, grouped term runs, concurrent stages |

- Shared invariant: ids are `NodeComparator` ranks, so id-order == term-order — every engine
  provably emits the same index content
- Disk engines (1, 4): external merge sort of (term,row) records → dictionaries by merge-dedup → ids
  by sort-merge join → two id-quad sorts → streamed HDF5. RAM bounded by spill batch sizes at ANY
  quad count
- In-memory outputs are structurally identical per source; disk outputs are isomorphic (rank-derived
  bnode labels)
- All engines publish atomically: build to sibling `.tmp`, rename over destination

---

[← Previous: Query path](../query-path/){: .btn } [Back to the overview](../){: .btn .btn-primary }
