---
title: Rank / select directories
parent: Architecture
nav_order: 7
permalink: /architecture/rank-select/
---

# Rank / select directories (SB, BB)

O(log n) `select1` over the B bitmaps — since format version 3
{: .fs-6 .fw-300 }

- **Each B bitmap carries a two-tier directory, sized in the build scan:**
  - `SB[j]` = total 1-bits in the first j superblocks (SUPERBLOCK = 512 bits); `SB[0]` = 0
  - `BB[k]` = 1-bits between block k's superblock start and block k (BLOCK = 64 bits = one machine
    word); `BB[k]` = 0 when k opens its superblock
- **`select1(rank)`: binary-search SB, then BB, then one broadword scan inside a single 64-bit word**
- **SB is copied into memory at a level's first `select1`** (8 bytes per 512 bits of bitmap; past
  2^24 entries it is read where it lies), so finding the superblock touches no file: what is left is
  one `BB` read and one bitmap word
- **A run's end is the next set bit after its start**: a short forward word scan finds it, and the
  directory is consulted only for long runs such as a large graph's subject run (`RangeSelect`)
- **BLOCK = word size means writers can derive the whole directory from word popcount prefix sums —
  and readers never disagree with the linear fallback**
- **Files older than format version 3 have an incompatible directory: readers detect the version
  and fall back to a linear `select1` scan (slower, still correct)**

---

[← Previous: GSPO / GPOS indexes](../indexes/){: .btn } [Next: Integer sequences →](../bit-packed-buffers/){: .btn .btn-primary }
