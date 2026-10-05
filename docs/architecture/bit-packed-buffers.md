---
title: Bit-packed buffers
parent: Architecture
nav_order: 7
permalink: /architecture/bit-packed-buffers/
---

# The storage primitive: bit-packed unsigned buffers

- **Every integer dataset is the same primitive: `numEntries` values of `width` bits, packed
  MSB-first into big-endian bytes**
  - `width` is the minimum multiple of 8 that fits the value range (MinBits of the max id), so a
    store with 40M objects spends 4 bytes per object id, not 8
  - 1-bit width = the B bitmaps; 64-bit width carries full two's-complement longs
- **Byte-aligned widths are what make the parallel writers possible: entry i occupies exactly bytes
  [i\*w/8, (i+1)\*w/8) — concurrent positional writes never share a byte**
- **Readers address entries by long offsets (no 2 GiB ceiling) over jHDF-mapped, FFM-mapped, or
  HTTP-ranged bytes**
- **`binarySearch` / `lowerBound` / `upperBound` operate directly on the packed form with unsigned
  comparison**

---

[← Previous: Rank / select directories](../rank-select/){: .btn } [Next: Query path →](../query-path/){: .btn .btn-primary }
