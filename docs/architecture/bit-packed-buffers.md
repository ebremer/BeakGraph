---
title: Integer sequences
parent: Architecture
nav_order: 8
permalink: /architecture/bit-packed-buffers/
---

# The storage primitives: bit-packed and block-structured sequences

- **Bit-packed: `numEntries` values of `width` bits, packed MSB-first into big-endian bytes**
  - Widths 1–57 or exactly 64, set per array from its value range: 1 bit for the B bitmaps, 5 for
    `datatypes`, `1 + MinBits(n)` for `offsets`, whole bytes for the rank/select directories; widths
    32 and 64 carry two's-complement ints and longs
  - Entry i is one unaligned 64-bit read at byte `(i·w) >> 3`, a shift and a mask
  - Used for the bitmaps, the dictionary columns and value stores, and the rank/select directories
- **Block-structured (format v6): the id sequences** — `S<c>` of both indexes and the `graphs` /
  `subjects` / `objects` lists
  - Blocks of 1,024 entries (× 4 until a sequence has at most 2^22 blocks); a block stores each
    entry less the block's smallest (its base) in the bits their spread needs — 0 to 57, or 64 for the
    entries themselves — padded to whole 64-bit words; 8 zero bytes close the array
  - The directory `<name>_blocks` is 9 bytes a block (base, then width) and the reader holds it in
    memory: an entry is still one read, and a run of nearby ids costs the bits of their spread, not
    of the largest id in the store
  - `binarySearch` / `lowerBound` / `upperBound` inside a long sorted run narrow to one block by the
    block bases before touching the data
- **Writers build at byte-aligned widths, then encode.** In memory an id sequence is held at a
  multiple of 8 bits, so entry i occupies exactly bytes `[i·w/8, (i+1)·w/8)` and concurrent
  positional writes never share a byte — what lets `-method 3` emit an index in parallel chunks.
  `BlockedEncoder` then lays out and writes the blocks in two passes: one encoder for every engine,
  so the same sequence is the same bytes whichever engine built the store
- **Readers address entries by `long` offsets (no 2 GiB ceiling) over memory-mapped or HTTP-ranged
  bytes, with unsigned comparison throughout**

---

[← Previous: Rank / select directories](../rank-select/){: .btn } [Next: Query path →](../query-path/){: .btn .btn-primary }
