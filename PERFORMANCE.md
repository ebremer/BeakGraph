# Performance: where BeakGraph can get much faster

Design changes proposed on 2026-11-14, after BeakGraph moved from jHDF and the native HDF5 library to
[Falcon](https://github.com/ebremer/falcon). Old stores are out of scope: these changes may break the
on-disk format. The measurements quoted are from one Windows machine (Ryzen 9 5950X, GraalVM CE 25), a
4.0M-quad synthetic store with 5,000 named graphs, and a 1.79 GB format-v2 store of real data ("TCGA").

The one structural change most of the speed gains - and the Zarr option - depend on: make the **block**,
not the single entry, the unit BeakGraph stores, fetches and decodes.

## 1. Block-structured arrays with an in-memory directory

- **Today:** each index level is one long bit-packed array with a single width for all of it, read one
  entry at a time through an interface call. Finding a run (`select1`) searches a rank directory that lies
  in the file, several scattered probes per lookup, and a search inside a long run probes across the whole
  run. Over HTTP each cold probe can be a range request.
- **Proposed:** cut every array into blocks of a few thousand entries, each delta- and bit-packed with its
  own width. Keep a small directory (first value and offset per block) on the heap, loaded at open.
- **What it buys:**
  - A lookup becomes a search in the directory plus one block read.
  - A block is decoded into a `long[]` in one pass, with no per-entry interface call. That should remove
    three measured penalties: the native binary's 30-60% on reads, the 2x slowdown when local and remote
    stores share a JVM, and the trade-off between reading through the memory segment and through a
    ByteBuffer view of it (`beakgraph.ffm.threshold`).
  - Per-block widths shrink the index arrays; sampled with a general compressor, the index arrays of real
    stores came down to between 1% and 60% of their size.

Status: see "Progress" at the end.

## 2. One format, two containers

Define the store as named byte arrays, integer attributes and block directories, independent of the
container.

| | HDF5 | Zarr v3 |
|---|---|---|
| A BeakGraph array | one contiguous dataset, blocks at computed offsets | one array, chunk = block, sharded |
| Local read | memory-mapped whole, as now | map the shard file |
| Remote read | one range per block | one chunk per block |
| Open | as now | consolidated metadata, one request |

- Searched blocks stay uncompressed; zstd only on cold payloads such as strings. Generic compression of
  the searched arrays measured 10x to 1,000x slower.
- `HdfProfile` and `WritableHdfFile` are already the only doors to the container, so this is one interface
  with a Falcon HDF5 and a Falcon Zarr implementation.
- Falcon would need one addition: its Zarr stores return byte arrays today, so mapped local reads need a
  zero-copy view of a chunk or shard.

## 3. Fix the index shape

- **Dense graph ids.** Level 0 is the whole entity id space, and every entity that is not a graph gets a
  dummy row at each level. In the 4.0M-quad store that is 800,004 of the 4,805,004 object-level rows and
  nearly all of the first level.
- **An optional graph-last order (such as SPOG).** A pattern without a fixed graph loops over every graph
  today: 1,000 subject lookups across 5,000 graphs took 14 s. A subject-first index answers each in one
  lookup, at the cost of store size and build time.

## 4. Run queries on ids, in batches

Block decoding can feed batch iterators, merge joins and late term lookup. More aggregates could also come
straight from index counts: the count fast path covers `?s ?p ?o` in one graph, but `GRAPH ?g { ... }`
still walks every quad (0.48 s for 4.0M). This part is engine work, not Falcon.

## 5. Binary geometry instead of WKT text

Geometries and their pyramid levels are stored as WKT strings, and the exact `sfIntersects` test parses the
text. Strings were 37% of the TCGA store. Delta-coded coordinate arrays with bounding-box columns would be
smaller and skip the parsing. Not measured, and it needs a decision: SPECIFICATIONS.md §10.2 requires the
original WKT verbatim.

## Where "much faster" would come from

- **Cross-graph queries:** orders of magnitude, from item 3.
- **Cold and remote lookups:** several times, from item 1.
- **Native binary:** back to at least its speed before Falcon, from item 1.
- **Scans and joins:** only with item 4.

These are expectations from the measurements, not results.

## Progress

### Item 1, first step: format v6 (2026-11-14, not committed when written)

Done, and described in SPECIFICATIONS.md §5.1a:

- The id sequences of both indexes (`Ss`, `Sp`, `So`) and the dictionary's role lists (`graphs`,
  `subjects`, `objects`) are block-structured: 1,024 entries a block, each block with its own base and
  width, and a directory dataset (`<name>_blocks`) the reader keeps on the heap.
- A search inside a sorted run that spans whole blocks is narrowed to one block from the directory, without
  reading the file.
- The rank/select directory's superblock counters are copied to the heap at a level's first lookup.
- One encoder (`BlockedEncoder`) serves all six engines, which still write the same bytes for the same
  input. Stores of format v3-v5 still read; earlier builds refuse a v6 store.

Measured against the commit before (`3c7d6cb`), same machine:

| What | Before | Format v6 |
|---|---|---|
| 4.0M-quad synthetic store | 76.3 MB | 62.1 MB (-19%) |
| TCGA source data rebuilt, 10.9M quads | 792 MB | 639 MB (-19%) |
| ... its two index groups | 327 MB | 203 MB (-38%) |
| JMH `directorySelect1` | 73.1 ns | 53.6 ns (-27%) |
| JMH `adjacentPair` (a run's two ends) | 132.8 ns | 85.4 ns (-36%) |
| JMH `chainJoin` / `pointLookup` / `starJoin` | 78.1 / 2.89 / 4.88 us | 65.4 / 2.64 / 4.42 us (-16% / -9% / -9%) |
| JMH scans (`fullScanFind`, `fullScanSparql`, ...) | - | -3% to +11%, about the spread between runs |
| Queries on the 4.0M-quad store, in process | - | within 6%, most within 3% |
| Build time, 4.0M quads / TCGA | 9.8 s / 156 s | 9.8 s / 155 s |
| Remote, 160-query battery on the 4.0M-quad store | 273 requests, 41.3 MB | 241 requests, 39.1 MB |
| Remote, opening that store | 2 requests, 0.25 MB | 5 requests, 0.62 MB |
| Native binary, `-verify -deep` / `-export NQ` | 39.8 s / 7.3 s | 40.1 s / 7.3 s |

What that says:

- **Smaller, and faster to look things up; scans unchanged.** A profile of the query workload shows why:
  fetching ids is about 6% of a scan's time on the JVM, and the rest is Jena's binding machinery. The
  storage form cannot make scans much faster by itself; item 4 can.
- **Opening a store over HTTP costs three more requests**, because the nine directories are read at open.
  They are small (0.4 MB together here) and could be laid out together, or read on first use.
- **The native binary is where it was.** Its read penalty comes from reading through the memory segment an
  entry at a time, and the scan paths still do that.
- The TCGA figure is for its source quads alone: the export drops the derived spatial graphs and the
  rebuild did not ask for them again, so it says nothing about query time on that data.

Still open under item 1:

- Use the bulk decode (`get(index, long[], ...)`, a block into an array from whole words) in the scan
  paths: the full-scan iterator, export and the deep verify. Small on the JVM; the native binary's way out.
- The dictionary's other arrays (`offsets`, the typed value stores) are still single-width.
- Lay the directories out together (or load them lazily) for remote opens.
- Query times on a real store with its named graphs, rebuilt in the current format.
