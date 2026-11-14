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

Status: see "Progress" at the end. What was built differs from the table in one way: an array is one
uncompressed chunk, not a shard of block-sized chunks.

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

### Item 2: one format, two containers (2026-11-14, not committed when written)

Done, and described in SPECIFICATIONS.md §3a and docs/INSTRUCTIONS.md §4a:

- The store is defined apart from its container: `com.ebremer.beakgraph.store` (`OpenStore`, `StoreGroup`,
  `StoreArray` to read; `WritableStore`, `WritableGroup`, `WritableDataset` to write), with an HDF5 and a
  Zarr implementation, both on Falcon. Readers, writers and the query engine do not know which they have.
- All six engines write either container, and the native binary too. `-zarr`, or a destination named
  `*.zarr`, chooses Zarr; every command reads either.
- The two containers hold the same store: the same groups, the same arrays byte for byte, the same
  attributes. A test compares them through Falcon's general HDF5 and Zarr readers, for every engine.
- Falcon gained the view this needed: `ZarrArray.chunkSegment` (a raw chunk as a mapped, read-only
  segment), on `Store.map`, which `FileSystemStore` implements.

Where it differs from the proposal above, and why:

- **An array is one uncompressed chunk, not a shard of block-sized chunks.** The blocks of format v6 vary
  in size, so a Zarr chunk cannot be a block; and a shard's index would add nothing BeakGraph reads, while
  the reader would have to check it before trusting the layout. One chunk stored with the `bytes` codec
  alone is the Zarr counterpart of an HDF5 contiguous dataset: its file is the array, and metadata alone
  says so.
- **Falcon holds a chunk in one buffer of under 2 GiB**, so an array past 1 GiB is cut into several
  chunks. Each is mapped, but the array is then read through its mappings a range at a time, which is
  slower. One contiguous mapping per array is what keeps the read path the same class as HDF5's.
- **A Zarr reader maps every array when it opens a store.** A store is many files; mapped up front, a store
  replaced underneath an open reader is still read whole, and a missing file cannot later read as zeros.

Measured on the 4.0M-quad store, HDF5 file against Zarr store, same machine, same jar:

| What | HDF5 | Zarr |
|---|---|---|
| On disk | 62,088,673 bytes, 1 file | 62,113,759 bytes, 126 files |
| Build, `-method 3` / `-method 4` | 10.8 s / 17.8 s | 11.8 s / 17.8 s |
| Queries in process (six phases, three alternating runs) | - | -12% to +6%, within the spread of runs; same answers |
| `-verify -deep` / `-export NQ` | 6.93 s / 3.78 s | 6.88 s / 3.52 s |
| Open a store in a warm JVM | 0.4 ms (1.8 ms as written, see below) | 6.5 ms |
| First query after the open | 2.9 ms (17.7 ms as written) | 3.0 ms |
| Close | 0.8 ms | 1.4 ms |
| Open in a fresh JVM, to the first answer | 1.02 s | 1.04 s |

What that says:

- **Same answers at the same speed, as designed**: the readers work on the same mapped bytes.
- **Opening costs about 6 ms more**: 57 files are mapped instead of one, and one JSON document is parsed.
  A pool opens a store once, so this is paid per pooled reader, not per query.
- **A build costs about a second more** with `-method 3` here: 126 files are created and forced to disk
  instead of one (forcing 126 copies of them took 0.53 s, the one file 0.03 s).
- The native binary writes the same Zarr store as the JVM, file for file, with every engine, and
  verifies, exports and serves it. Its timings were not taken.

Found on the way, in the HDF5 container, and not changed:

- **Falcon writes its HDF5 files as sparse files, and on Windows a mapped sparse file is slow to touch.**
  Falcon's writer opens its output with `StandardOpenOption.SPARSE`. On this machine the first touch of a
  page of a mapped sparse file costs about 150 microseconds, against about 1 for a file that is not
  sparse: 8.4 ms against 0.07 ms for 50 pages spread over the 62 MB store, measured with a plain JDK
  mapping. A byte-identical copy of the file is fast; marking that copy sparse makes it slow, and clearing
  the mark does not make it fast again. So every HDF5 store written since the move to Falcon pays this once
  per page per open on Windows: the first query after an open takes 17.7 ms where a copy of the file takes
  2.9 ms, and an export about 0.2 s longer. Steady-state queries are not affected. The "as written" figures
  above are this. Not writing sparse files (in Falcon), at least on Windows, would remove it; that is
  Falcon's decision, since a dataset created ahead of its data is cheaper to lay out in a sparse file.
  Linux and macOS were not measured.

Still open under item 2:

- Zarr stores over HTTP and from object stores, through the endpoint and the pool. `ZarrStore.open(store,
  name)` reads from any Falcon `Store` a range at a time, but with no cache, so every read is a request.
- Zarr stores inside an LWS storage directory are listed as directories, not served as stores.
- A single-file form: a ZIP archive with uncompressed entries is a Zarr store Falcon reads, and could be
  mapped as one file; Falcon's `ZipStore` does not map yet, and a store takes a value only as one array,
  so an array larger than the heap cannot be written into one.
- Arrays past 1 GiB: read through one view per array, as in HDF5.
- Not checked against zarr-python, which is not installed here: Falcon's general Zarr reader reads every
  array of a store, and that reader is tested against zarr-python in Falcon.

Still open under item 1:

- Use the bulk decode (`get(index, long[], ...)`, a block into an array from whole words) in the scan
  paths: the full-scan iterator, export and the deep verify. Small on the JVM; the native binary's way out.
- The dictionary's other arrays (`offsets`, the typed value stores) are still single-width.
- Lay the directories out together (or load them lazily) for remote opens.
- Query times on a real store with its named graphs, rebuilt in the current format.
