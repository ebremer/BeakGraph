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

Found on the way, in the HDF5 container (fixed since: see "After the speed test" at the end):

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

- Done since (see "Zarr: one file, over HTTP, in a storage directory, past one chunk" at the end): Zarr
  stores over HTTP through a cache, Zarr stores served as stores from an LWS storage directory, the
  single-file form, and arrays of several chunks read where their values lie.
- Not checked against zarr-python, which is not installed here: Falcon's general Zarr reader reads every
  array of a store, and that reader is tested against zarr-python in Falcon. (Python's own `zipfile`
  reads the single-file form and finds every CRC right.)

Still open under item 1:

- Use the bulk decode (`get(index, long[], ...)`, a block into an array from whole words) in the scan
  paths: the full-scan iterator, export and the deep verify. Small on the JVM. The native binary no longer
  waits for it: it reads through the ByteBuffer view now (see "After the speed test"), and the bulk decode
  would make the choice of view not matter.
- The dictionary's other arrays (`offsets`, the typed value stores) are still single-width.
- Lay the directories out together (or load them lazily) for remote opens.
- Query times on a real store with its named graphs, rebuilt in the current format.

### After the speed test: sparse files and the native binary's view (2026-11-14, not committed when written)

SPEEDTEST.md compared this branch with `master` and found two places where it had lost ground. Both are
fixed:

- **Falcon no longer writes sparse files** (Falcon's hdf5/TODO.md, P2 WF12): its writer no longer opens
  its output with `StandardOpenOption.SPARSE`. Nothing changes in BeakGraph, and no byte of a store
  changes. A store Falcon wrote before this stays slow to open on Windows until it is copied (a copy, not
  a move: the mark travels with the file).
- **The native binary reads mapped arrays through a ByteBuffer view of their segment**, as it did before
  the move to Falcon: `beakgraph.ffm.threshold` defaults to 2147483647 in a native image and stays 0 on a
  JVM (`DatasetBytes`). Arrays past 2 GiB are read through the segment as before.

Measured on the 4.0M-quad store, each configuration on the store it built: `master` (33febe9), this branch
before (bf09ba1, Falcon 0.2.0 as it was) and after.

| What | master | before | after |
|---|---|---|---|
| HDF5 file marked sparse | no | yes | no |
| Warm JVM: open, first query, close | 4.4 ms | 48.4 ms (3.8 ms for a plain copy) | 4.0 ms |
| ... the first query alone | 2.8 ms | 44.6 ms | 2.9 ms |
| Fresh JVM: first query | 106 ms | 173 ms | 113 ms |
| Build, `-method 3` / `-method 4` | 9.9 s / - | 10.8 s / 17.9 s | 10.8 s / 17.8 s |
| ... the container step of `-method 3` | 0.08 s | 0.41 s | 0.43 s |
| Native `-verify -deep` | 29.5 s | 41.1 s | 30.8 s |
| Native `-export NQ` | 5.50 s | 7.52 s | 5.64 s |
| Native endpoint: count of every quad | 1.35 s | 2.18 s | 1.39 s |
| Native endpoint: quads grouped by predicate | 2.55 s | 3.31 s | 2.66 s |
| Native endpoint: 1,000 subject lookups, graph not given | 24.4 s | 36.9 s | 24.4 s |
| Native endpoint: 200 graphs dumped | 4.08 s | 4.56 s | 4.23 s |
| Native endpoint: 100 two-hop joins | 0.98 s | 1.04 s | 0.96 s |

What that says:

- **Opening an HDF5 store costs what it did on `master`.** The file written now opens like a plain copy
  of the old one, and writing it takes no longer, with the in-memory and the disk engines alike.
- **How slow a sparse file is varies from file to file**: the first query after an open took 18.9 ms on
  the store of the speed test and 44.6 ms on this one, written by the same code from the same source.
- **The native binary is back within 5% of `master`** on everything it had lost: the deep verify, the
  export, and the endpoint's counts and cross-graph lookups. Forced back to the segment
  (`-Dbeakgraph.ffm.threshold=0`) on the new store, it takes 37.4 s and 7.23 s again. A Zarr store reads
  the same: 29.6 s and 5.70 s.
- **The speed test's "falcon2 is 30-40% faster at native dumps and joins" did not hold up.** The same
  binary took 4.6 s and 1.0 s for them here, where it had taken 2.6 s and 0.5 s; `master` took about 4.1 s
  and 0.9 s both times. The rounds of those two phases spread widely (3.5 s to 4.7 s for the dumps in a
  later check), and the collector was ruled out: the same number of collections, and the same times with
  a 1 GiB young generation. What made the earlier run faster was not found. Read the two as equal.
- Not measured again: real data, Linux, macOS.

### The endpoint's response buffer (2026-11-14, not committed when written)

A profile of the endpoint (Flight Recorder, JVM, the 4.0M-quad store, one client on a connection kept
alive) showed where a small query's time went: not in the query. Fuseki sets Jetty's output buffer to
5 MiB, Jetty's buffer pool keeps no buffer past 64 KiB, and so every response had a 5 MiB direct buffer
allocated, zero-filled and freed for it. That was 92% of the server's CPU in an `ASK { }`, 72% in a two-hop
join, 25% in a dump of 801 rows; a fresh 5 MiB direct buffer measured 0.67 ms by itself.

Done: the endpoint sets the buffer to 64 KiB, the largest the pool keeps
(`SPARQLEndPoint.sizeOutputBuffers`, `beakgraph.http.output.buffer`). Per request, in milliseconds (median
of 1,000 requests, two servers each):

| Query | JVM before | JVM after | Native before | Native after |
|---|---|---|---|---|
| `ASK { }` | 0.93 | 0.26 | 0.93 | 0.23 |
| Lookup of a subject in one graph | 1.05 | 0.35 | 1.13 | 0.36 |
| Two-hop join in one graph | 1.21 | 0.53 | 1.33 | 0.67 |
| Dump of one graph (801 rows, 239 KB of XML) | 3.62 | 2.88 | 5.90 | 5.15 |
| Lookup of a subject, graph not given (5,000 graphs) | 7.5 | 6.4 | 16.0 | 14.5 |

What that says, and what it costs:

- **Every request is about 0.7 ms shorter**, which is most of a small one.
- **Serving files is unchanged** (directory mode): the 62 MB store whole in 0.17 s before and after, a
  64 KiB byte range in 3.1 ms before and after.
- **A response is on the wire sooner**: once 64 KiB of it are written, where it was 5 MiB. A failure that
  surfaces later than that ends in an aborted transfer, not a status code. One within the first 64 KiB is
  answered with its status as before, and the buffer is never set under the 16 KiB the service holds back.
- **How these were measured matters.** The endpoint tables of SPEEDTEST.md, and of "After the speed test"
  above, were taken with a new connection for each request. On this machine that adds a median 0.8 ms and
  stalls of about 15 ms on a tenth to a half of the requests: a dump took 14 to 22 ms there and takes
  3.6 ms on a connection kept alive. That is the likely reason the native binary's dumps and joins did not
  reproduce between runs. Whose side the stall is on was not found.

Seen in the same profiles, and not changed (but for the first, done since: see the next section):

- **`GRAPH ?g { ... }` runs through Jena**, which builds an iterator pipeline for each graph: 88% of a
  subject lookup without a graph, 58% of it building those pipelines; and 45% of the count and group-by
  workload. The executor has no case for a variable graph.
- **Writing the result as XML is 55% of a dump.** The endpoint answers JSON, CSV and XML: a client asking
  for tab-separated values gets XML.
- **The node cache (Caffeine) is 14% of a subject lookup without a graph, 5% of a dump and about half of
  `-verify -deep`**, whose 9.9 s of wall time took 52.8 s of CPU. (Replaced since for the terms of ids: see
  "The terms by id".)
- **`BindingNodeId.get` is 12.7% of the scans**: every row asks again whether each variable is bound.
- **A 64 KiB byte range of a file takes 3.1 ms** in directory mode, twelve times an `ASK`; the remote
  battery's 241 range requests take 0.6 s in all.
- **A `-method 3` build spends 38% of its CPU comparing terms** for the dictionary's order.

### Graph variables on ids, and counts from the index (2026-11-14, not committed when written)

Two changes to the query engine, for the queries that go over every named graph. Neither touches the
store's format.

- **`GRAPH ?g { BGP }` is one walk over the named graphs on ids** (`GraphVarBG`, from the executor's new
  case for a variable graph). Jena executed the pattern anew in every graph: the binding substituted into
  the algebra, a graph view, an execution context, an iterator pipeline, and the graph's name assigned to
  each row afterwards. The walk binds `?g` to a graph's id and reads the pattern with the id-level
  iterators, one child binding and one iterator a graph.
  - **What it takes:** a variable graph over a basic graph pattern, in the store's own dataset. A block
    with a `FILTER`, `OPTIONAL` or `UNION` inside it, a `FROM NAMED` dataset, and a triple term with
    variables run as before, and so does an input row that binds `?g` already: Jena's own rules for a
    single graph apply to it.
  - **A filter inside the block is why the shape is that narrow:** SPARQL joins the graph's name to the
    pattern's solutions afterwards, so `GRAPH ?g { ... FILTER(?g = <x>) }` does not see `?g`. Binding it
    first is the same thing for a pattern, and not for a filter.
  - **A large graph on the way is still scanned in parallel**, under the conditions of before.
- **Counts over every named graph, and counts by graph or by predicate, come from the index**
  (`AggregateCountFastPath`, `IndexCounts`). The fast path answered a count in one graph with no group
  keys. It now also answers `COUNT(*)` (and `COUNT(?x)`, `COUNT(DISTINCT *)`, `COUNT(DISTINCT ?g)`,
  `COUNT(DISTINCT ?p)`) over `GRAPH ?g { ?s ?p ?o }`, and `GROUP BY ?p`, `GROUP BY ?g` and
  `GROUP BY ?g ?p`. A predicate's quads in a graph are the distance between where its rows begin in GPOS
  and where the next predicate's do: two `select1` calls a predicate, and no row read.
- **The index reader's lookups by component letter are array loads** (`IndexReader`). They were maps
  keyed by a boxed `Character`, asked nine times for every iterator built: a fifth of the walk's time once
  Jena's part of it was gone.

Measured at the endpoint (JVM, one client on a connection kept alive, a 4.0M-quad store of 5,000 graphs
of 801 quads built for this; each build on two servers in turn), in milliseconds a request:

| Query | Before | After |
|---|---|---|
| Subject lookup, graph not given | 7.0 | 2.2 |
| Subject and predicate, graph not given | 6.0 | 1.5 |
| Two-hop join from a subject, graph not given | 9.9 | 1.7 |
| Object lookup (a literal), graph not given | 7.6 | 2.1 |
| The rows of a type in every graph, counted (200,000) | 42 | 28 |
| The rows of a predicate in every graph, counted (800,000) | 192 | 155 |
| Count of every quad | 642 | 3.1 |
| Named graphs counted (`COUNT(DISTINCT ?g)`) | 1,116 | 2.4 |
| Quads grouped by predicate | 1,145 | 6.6 |
| Quads grouped by graph (5,000 rows) | 680 | 18 |
| Quads grouped by graph and predicate (30,000 rows) | 1,341 | 114 |
| Quads of one graph grouped by predicate | 0.42 | 0.27 |
| Subject lookup in one graph (not changed) | 0.32 | 0.30 |
| Count of one graph (not changed) | 0.30 | 0.30 |
| Dump of one graph, 801 rows (not changed) | 2.7 | 2.8 |

What that says:

- **A lookup without a graph is three to six times faster**, and what is left of it is the work itself: a
  profile of the walk has the search of each graph's subjects and the select that finds them at 36%, the
  bindings at about 10%. Making it much faster again takes an index that leads with the term (item 3).
- **A scan across graphs gains less**, 20 to 35%: there the rows cost more than the graphs do.
- **The counts no longer depend on the number of quads**, only on the number of graphs and predicates.
- **Answers are the same**: every query of the table gave the same rows from both builds, and the tests
  hold the walk to Jena's operator and to an in-memory dataset on 51 query shapes.

Since then, on branch `falcon3`: a block with a `FILTER`, `OPTIONAL` or `UNION` inside it is walked too, and
a walk whose rows do work goes parallel (see "Blocks, rows and workers" below).

Found on the way, and not changed:

- **CSV results cost ten times what XML results do at the endpoint**, in both builds: a graph's 801 rows
  take 31 ms as CSV, 2.7 ms as XML and 3.6 ms as JSON; 30,000 grouped rows 1.66 s, 0.11 s and 0.15 s.
  Jena's CSV writer flushes once, at the end, so the cause is in how its writes reach the response; it was
  not looked into further. The two grouped rows of the table above are XML timings for that reason.

### Blocks, rows and workers (2026-11-15, branch `falcon3`, not committed when written)

Three more changes to the query engine, all in how a query goes over the rows of many graphs. The store's
format is untouched.

- **A `GRAPH ?g` block with a `FILTER`, `OPTIONAL` or `UNION` inside it is walked on ids** (`GraphVarBG`).
  The walk took a plain pattern; any other block ran graph by graph through Jena. It now evaluates, in each
  graph, the operators Jena itself runs by handing each row of the left to the right: patterns, filters,
  sequences, conditionals (the optional ARQ can run row by row) and unions, and what ARQ makes of
  `FILTER(?x = <iri>)` (an assignment) and of `FILTER(a || b)` (a disjunction). Each filter is placed after
  the triples that bind its variables, and is its pattern's range hints, as for a pattern outside a graph
  block.
  - **What keeps the answers the same:** SPARQL joins the graph's name to the block's solutions afterwards,
    so nothing inside the block may see `?g`. The walk binds it first; it therefore takes such a block only
    when nothing in it mentions `?g`. (A plain pattern may: for a pattern, binding it first is that join.)
  - **Still Jena's:** `MINUS`, `BIND`, a sub-select, a property path, `EXISTS` inside a filter, an optional
    or a join ARQ does not run row by row, a filter on `geof:sfIntersects` in a store with the spatial
    index (Jena's route seeds it from that index), an assignment of a term the store has no id for.
- **A walk hands the graphs it has left to parallel workers** once it has produced
  `beakgraph.scan.parallel.threshold` rows, where a row costs more than reading it: a join, an optional.
  The existing parallel scan was planned for one graph of that many positions; a store of thousands of
  small graphs never met that, and ran its joins on one thread. A lookup, a `LIMIT` met early and a walk
  under an outer row start no worker.
- **Rows bind their new variables without a lookup** (`RowVars`, `BindingNodeId.bind`). Every row asked,
  for each variable it binds, whether the binding under it already held one, walking the chain of layers:
  the answer is the same for every row of an iterator, and is found once. A variable that is not new -
  held by the binding underneath, or repeated in the pattern - keeps the check that compares its two
  values. The reader also looks a pattern position up in the binding once where it looked twice.

Measured as before (the endpoint on the JVM, one client on a connection kept alive, 4.0M quads in 5,000
graphs, each build on two servers in turn), in milliseconds a request, `falcon2` against `falcon3`:

| Query, all under `GRAPH ?g` | Before | After |
|---|---|---|
| Subject lookup, a filter in the block | 8.4 | 1.9 |
| Subject lookup, an optional in the block | 6.4 | 1.4 |
| Subject lookup, a union in the block | 10.1 | 2.5 |
| 200,000 rows with an optional in the block, counted | 655 | 54 |
| 400,000 rows of a union in the block, counted | 113 | 39 |
| 80,000 of 800,000 rows passing a filter in the block, counted | 45 | 24 |
| A join of three patterns, 200,000 rows, counted | 223 | 88 |
| Every quad of every graph scanned and counted (4.0M) | 843 | 512 |
| The rows of a predicate, counted (800,000) | 154 | 123 |
| The rows of a type, counted (200,000) | 31.5 | 24.7 |
| A number of every subject, summed (800,000) | 207 | 177 |
| Subject lookup, plain | 2.1 | 2.1 |
| Lookup, count and dump in one graph (not changed) | the same | the same |

What that says:

- **A block with a filter, an optional or a union now costs what a plain one does**: four times less for a
  lookup, twelve times less where an optional is tried for every row of 5,000 graphs.
- **Workers pay where rows do work, and not otherwise.** With the hand-off switched off the join takes
  195 ms and the optional 108 ms: workers are the last 2.2 to 2.5 times of those two rows of the table.
  A single pattern's rows were tried too: 13 to 17% faster for three scans, 12% slower for a fourth, with
  every core busy. One thread consumes the rows either way, and handing a row from one thread to another
  costs about what reading it does. So those stay on one thread, and their gain in the table is the
  bindings'.
- **The bindings' share was larger than the profile's 13% suggested** for the scan of every quad, whose
  rows bind three variables each: 0.84 s to 0.51 s on one thread.
- **Answers are the same**: every query gave the same rows from both builds; the tests hold the walk to
  Jena's operator and to an in-memory dataset on 64 query shapes it takes and 22 it leaves, and hold rows
  with a repeated or an already bound variable to an in-memory dataset on 30 more.
- Not measured: the native binary, and real data.

Still open here:

- A walk of single patterns is limited by the thread that consumes its rows: aggregating on ids, in the
  workers, is item 4. (Done since: see "Groups and aggregates on ids, in the workers".)
- An optional or join that ARQ keeps as a left join or a join, `MINUS` and `BIND` inside a graph block.

### The terms by id (2026-11-15, branch `falcon3`, not committed when written)

One change, to the lookup a query makes for every value it reads: the term of an id
(`SimpleNodeTable.getNodeForNodeId`). Both directions of the node table were Caffeine caches, and the
profiles above had the id-to-term one at 14% of a lookup without a graph, 23% of a count of 800,000 values
and about half of `-verify -deep`. The store's format is untouched.

- **Terms are kept by page** (`IdNodeCache`). A dictionary counts its ids up in the order of its terms and
  the indexes hand them out in that order, so the ids a query asks for lie together. The terms of 16
  consecutive ids share a page, an array of 16 references; a table of buckets of four slots, kept no more
  than an eighth full, finds the page. A hit reads the slot, the page's number and the reference: no boxed
  key, no lock, nothing written, nothing left for another thread to tidy.
- **The table is in the order of the ids too.** The pages of 4,096 consecutive ids have buckets that follow
  one another, and where such a run begins is hashed. A query that goes through ids in order goes through
  the table in order; so does a collection, which moves the terms into that order as it finds them through
  the table.
- **The bound is the same property**, `beakgraph.nodetable.cache.size`, in the same weights (1 a term, more
  for a long literal). Over it, whole pages are dropped from a position that moves round the table until a
  tenth of it is free; Caffeine chose entries by how often and how lately they were asked. Memory beside
  the terms themselves: nine bytes a term when the ids of a page are all in use, 144 when a term is alone
  on its page, which is about what a Caffeine entry was.
- **Term to id stays in Caffeine**: it is asked for a query's constants and for joined values, and a term
  is not a number that could be an index.

Measured on the store of 4.0M quads in 5,000 graphs (805,016 entities, 1,611,017 objects), `590b73f`
against the working tree.

Nanoseconds a term, one thread asking the node table, the median of five rounds, the heap collected once
after the cache is filled (`target/perf23/NodeBench.java` when measured; not in the repository):

| Asked | Before | After |
|---|---|---|
| 1,000 terms, again and again | 8.4 | 5.9 |
| 200,000 terms in the order of their ids, 100 times each | 17.1 | 7.7 |
| 200,000 terms in no order, 100 times each | 74.9 | 39.3 |
| 1,611,017 terms under the bound of 1,000,000, 4 times each | 174 | 127 |
| 200,000 terms in the order of their ids, 4 threads at once | 21.7 | 6.3 |
| 200,000 terms in the order of their ids, 16 threads at once | 35.7 | 7.3 |
| 200,000 terms in no order, 4 threads at once | 80.0 | 33.4 |
| 200,000 terms in no order, 16 threads at once | 77.4 | 34.9 |

The passes and queries that read terms (the command line: medians of three runs, on the JVM and in native
binaries built from both trees; the endpoint: the JVM, four servers a build, one client on a connection
kept alive):

| | Before | After |
|---|---|---|
| `-verify -deep`, time | 5.75 s | 3.96 s |
| `-verify -deep`, processor time | 35.9 s | 17.1 s |
| `-export NQ`, time | 2.71 s | 2.67 s |
| `-export NQ`, processor time | 9.6 s | 9.6 s |
| Native binary: `-verify -deep`, time | 28.4 s | 11.4 s |
| Native binary: `-verify -deep`, processor time | 42.4 s | 14.8 s |
| Native binary: `-export NQ`, time | 4.06 s | 4.06 s |
| Endpoint: the rows of a predicate, counted (800,000) | 122 ms | 110 ms |
| Endpoint: a number of every subject, summed (800,000) | 183 ms | 175 ms |
| Endpoint: the other 22 queries of the tables above | the same | the same |

What that says:

- **The command line's check gains most.** `-verify -deep` reads every quad as terms, 2.4M of them under a
  bound of 1M: 31% less time, and half the processor time, which had gone to Caffeine's evictions and to
  its upkeep on other threads.
- **And most of all in the native binary**, where the same check takes 11.4 s where it took 28.4 s: it
  lost 17 s there and 1.8 s on the JVM, so what Caffeine did for each term cost a native image far more.
  The native check is still three times the JVM's.
- **Threads that ask at once do not slow each other.** A lookup in id order costs a thread 7 ns whether 1
  or 16 of them ask; through Caffeine, which records each read for its eviction policy, it rose from 17 ns
  to 36 ns.
- **The endpoint gains little**: a tenth on a count of 800,000 values. Their sum takes 175 ms on every
  server where it took 175 on two and 190 on two. Lookups, dumps and group-bys take what they took, each
  within the few percent its servers differ by. The profiles had suggested more (23% of that count); a
  share of samples that includes another thread's upkeep is not the time a request waits.
- **`-export` does not ask the node table** - it writes the dictionary's text - so it is unchanged, as it
  should be.
- **A loop of cache hits has to be collected once to be measured.** With this cache such a loop allocates
  nothing, so the heap is never collected and the terms stay where they were decoded, among the garbage of
  their decoding: 35 ns a term in order instead of 8, and 80 in no order instead of 39. Caffeine's loop
  boxed a key for every lookup and so collected by itself (25 and 68 to 80 without the collection above).
  A server's queries allocate all the time, so the table gives both builds one collection.
- **Two other designs were measured first, and dropped.** A table of single terms, eight to a bucket by a
  multiplied hash with an array of ids beside it: 33 ns in order where Caffeine took 15, because the hash
  scatters ids that arrive together, and 790 ns over the bound, because each sweep counted the whole
  table. Then pages found through an array of page numbers: 74 ns in no order where Caffeine took 63, one
  more place in memory for every lookup.
- **Answers are the same**: every query gave the same rows from both builds, and the export the same
  403,932,568 bytes.
- Not measured: many clients at once at the endpoint (the table has lookups from 16 threads; the tests hold
  the cache to its answers and its bound on every core), the native binary at the endpoint, and real data.

### Groups and aggregates on ids, in the workers (2026-11-15, branch `falcon3`, not committed when written)

The part of item 4 that the last sections kept pointing at: an aggregate's rows were read by workers and then
handed, one by one, to the single thread that grouped them. `GROUP BY` and the aggregates are now computed
where the rows are read (`GroupOnIds`). The store's format is untouched.

- **A row stays its ids.** The group is found by the ids of the key variables in a table of longs. `COUNT(*)`,
  `COUNT(?v)` and `COUNT(DISTINCT ?v)` are a row, a variable that has an id, a set of ids: no term is read.
- **`SUM`, `AVG`, `MIN`, `MAX` are Jena's own accumulators**, given each row, so a value is added, compared and
  found in error by the code that does so in Jena's group. Each worker has its own; their results are combined
  by the operation that made them (Jena's addition, its ordering, the sum over the count).
- **Workers keep what they read.** The named graphs are divided among them as for the walk, and a graph whose
  first pattern is a large scan is divided by position. A worker's groups are put together with the others'
  when it is done, in the order of the work; one with more than `beakgraph.group.flush` groups hands them on
  as it goes.
- **A predicate's scan is divided by its rows where its objects are too few** (`ScanChunks`, for this caller
  only). A parallel scan was divided by object positions, and needed 65,536 of them: `?s a ?type`, four types
  and 400,000 subjects, was one thread's.
- **Filters are evaluated by the workers**, each with copies of the expressions, since an expression may keep
  state. (The walk that hands its rows on still evaluates filters on the consuming thread.)
- **Values are read by id, past Jena's cache** (`RowValue`). Jena makes the value of a term through one cache
  of 10,000 entries for the whole JVM. Over many different terms it is written and evicted for every row, and
  threads wait for each other there: it was 65% of the processor time of a filtered count on every core. A
  worker's copy of an expression finds a value it made before by the row's id, and makes a new one the way
  Jena's makers do for an IRI, a string, an integer, a decimal, a double or a float; any other kind is Jena's
  to make, through its cache.
- **When workers start is counted, not timed**: after reading has cost `beakgraph.group.handoff` (4,096: a
  row grouped, a row put to a later pattern or a filter, four for a graph visited), and as many as the graphs
  left would each have that much to do. The walk waits for 65,536 rows produced, which a filter that passes
  little or a join that finds little never reaches.

What it takes: plain variables as keys, the aggregates above, over patterns the walk runs (also under
`FILTER`, `OPTIONAL`, `UNION`) in one graph, under `GRAPH <iri>`, under `GRAPH ?g`, or a union of those. A
key that is an expression, `SAMPLE`, `GROUP_CONCAT`, `DISTINCT` inside an aggregate other than
`COUNT(DISTINCT ?v)`, the union graph and `FROM` lists are Jena's group as before. The counts the index
holds still come from the index.

Measured at the endpoint (JVM, one client on a connection kept alive, each build on two servers in turn, 15
requests after 15), in milliseconds a request, `12da8d1` against the working tree.

The store of 4.0M quads in 5,000 graphs, all under `GRAPH ?g`:

| Query | Before | After |
|---|---|---|
| Every quad scanned and counted (4.0M) | 411 | 20.5 |
| The rows of a predicate counted (800,000) | 96 | 6.3 |
| A number of every subject summed (800,000) | 113 | 14.0 |
| Sum, average, least and greatest of it | 227 | 23.1 |
| Its distinct values counted (1,000) | 110 | 5.8 |
| Distinct subjects counted (800,000) | 470 | 27.0 |
| Rows by tag (10 groups) | 118 | 5.3 |
| Count, sum and greatest by tag, over a join (10 groups) | 345 | 43.2 |
| Rows by value (1,000 groups) | 131 | 16.7 |
| Rows by graph and type (20,000 groups) | 303 | 81 |
| Rows by subject (800,000 groups) | 2,627 | 1,083 |
| A filter that passes 800 of 800,000 | 117 | 9.7 |
| A join of three patterns counted (200,000) | 91 | 15.4 |
| An optional counted (200,000) | 57 | 9.0 |
| A lookup counted (one graph of the 5,000 has it) | 2.4 | 2.2 |
| Lookups, dumps and the index's counts | the same | the same |

A store of one graph of 2.0M quads (400,000 subjects), under `GRAPH <iri>`:

| Query | Before | After |
|---|---|---|
| Every quad counted through a filter (2.0M) | 349 | 13.9 |
| Quads by predicate through a filter (5 groups) | 491 | 15.2 |
| Names counted and the greatest found (400,000 of their own) | 235 | 9.4 and 23.4 |
| A join counted and summed (400,000) | 338 | 23.0 |
| Numbers counted and summed (400,000 rows of 1,000 values) | 85 | 6.5 |
| Rows by type (400,000 rows of 4 values) | 68 | 3.5 |
| Rows by tag and type, over a join (20 groups) | 417 | 21.2 |

What that says:

- **An aggregate now costs what reading its rows costs on every core**: 5 to 30 times less on this machine
  (16 cores) for the queries with few groups.
- **Many groups gain least.** 20,000 groups are 20,000 rows to write (81 ms, most of it the result); 800,000
  groups are 800,000 entries put together from the workers' tables on one thread.
- **A query that reads little is left alone.** The counted lookup is as it was. Started for every core as
  soon as it qualified, it took 3.8 ms instead of 2.6: hence workers by the work left.
- **The steps, as they were measured.** Grouping in the workers alone, waiting for 65,536 rows: the scan 23 ms,
  but the join 74 (from 90), the optional 36, the filtered count 15. Counting the work instead: 18, 9 and 7.
  On one graph: the filtered count took 106 ms until values were read by id (13.9), and the rows of a type
  13.6 ms on one thread until the scan was divided by its rows (3.5).
- **Answers are the same**: every query here gave the same rows from both builds, and the tests hold the
  groups to Jena's group over the same rows and to plain ARQ over the same data in memory, on 42 shapes it
  takes, 16 it leaves, 17 more read by workers and 300 drawn at random.
- **One difference to know**: a floating-point sum depends on the order of its additions, and workers add in
  another order than one thread. A `SUM` or `AVG` of `xsd:double` or `xsd:float` values can differ in its last
  digits from the one thread's answer (as it already could after a parallel scan). Integer and decimal sums
  are exact. The same query gives the same answer each time, but for a floating-point sum over more groups
  than a worker keeps to itself.
- **The native binary answers as the JVM does**: built from this tree, it gave the JVM's answer to each of
  the sixteen queries of the first table (the scan in 21 ms, the four aggregates of a number at once in
  133 ms). It was not measured against a native binary of the build before.
- `-verify -deep` and `-export` take what they took (3.9 s and 2.7 s).
- Not measured: many clients at once, and real data.

Still open here:

- The walk that hands its rows on (`SELECT *`, `DISTINCT`, a group Jena makes) still evaluates filters on one
  thread and reads values through Jena's cache.
- Dates, booleans and language-tagged strings still go through that cache in the workers.
- A group with very many keys is put together on one thread.
- `MINUS`, `BIND` and the joins ARQ keeps as joins are not blocks the walk runs, so neither are groups over
  them.

### Zarr: one file, over HTTP, in a storage directory, past one chunk (2026-11-15, branch `falcon`, not committed when written)

The four things left open under item 2, which HDF5 did and the Zarr container did not. The store format is
untouched (format v6); SPECIFICATIONS.md §3a gained item 8 (the single-file form) and says how a store is
read over HTTP.

- **One file.** `-zip`, or a destination named `*.zip`, writes a Zarr store as a ZIP archive
  (`name.zarr.zip`): the directory store's files as its entries, none compressed, so an entry is the chunk's
  bytes and is mapped where it lies in the archive. The writer builds the directory as before and packs it
  with the JDK's `ZipOutputStream` (which writes Zip64 when the archive needs it, and checks each entry
  against the CRC-32 taken while its chunk was written); the root's `zarr.json` comes first, the rest in key
  order, each with one fixed time, so the same store is the same archive byte for byte. Reading needed one
  addition to Falcon: `ZipStore.map`, an uncompressed entry as a mapped view of the archive (Falcon
  0.3.0).
- **Over HTTP.** `BG.getBeakGraph(URI)` and the pool open a URL that ends in `.zarr`. The root's `zarr.json`
  is fetched whole through Falcon's `HttpStore`; every chunk is then read by ranges through an
  `HTTPSeekableByteChannel` of its own, and the channels share one client and ONE block cache
  (`BlockCache`, 256 blocks of 128 KiB for the whole store). A chunk's channel is told its size, which the
  array declares, so nothing is asked about a chunk but the ranges read of it; the answer to its first range
  gives the validator the later ones are pinned to. Any other Falcon `Store` is read through the same cache.
- **In a storage directory.** The LWS scan skips dot-names, so a Zarr store was listed as a container
  holding only its `zarr.json`, and took no query. It is one data resource now, queried at its URL as an
  `.h5` is, and its keys are served by range, so one endpoint can read another's Zarr store in place.
- **Several chunks.** An array past the size of a chunk was read through its mappings a range at a time,
  each value copied out through a scratch buffer, and was treated as remote. `SegmentedBytes` reads a value
  straight from the chunk it lies in; the chunk is found by the high half of one multiplication.

Measured on the 4.0M-quad store (5,000 graphs), JVM, same machine as before:

| What | HDF5 | Zarr directory | Zarr in a ZIP |
|---|---|---|---|
| On disk | 57,334,529 bytes, 1 file | 57,358,663 bytes, 122 files | 57,377,607 bytes, 1 file |
| Build, `-method 3` (three each; the command line counts in steps of a second) | 8.8, 9.7, 8.8 s | 9.7, 8.7, 9.8 s | 9.9, 9.7, 9.7 s |
| Open a store in a warm JVM | 0.25 ms | 2.48 ms | 1.37 ms |
| First query after the open | 2.76 ms | 2.77 ms | 2.65 ms |
| Close | 0.70 ms | 1.05 ms | 0.75 ms |
| `-verify -deep` | 4.26 s | 4.08 s | 4.04 s |
| `-export NQ` (the same 403,932,568 bytes) | 2.94 s | 2.68 s | 2.66 s |

- **The archive reads as the directory does**, as designed: the same mapped bytes. It opens 1.1 ms faster
  than the directory (one file to open, not 57) and 1.1 ms slower than HDF5.
- **Where the archive's open goes** (Falcon alone, same store): the archive's directory 0.10 ms, the
  `zarr.json` 0.32 ms, mapping the 57 chunks one by one 0.79 ms, closing 0.16 ms. The whole archive as ONE
  mapping takes 0.10 ms to make and end, so mapping it once and cutting the chunks from that would take
  about 0.7 ms off; not done (`ZipStore.map` maps an entry by itself). The directory store: 0.40 ms for the
  document, 1.95 ms to map its 57 files, 0.43 ms to close them.
- **A build into an archive writes the store once more**: nothing to see at 57 MB; a store of 4.8 GB (below)
  took 71 to 85 s to write as an archive and 41 to 43 s as a directory, on a disk that forces about 100 MB
  a second. On disk it needs the store once, and its largest chunk twice, while it is packed.

Arrays of several chunks, two ways. The same 4.0M-quad store written with chunks of at most 1 MiB
(`-Dbeakgraph.zarr.chunk.max=1048576`: 10 of its 55 arrays are then several chunks, 167 files):

| | the build before (`07c8328`) | this tree | one chunk an array (either) |
|---|---|---|---|
| `-verify -deep` | 4.98 s (4.79 to 5.31) | 4.19 s (4.07 to 4.45) | 4.1 to 4.2 s |
| `-export NQ` | 2.94 s | 2.79 s | 2.72 s |

And at real size: a store of four arrays - 2.5 GiB (three chunks of 894,788,969 bytes), 1 GiB, 7 bytes
under 1 GiB, and 3 MiB; 4.8 GB in all - written as an archive and as a directory, and every array read back
against the pattern it was written with: around every chunk's edge, at 300,000 places drawn at random, a
megabyte across each edge, and past its end (refused, though the last chunk is stored longer than the
array). A value of 8 bytes at random in 64 MiB around a chunk's edge:

| | ns a read |
|---|---|
| One chunk, one mapping (`MemorySegmentBytes`) | 21.4 to 22.2 |
| Three chunks, this tree (`SegmentedBytes`) | 27.0 to 28.8 |
| Three chunks, before (`ChannelBytes` over the mappings) | 70.7 |
| For scale: an array of 3 MiB, all of it in the processor's cache | 2.3 |

- **Several chunks cost about 6 ns a read over one mapping**, where they cost about 50: a third of that was
  taken off by finding the chunk with an integer multiplication instead of one in floating point (34 ns as
  first written). The rest is the second level: the piece's object, then its bytes.
- **The native binary reads the pieces through ByteBuffer views**, as it reads one mapping (see "After the
  speed test"): its `-verify -deep` of the store with chunks of 1 MiB took 14.0 s through the segments and
  takes 11.6 s through views, against 11.3 s for the HDF5 file and 11.2 s for the archive.
- **The archive of 4.8 GB is a Zip64 archive** - five of its twelve entries begin past 4 GiB - which Falcon
  maps and Python's `zipfile` reads, every CRC-32 right.
- **Another ZIP tool, both ways** (Python's `zipfile`, on a small store): the archive extracted is the
  directory store file for file; the directory store zipped by that tool without compression, directory
  entries and all, verifies and exports the same bytes; zipped with compression it is refused, by name of
  the first chunk and of the rule.

Over HTTP, from a server on the loopback interface that counts what it is asked (two runs each, a fresh JVM
a run; the times are a loopback's, and what carries to a real link is the requests and the bytes):

| | HDF5 | Zarr, this tree | Zarr, before (`07c8328`) |
|---|---|---|---|
| Open | 6 requests, 580 KB | 4 requests, 42 KB | 4 requests, 42 KB |
| One subject's triples (the first query) | 34 requests, 6.3 MB, 191 to 238 ms | 54 requests, 4.4 MB, 166 to 179 ms | 23,499 requests, 3.7 to 4.1 s |
| One graph, 801 rows | 16 requests, 2.1 MB, 39 to 44 ms | 20 requests, 3.1 MB, 44 to 47 ms | 6,583 requests, 1.0 s |
| A join in a graph | 17 requests, 3.5 MB, 51 to 59 ms | 28 requests, 2.9 MB, 65 to 95 ms | 21,738 requests, 3.1 to 3.2 s |
| A name, in any graph | 26 requests, 11.7 MB, 110 to 125 ms | 28 requests, 12.0 MB, 127 to 134 ms | 134,666 requests, 19.3 to 19.7 s |
| Count every quad (from the index) | 8 requests, 3.8 MB, 41 to 42 ms | 10 requests, 4.3 MB, 43 to 51 ms | 158,759 requests, 22.6 to 22.8 s |
| Another subject's triples | 4 requests, 0.5 MB, 11 to 12 ms | 4 requests, 0.5 MB, 6 to 7 ms | 1,436 requests, 0.21 s |
| Count a tag, 400,000 rows | 11 requests, 10.6 MB, 79 to 88 ms | 15 requests, 11.0 MB, 79 to 86 ms | 215,996 requests, 30.6 to 30.9 s |
| All seven | **122 requests, 39.0 MB** | **163 requests, 38.3 MB** | **578,841 requests, 4.8 MB** |
| Each asked again | no request; 0.5 to 14 ms | no request; 0.5 to 18 ms | 52 to 1,819 requests a query |

- **Before, a read was a request**: 23,499 for one subject's five triples. Unusable over anything but a
  loopback, and it is why remote Zarr was listed as not there.
- **Now a Zarr store costs what an HDF5 file does in bytes, and a third more in requests.** The bytes are
  the same blocks. The requests are more because every small array is an object of its own, where HDF5's
  128 KiB block brings several small datasets at once: the first query touches 54 arrays' worth. On a link
  with a round trip of 20 ms that query would take about 0.7 s of waiting as HDF5 and 1.1 s as Zarr (the
  requests are made one after another; this is arithmetic, not a measurement).
- **Opening is cheaper than HDF5's**: one document, and the three block directories a reader looks at, each
  fetched at its own size - 42 KB against 580 KB, as HDF5's blocks are 128 KiB whatever is needed of them.
- **A `HEAD` a chunk was spent, and is not**: as first written, each chunk's channel looked at the chunk
  before reading it - 216 requests for the seven queries, not 163. The array already says the chunk's size.
- **Once read, a query asks the server nothing**, in either container. Zarr is then as fast as HDF5 for the
  lookups and 25 to 35% slower for the two counts that scan (12 against 9 ms, 18 against 13): a read goes
  through one more step, the array's reader, to its chunk's channel.
- **Any Falcon store through the same cache** (`ZarrStore.open(store, name)` on Falcon's `HttpStore`): the
  same 163 requests and 38.3 MB, and times within the spread of the channels'. What the channels add is
  not speed on a loopback: retries with backoff, a kept connection, and a chunk replaced on the server
  refused instead of read.

Tests: 2,353 pass (30 of them new: the several-chunk view against a plain array for every offset and
chunk length, offsets past 4 GiB and past the array; the archive against the HDF5 file and the directory for
every engine, byte for byte, and what is not such an archive; a store over HTTP against the local one, with
the server's requests counted, a chunk replaced, absent, gone, and of the wrong size, and a signed URL kept
out of every message; a storage directory serving a Zarr store, its keys and nothing else of its
directory, and read back through `BG.getBeakGraph(URI)`; `-zip` from the command line). Falcon: 970 Zarr
tests pass, 4 new.

Still open here:

- **A ZIP archive over HTTP.** It would be the better remote form of Zarr: one object, so small arrays would
  share a block as HDF5's datasets do. Falcon's `ZipStore` reads a local file only.
- **A store's small arrays fetched together** (or at once, in parallel) when it is opened: most of the 54
  requests of a first query are arrays of a few kilobytes.
- **One mapping of an archive** for all its chunks (0.7 ms of a 1.4 ms open).
- **A store replaced as a whole while it is read over HTTP** is noticed chunk by chunk, from the moment each
  is first read; a chunk never read before is taken from whichever store is there. A store on a server is
  to be published under a new name, as an HDF5 file is replaced by a new file.
- **The warm read path of a remote Zarr store** is a step longer than HDF5's (the 25 to 35% above).
- **Writing an archive in one pass**, without the directory first: for stores of many gigabytes, where the
  second write is minutes.
- Not measured: a real link (latency, TLS), an object store, and real data.

The native binary, built from this tree, was held to the JVM on synthetic data: its archive of a source is
the JVM's byte for byte (with `-method 3` and with `-method 4`); it verifies and exports the three
containers alike, the exports the same bytes; `-zip` and `-zarr` make the names they should; and it serves
a storage directory whose Zarr store a JVM then reads through `BG.getBeakGraph(URI)`. Not checked: a store
read over HTTP from inside the native binary. Nothing on the command line asks for it, and the binary is
built with the `https` URL protocol enabled and `http` not (`--enable-url-protocols=https`), which the one
document fetched through Falcon's `HttpStore` would go by.
