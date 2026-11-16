# Speed test: master against falcon, in HDF5 and in Zarr

Measured on 2026-11-15. Three configurations, each building its own store from the same source and reading
the store it built:

| Name here | Code | Container | Store format |
|---|---|---|---|
| **master** | `master`, commit `33febe9` (jHDF; the native HDF5 library for the disk engines) | HDF5 | v5 |
| **falcon HDF5** | `falcon`, commit `07c8328`, on Falcon 0.2.0 | HDF5 | v6 |
| **falcon Zarr** | the same build, destination named `*.zarr` | Zarr v3 directory store | v6 |

The three gave the same answers to every query but one, a floating-point average whose last digits differ
(see "How far to trust these"). Their exports are the same bytes on the synthetic stores and the same size
on real data.

This replaces the test of 2026-11-14, which compared master with `falcon2` at commit `bf09ba1` and is in this
file's history. What has changed on the falcon side since is at the end.

## Summary

- **Stores are smaller**: 21% on the store of many graphs, 42% on the store of one graph, 19% on real data.
- **Builds take the same time.**
- **Lookups are 3 to 6 times faster** at the endpoint, in one graph and across all of them.
- **Counts and aggregates are 5 to 80 times faster** where rows are read, and 13 to 465 times where the
  index now answers. On real data: 6 to 38 times for aggregates over 1.36M features (twice for a join that
  begins at a constant object), and 560 to 1,300 times for counts across the 31,311 graphs.
- **`-verify -deep` is a third faster on the JVM and 2.5 times faster in the native binary**; on real data,
  17% faster. `-export` is equal on the synthetic stores and 14% faster on real data.
- **The native binary is no longer slower than master's** at anything measured, and is 5 to 600 times
  faster at counts and aggregates.
- **Equal on both branches**: dumps of one graph (1.3 times), a query answered from the spatial index, a
  query cut short by `LIMIT`, the list of graphs, and opening a store.
- **HDF5 and Zarr perform alike on falcon**, apart from opening a store, where Zarr takes about 2.4 ms more.

## Setup

- One machine: Windows 11, Ryzen 9 5950X (16 cores), 128 GB RAM, GraalVM CE 25.0.4. Linux and macOS were
  not measured.
- **Store of many graphs**: 4,005,001 quads in 5,000 named graphs of 801 quads each, from one TriG file.
- **Store of one graph**: 2,000,001 quads, 400,000 subjects of five triples in one named graph.
- **Real data**: a 1.79 GB format-v2 store of pathology segmentation data ("TCGA"). A copy was exported once
  to TriG (3.87 GB) and each configuration rebuilt it with `-method 4 -spatial`. The rebuilt stores hold
  28,492,336 quads in the default graph - 1,356,779 features with a polygon, a classification, a
  probability and ten shape measurements each - and the spatial index's 31,311 tile graphs.
- The configurations took turns, in an order that rotated from repetition to repetition.
- **Endpoint figures** are one client on a connection kept alive, asking for XML; a figure is the mean of
  each server's median, over three servers a configuration for the first table and two for the others.
- **Real-data queries were asked in process**, not through the endpoint, which listens on every interface:
  the data was not served. They therefore carry no HTTP and no result writing.
- The native binary of master is its code built with the reachability metadata of the `falcon` branch: as
  committed on master, the binary dies at startup.

## Synthetic stores, JVM

### Size and build

| | master | falcon HDF5 | falcon Zarr |
|---|---|---|---|
| Many graphs: store on disk | 72,330,677 bytes | 57,334,529 bytes | 57,358,663 bytes in 122 files |
| Many graphs: build, `-method 3` | 8.7 s | 8.8 s | 9.7 s |
| Many graphs: build, `-method 0` (one run) | 16.8 s | 16.7 s | 17.8 s |
| One graph: store on disk | 34,517,264 bytes | 19,908,265 bytes | 19,932,293 bytes in 122 files |
| One graph: build, `-method 3` | 4.7 s | 5.7 s | 5.7 s |
| One graph: build, `-method 0` (one run) | 9.7 s | 10.7 s | 10.7 s |

`-method 3` is the median of three builds, which agreed to a tenth of a second. The command line polls once
a second for its conversions to end, so these times move in steps of one second: falcon's build of the store
of one graph, and its Zarr build of the store of many, ended one step later than master's every time, which
is more than nothing and at most a second.

### Opening a store

In a warm JVM, 400 cycles of open, first query (`SELECT * WHERE { GRAPH ?g { ?s ?p ?o } } LIMIT 1`) and
close; medians of the last 200, the median of three processes, in milliseconds.

| Many graphs (ms) | master | falcon HDF5 | falcon Zarr |
|---|---|---|---|
| Open | 1.27 | 0.24 | 2.52 |
| First query | 2.93 | 3.06 | 2.81 |
| Close | 0.01 | 0.72 | 1.08 |
| Whole cycle | 4.22 | 4.02 | 6.40 |

| One graph (ms) | master | falcon HDF5 | falcon Zarr |
|---|---|---|---|
| Open | 1.56 | 0.27 | 2.65 |
| First query | 2.14 | 2.01 | 1.77 |
| Close | 0.02 | 0.57 | 1.00 |
| Whole cycle | 3.71 | 2.84 | 5.43 |

### Command line

Medians of three runs.

| | master | falcon HDF5 | falcon Zarr |
|---|---|---|---|
| Many graphs: `-verify -deep`, time | 5.65 s | 3.84 s | 3.96 s |
| Many graphs: `-verify -deep`, processor time | 38.5 s | 15.7 s | 19.9 s |
| Many graphs: `-export NQ`, time | 2.68 s | 2.73 s | 2.74 s |
| Many graphs: `-export NQ`, processor time | 8.5 s | 10.2 s | 9.8 s |
| One graph: `-verify -deep`, time | 3.69 s | 2.26 s | 2.08 s |
| One graph: `-verify -deep`, processor time | 32.1 s | 11.4 s | 9.7 s |
| One graph: `-export NQ`, time | 1.76 s | 1.73 s | 1.78 s |
| One graph: `-export NQ`, processor time | 7.3 s | 7.5 s | 7.6 s |

The exports are the same bytes from all three: 403,932,568 from the store of many graphs, 194,378,332 from
the store of one.

### SPARQL endpoint over HTTP: lookups, dumps and the counts the index holds

The store of many graphs; milliseconds a request.

| Query (ms) | master | falcon HDF5 | falcon Zarr | master / falcon HDF5 |
|---|---|---|---|---|
| Subject lookup, graph not given | 7.08 | 2.11 | 1.99 | 3.4 |
| Subject and predicate, graph not given | 6.46 | 1.45 | 1.37 | 4.5 |
| Two-hop join, graph not given | 10.1 | 1.58 | 1.55 | 6.4 |
| Object lookup, graph not given | 7.29 | 2.01 | 1.89 | 3.6 |
| Subject lookup, a filter in the block | 8.29 | 1.88 | 1.80 | 4.4 |
| Subject lookup, an optional in the block | 6.53 | 1.37 | 1.32 | 4.8 |
| Subject lookup, a union in the block | 10.1 | 2.38 | 2.30 | 4.3 |
| Subject lookup in one graph | 0.98 | 0.35 | 0.35 | 2.8 |
| Count of one graph | 1.00 | 0.32 | 0.33 | 3.1 |
| Quads of one graph grouped by predicate | 1.30 | 0.44 | 0.42 | 3.0 |
| Dump of one graph (801 rows) | 3.43 | 2.64 | 2.69 | 1.3 |
| Count of every quad | 611 | 2.80 | 3.02 | 218 |
| Graphs counted | 1,055 | 2.27 | 2.05 | 465 |
| Quads grouped by predicate | 1,076 | 6.61 | 6.14 | 163 |
| Quads grouped by graph (5,000 rows) | 969 | 18.6 | 18.5 | 52 |
| Quads grouped by graph and predicate (30,000 rows) | 1,501 | 116 | 112 | 13 |
| Rows of a type in every graph, counted (200,000) | 39.6 | 2.18 | 3.49 | 18 |
| Rows of a predicate in every graph, counted (800,000) | 193 | 19.6 | 13.9 | 9.8 |
| A number of every subject in every graph, summed (800,000) | 281 | 13.9 | 13.8 | 20 |
| Every quad of every graph, scanned and counted (4.0M) | 936 | 21.6 | 20.9 | 43 |
| Join scan in every graph, counted (200,000) | 297 | 17.7 | 17.4 | 17 |
| Rows under a filter in the block, counted (80,000 of 800,000) | 40.7 | 8.89 | 9.45 | 4.6 |
| Rows with an optional in the block, counted (200,000) | 663 | 9.54 | 9.99 | 69 |
| Rows of a union in the block, counted (400,000) | 95.6 | 2.74 | 2.94 | 35 |

### SPARQL endpoint over HTTP: groups and aggregates

The store of many graphs, every query under `GRAPH ?g`, 15 requests after 15; milliseconds a request.

| Query (ms) | master | falcon HDF5 | falcon Zarr | master / falcon HDF5 |
|---|---|---|---|---|
| Every quad scanned and counted (4.0M) | 641 | 20.6 | 20.8 | 31 |
| Rows of a predicate counted (800,000) | 156 | 6.05 | 6.31 | 26 |
| A number of every subject summed (800,000) | 179 | 13.7 | 14.2 | 13 |
| Sum, average, least and greatest of it | 334 | 23.9 | 23.6 | 14 |
| Its distinct values counted (1,000) | 172 | 6.14 | 5.73 | 28 |
| Distinct subjects counted (800,000) | 655 | 23.7 | 25.8 | 28 |
| Rows by tag (10 groups) | 222 | 5.38 | 5.24 | 41 |
| Count, sum and greatest by tag, over a join (10 groups) | 1,159 | 42.7 | 43.3 | 27 |
| Rows by value (1,000 groups) | 235 | 16.7 | 16.1 | 14 |
| Rows by graph and type (20,000 groups) | 422 | 79.6 | 76.9 | 5.3 |
| Rows by subject (800,000 groups) | 3,740 | 1,101 | 1,110 | 3.4 |
| A filter that passes 800 of 800,000 | 187 | 9.53 | 9.62 | 20 |
| A join of three patterns counted (200,000) | 273 | 16.0 | 15.8 | 17 |
| An optional counted (200,000) | 722 | 8.80 | 8.65 | 82 |
| A lookup counted (one graph of the 5,000 has it) | 11.5 | 2.19 | 1.72 | 5.2 |
| One graph: rows by type (160 rows) | 1.26 | 0.58 | 0.52 | 2.2 |

The store of one graph, every query under `GRAPH <iri>`:

| Query (ms) | master | falcon HDF5 | falcon Zarr | master / falcon HDF5 |
|---|---|---|---|---|
| Every quad counted through a filter (2.0M) | 372 | 15.1 | 13.9 | 25 |
| Quads by predicate through a filter (5 groups) | 547 | 14.1 | 13.7 | 39 |
| Names counted and the greatest found (400,000 of their own) | 348 | 7.75 | 12.1 | 45 |
| A join counted and summed (400,000) | 498 | 23.4 | 20.8 | 21 |
| Numbers counted and summed (400,000 rows of 1,000 values) | 121 | 6.06 | 6.28 | 20 |
| Rows by type (400,000 rows of 4 values) | 90.9 | 3.19 | 3.32 | 28 |
| Rows by tag and type, over a join (20 groups) | 608 | 20.7 | 18.8 | 29 |

## Synthetic stores, native binary

Means of two runs.

| | master | falcon HDF5 | falcon Zarr |
|---|---|---|---|
| Many graphs: `-verify -deep` | 29.3 s | 11.6 s | 11.2 s |
| Many graphs: `-export NQ` | 4.08 s | 4.03 s | 4.17 s |
| One graph: `-verify -deep` | 11.1 s | 4.17 s | 4.18 s |
| One graph: `-export NQ` | 2.13 s | 2.02 s | 2.04 s |
| Endpoint ready after start | 510 ms | 506 ms | 523 ms |

The native endpoint on the store of many graphs, two servers a configuration; milliseconds a request.
From "rows by subject" down master has one server: on the second, a request of that query passed the
endpoint's 30-second limit and was refused, which ended the run.

| Query (ms) | master | falcon HDF5 | falcon Zarr | master / falcon HDF5 |
|---|---|---|---|---|
| Subject lookup, graph not given | 21.0 | 3.30 | 3.34 | 6.4 |
| Subject lookup in one graph | 0.99 | 0.32 | 0.32 | 3.1 |
| Dump of one graph (801 rows) | 6.57 | 4.74 | 4.80 | 1.4 |
| Count of every quad | 1,836 | 3.06 | 3.12 | 600 |
| Quads grouped by predicate | 3,807 | 8.45 | 8.49 | 451 |
| Every quad scanned and counted (4.0M) | 1,602 | 44.2 | 39.6 | 36 |
| Rows of a predicate counted (800,000) | 557 | 5.55 | 5.63 | 100 |
| A number of every subject summed (800,000) | 593 | 13.3 | 13.2 | 45 |
| Sum, average, least and greatest of it | 1,414 | 37.2 | 37.4 | 38 |
| Its distinct values counted (1,000) | 574 | 6.50 | 6.61 | 88 |
| Distinct subjects counted (800,000) | 3,241 | 30.4 | 30.7 | 107 |
| Rows by tag (10 groups) | 993 | 5.56 | 5.73 | 178 |
| Count, sum and greatest by tag, over a join (10 groups) | 3,569 | 96.9 | 105 | 37 |
| Rows by value (1,000 groups) | 736 | 18.3 | 18.9 | 40 |
| Rows by graph and type (20,000 groups) | 1,393 | 138 | 139 | 10 |
| Rows by subject (800,000 groups) | 15,796 | 2,905 | 2,903 | 5.4 |
| A filter that passes 800 of 800,000 | 814 | 15.0 | 15.1 | 54 |
| A join of three patterns counted (200,000) | 622 | 29.6 | 30.5 | 21 |
| An optional counted (200,000) | 2,782 | 17.8 | 17.8 | 156 |
| A lookup counted (one graph of the 5,000 has it) | 28.6 | 1.94 | 1.95 | 15 |
| One graph: rows by type (160 rows) | 1.12 | 0.33 | 0.34 | 3.4 |

The scan of every quad took 32 and 56 ms on falcon HDF5's two servers and 21 and 59 ms on Zarr's: the
figure above is a mean of two that differ.

## Real data, JVM

One run each of the build, the verify and the export.

| | master | falcon HDF5 | falcon Zarr |
|---|---|---|---|
| Store on disk | 3,165,134,031 bytes | 2,549,441,441 bytes | 2,549,466,785 bytes in 131 files |
| Build, `-method 4 -spatial`, time | 878 s | 884 s | 855 s |
| Build, processor time | 1,844 s | 1,796 s | 1,854 s |
| `-verify -deep`, time | 409 s | 339 s | 354 s |
| `-verify -deep`, processor time | 946 s | 588 s | 605 s |
| `-export TRIG` (3,869,724,679 bytes from each), time | 312 s | 267 s | 281 s |
| `-export TRIG`, processor time | 680 s | 434 s | 452 s |
| Reader constructed in a fresh JVM (two processes) | 628 and 723 ms | 628 and 857 ms | 634 and 647 ms |

Queries in process: each at least five times after at least two warm-ups, and for as long as two seconds
after one and a half where a query is short; the mean of two processes' medians, in milliseconds.

| Query (ms) | master | falcon HDF5 | falcon Zarr | master / falcon HDF5 |
|---|---|---|---|---|
| Quads of the default graph, counted | 0.44 | 0.43 | 0.43 | 1.0 |
| Quads of the default graph by predicate (31 rows) | 2,345 | 0.49 | 0.48 | 4,737 |
| Quads of every named graph, counted | 8,679 | 13.1 | 13.6 | 661 |
| Quads of every named graph by predicate | 14,331 | 10.9 | 10.6 | 1,315 |
| The ten largest named graphs | 12,028 | 21.5 | 21.5 | 560 |
| The list of named graphs (31,311 rows) | 32.0 | 30.4 | 31.6 | 1.1 |
| Features counted (1.36M) | 66.7 | 10.6 | 11.6 | 6.3 |
| Features by classification, a join (1.36M) | 1,335 | 673 | 633 | 2.0 |
| Probabilities: count, least, greatest, distinct (1.36M) | 352 | 10.7 | 10.8 | 33 |
| Probabilities: sum and average | 227 | 12.5 | 12.5 | 18 |
| Sphericity: count, least, greatest (1.36M) | 1,162 | 30.7 | 30.7 | 38 |
| Sphericity: average (last digits differ) | 766 | 24.1 | 24.2 | 32 |
| Area by classification: count and greatest, a join of three patterns | 3,993 | 176 | 168 | 23 |
| Large features counted, a filter on the area | 0.33 | 0.24 | 0.24 | 1.3 |
| Elongated and large features counted: two measures, filtered | 2,056 | 79.4 | 76.7 | 26 |
| Polygons of a pyramid level across the tile graphs, counted | 72.5 | 8.36 | 6.60 | 8.7 |
| Features in a region (`geof:sfIntersects`, spatial index) | 287 | 265 | 260 | 1.1 |
| 1,000 polygons with their features (`LIMIT`) | 7.37 | 8.59 | 8.30 | 0.9 |

## What explains the differences

Each of these is a section of PERFORMANCE.md, where it was measured by itself.

- **Smaller stores** are format v6: id sequences in blocks, each with the width its values need.
- **Lookups in one graph** (0.98 to 0.35 ms) are the endpoint's response buffer: Fuseki asked for 5 MiB a
  response, which Jetty's pool does not keep, so every answer allocated and cleared one.
- **Lookups without a graph** are `GRAPH ?g` walked once on ids, where Jena ran the block anew in every
  graph, also under `FILTER`, `OPTIONAL` and `UNION`.
- **Counts of every quad, by graph and by predicate** are read off the index.
- **Aggregates over rows** are grouped on ids by the workers that read the rows, which also evaluate the
  filters, read values past Jena's JVM-wide cache of them, and divide a predicate's scan by its rows.
- **`-verify -deep`** is the node table's cache of terms by id, in pages of consecutive ids, in place of a
  Caffeine cache; the native binary gains most because Caffeine cost it most.
- **The native binary's read path** goes through a ByteBuffer view of the mapped file, and Falcon no longer
  writes sparse files: the two things that made it slower than master's in the earlier test.
- **Zarr opens slower** because a store is one file an array: dozens of files to map and to unmap instead
  of one.

## Where falcon is not faster

- **Builds**: equal on real data and on the store of many graphs in HDF5; one step of the one-second
  polling slower on the store of one graph, and in Zarr on the store of many.
- **`-export` on the synthetic stores**, and a **dump of one graph** (1.3 times): the cost is writing the
  result.
- **A query answered from the spatial index** (`geof:sfIntersects`): that route is Jena's on both branches.
- **A query cut short by `LIMIT`**, and **the list of graphs**.
- **A join that begins at a pattern with a constant object** gains only twice (features by classification:
  `?f a geo:Feature` is read on one thread, since only a scan with subject and object both open is divided
  among workers).
- **Many groups**: 800,000 groups are put together on one thread (3.4 times).

## How far to trust these

- One machine, one operating system.
- **The endpoint was asked by one client at a time.** How the branches compare under many clients was not
  measured; falcon's aggregates use every core for one query.
- **The real-data build, verify and export are one run each**, long enough that a few percent is the
  resolution, not more. The real-data queries ran in process, so they are not comparable with the endpoint
  tables row for row.
- **One answer differs**: the average of 1.36M `xsd:double` values agrees to ten digits and not in the
  last. falcon's workers add the values in another order than one thread does, and a floating-point sum
  depends on the order. Every other answer is the same, row for row.
- **master's native binary is not the one its branch builds** (see Setup), and its second endpoint run
  ended early.
- Not measured this time, though the earlier test had them: the JMH micro-benchmarks, queries in process on
  the synthetic stores, remote reads over HTTP ranges, and peak memory.
- The scripts that took these measurements were kept outside the repository and are not part of it.

## Against the earlier test

The test of 2026-11-14 found `falcon2` equal to master at queries on the JVM, 13-23% slower over the
endpoint at scans and joins, slower to open a store (21 ms against 4.5 ms), and 30-70% slower in the native
binary at scan-heavy work. Since then, on the falcon side:

| | master | then (`bf09ba1`) | now (`07c8328`) |
|---|---|---|---|
| Open, first query, close (ms) | 4.2 - 4.5 | 21.3 | 4.0 |
| Native `-verify -deep` | 29 - 30 s | 42.2 s | 11.6 s |
| Native endpoint, count of every quad | 1.3 - 1.8 s | 2.26 s | 3.1 ms |
| JVM endpoint, count of every quad | 0.58 - 0.61 s | 0.66 s | 2.8 ms |

(The synthetic source was generated anew for this test, so its stores are not byte for byte those of the
earlier one; the real-data stores are the same size to the byte.)
