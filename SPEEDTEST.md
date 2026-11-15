# Speed test: master against falcon2, in HDF5 and in Zarr

Measured on 2026-11-14. Three configurations, each building its own store from the same source and reading
the store it built:

| Name here | Code | Container | Store format |
|---|---|---|---|
| **master** | `master`, commit `33febe9` (jHDF; the native HDF5 library for the disk engines) | HDF5 | v5 |
| **falcon2 HDF5** | `falcon2`, commit `bf09ba1`, on Falcon 0.2.0 | HDF5 | v6 |
| **falcon2 Zarr** | the same build, destination named `*.zarr` | Zarr v3 directory store | v6 |

The three gave the same answers to every query (row counts and hashes agree) and byte-identical exports.

## Summary

- **Queries on the JVM run at the same speed**, within a few percent, in process and on both stores below.
  Over the HTTP endpoint, scans and joins are 13-23% slower on falcon2.
- **falcon2 stores are 19% smaller** (format v6), on synthetic and on real data.
- **Lookups are faster, scans slightly slower**: in the micro-benchmarks falcon2 is 15-44% faster at selects
  and at locating an object term, and 0-10% slower at full scans. The other dictionary operations are within
  about 10% either way, but for `extractEntity`, 7-17% slower.
- **HDF5 and Zarr perform alike on falcon2**, apart from opening a store.
- **Opening a store is where falcon2 loses**: open, first query and close take 4.5 ms on master and 10-21 ms
  on falcon2 for the synthetic store, 6.5 ms against 24-41 ms for the real one.
- **The native binary is 30-70% slower on scan-heavy work** on falcon2 (verify, export, counts, lookups across
  graphs) and 30-40% faster at per-graph dumps and joins. Builds are equal. (The faster dumps and joins did
  not reproduce later: see "Since this was measured".)
- **Builds take the same time**, apart from the step that writes the container.

## Setup

- One machine: Windows 11, Ryzen 9 5950X (16 cores), 128 GB RAM, GraalVM CE 25.0.4. Linux and macOS were
  not measured.
- **Synthetic store**: 4,005,000 quads in 5,000 named graphs of 801 quads each, from one TriG file.
- **Micro-benchmark store**: the `benchmarks/` module's own, 50,000 subjects.
- **Real data**: a 1.79 GB format-v2 store of pathology segmentation data ("TCGA"). It was exported once to
  TriG (3.87 GB), and each configuration rebuilt it with `-method 4 -spatial`, which derives the spatial index
  and its tile graphs again. The rebuilt stores each report 28,492,336 source quads and hold 31,311 graphs.
  (The original records 10,854,248 source quads; the difference was not looked into.)
- The configurations took turns, and the order they ran in rotated from repetition to repetition. Unless a
  table says otherwise a figure is the median of three runs (startup: of ten); ranges are in brackets where
  they matter.
- The native binary of master is its code built with the reachability metadata of the `falcon` branch: as
  committed on master, the binary dies at startup.

## Synthetic store, JVM

### Size and build

| | master | falcon2 HDF5 | falcon2 Zarr |
|---|---|---|---|
| Store on disk | 76,266,663 bytes | 62,088,673 bytes | 62,113,759 bytes in 126 files |
| Build, `-method 0`, wall | 24.8 s | 25.8 s | 25.8 s |
| Build, `-method 3`, wall | 9.8 s | 10.9 s | 10.8 s |
| Build, `-method 4`, wall | 16.9 s | 17.8 s | 17.8 s |
| Peak memory, `-method 0` / `3` / `4` | 3.0 / 3.1 / 5.2 GB | 3.1 / 3.1 / 5.2 GB | 3.0 / 3.1 / 5.1 GB |

The command line polls once a second for its conversions to end, so wall time moves in steps of one second.
The `-method 3` writer logs its own times, which are finer:

| `-method 3`, from the writer's log (ms) | master | falcon2 HDF5 | falcon2 Zarr |
|---|---|---|---|
| Stage 1, ingest | 3,733 | 3,665 | 3,723 |
| Stage 2, dictionaries | 1,981 | 2,021 | 2,010 |
| Stage 3, indexes | 3,049 [2,970..3,311] | 3,790 [3,225..3,944] | 2,860 [2,839..3,066] |
| Stage 4, writing the container | 78 | 329 | 855 |
| Write complete | 8,848 | 9,792 | 9,514 |

Stages 1 to 3 run the same code in both falcon2 columns, so their difference in stage 3 is the spread between
runs. What differs for certain is the container step: 0.08 s, 0.33 s, 0.86 s.

### Opening a store

In a warm JVM, 400 cycles of open, first query (`SELECT * WHERE { GRAPH ?g { ?s ?p ?o } } LIMIT 1`) and close;
medians of the last 200, in milliseconds. The fourth column is a plain copy of the falcon2 HDF5 file, which is
not an NTFS sparse file (see "What explains the differences").

| Warm JVM (ms) | master | falcon2 HDF5 | falcon2 Zarr | falcon2 HDF5, plain copy |
|---|---|---|---|---|
| Open | 1.54 | 1.68 | 5.52 | 0.35 |
| First query | 2.93 | 18.86 | 3.23 | 3.12 |
| Close | 0.01 | 0.72 | 1.28 | 0.74 |
| Whole cycle | 4.48 | 21.32 | 10.05 | 4.23 |

In a fresh JVM (median of 7 processes, ms):

| Fresh JVM (ms) | master | falcon2 HDF5 | falcon2 Zarr | falcon2 HDF5, plain copy |
|---|---|---|---|---|
| Reader constructed | 726 | 732 | 731 | 715 |
| First query | 107 | 132 | 115 | 116 |
| Open to the second answer | 991 | 1,014 | 1,000 | 992 |

JVM startup (`-version`): 0.67 s, 0.66 s, 0.65 s.

### Queries in process

One reader, each phase run 15 times after 3 warm-ups; the median of three processes' medians, and the best
round of all, in milliseconds.

| Phase (ms) | master | falcon2 HDF5 | falcon2 Zarr |
|---|---|---|---|
| Graph list (5,000 rows) | 7.9 (best 4.8) | 8.8 (4.3) | 8.1 (5.5) |
| 200 graphs dumped (160,200 rows) | 125.8 (115.9) | 127.1 (119.5) | 132.6 (123.3) |
| 200 graphs: typed subjects (32,000 rows) | 32.4 (28.2) | 33.3 (28.5) | 33.2 (30.0) |
| 200 graphs: quad counts | 18.2 (14.8) | 17.9 (13.1) | 18.4 (15.7) |
| Count of every quad | 472 (427) | 478 (463) | 493 (466) |
| Quads grouped by predicate | 910 (827) | 892 (860) | 975 (881) |

### Command line

| | master | falcon2 HDF5 | falcon2 Zarr |
|---|---|---|---|
| `-verify -deep` | 6.45 s | 6.77 s | 6.86 s |
| `-export NQ` (439,483,700 bytes from each) | 3.47 s | 3.58 s | 3.62 s |

### SPARQL endpoint over HTTP

One server per configuration; each phase 5 rounds, the median of the last 3, in milliseconds.

| JVM endpoint (ms) | master | falcon2 HDF5 | falcon2 Zarr |
|---|---|---|---|
| Ready after start | 1,177 | 1,110 | 1,130 |
| 200 graphs dumped (801 rows each) | 2,891 | 2,996 | 2,922 |
| 1,000 subject lookups, graph not given | 17,291 | 17,602 | 17,074 |
| 100 two-hop joins in one graph | 868 | 1,015 | 1,001 |
| Count of every quad | 577 | 659 | 650 |
| Quads grouped by predicate | 978 | 1,123 | 1,207 |

### Remote reads over HTTP ranges

HDF5 only: a Zarr store is not read over HTTP by the endpoint or the pool yet. The `RemoteVsLocalBench`
battery on the store each wrote, two runs each, no mismatches:

| | master | falcon2 HDF5 |
|---|---|---|
| Range requests | 256 | 241 |
| Bytes fetched | 43.37 MB | 39.11 MB |
| Time, remote | 0.32-0.33 s | 0.29-0.31 s |

### Micro-benchmarks (JMH, 2 forks, mean ± standard deviation)

| Benchmark | master | falcon2 HDF5 | falcon2 Zarr | Unit |
|---|---|---|---|---|
| `SelectBench.adjacentPair` | 114.56 ± 0.55 | 85.73 ± 6.51 | 82.29 ± 3.11 | ns |
| `SelectBench.directorySelect1` | 60.91 ± 3.56 | 51.72 ± 0.37 | 51.11 ± 0.58 | ns |
| `SelectBench.linearSelect1` | 1,786.79 ± 10.11 | 1,358.56 ± 4.22 | 1,152.72 ± 17.08 | ns |
| `DictionaryBench.entityLocateHit` | 30.79 ± 1.68 | 27.55 ± 0.28 | 28.81 ± 0.15 | ns |
| `DictionaryBench.objectLocateHit` | 31.82 ± 1.69 | 18.44 ± 0.33 | 17.88 ± 1.24 | ns |
| `DictionaryBench.predicateLocateHit` | 9.45 ± 0.14 | 9.41 ± 0.12 | 9.71 ± 0.22 | ns |
| `DictionaryBench.locateMiss` | 18.47 ± 0.46 | 18.99 ± 0.24 | 18.41 ± 0.36 | ns |
| `DictionaryBench.extractEntity` | 60.00 ± 1.12 | 63.98 ± 5.82 | 70.49 ± 2.99 | ns |
| `DictionaryBench.extractObject` | 76.33 ± 1.29 | 76.02 ± 1.04 | 78.53 ± 2.09 | ns |
| `DictionaryBench.nodeTableLookup` | 16.68 ± 1.30 | 17.08 ± 1.04 | 16.22 ± 0.80 | ns |
| `QueryBench.pointLookup` | 2.71 ± 0.07 | 2.59 ± 0.09 | 2.70 ± 0.05 | µs |
| `QueryBench.starJoin` | 4.55 ± 0.13 | 4.33 ± 0.07 | 4.50 ± 0.17 | µs |
| `QueryBench.chainJoin` | 73.26 ± 1.36 | 65.04 ± 1.40 | 61.93 ± 0.98 | µs |
| `QueryBench.rangeFilter` | 51.66 ± 1.34 | 54.99 ± 1.30 | 55.39 ± 1.58 | µs |
| `QueryBench.graphVarScan` | 2,717 ± 66 | 2,813 ± 35 | 2,705 ± 106 | µs |
| `QueryBench.distinctSubjects` | 1,985 ± 100 | 2,103 ± 35 | 2,050 ± 25 | µs |
| `QueryBench.predicateScan` | 9,356 ± 1,008 | 10,902 ± 1,188 | 9,568 ± 753 | µs |
| `QueryBench.fullScanFind` | 24,255 ± 897 | 26,733 ± 483 | 26,498 ± 969 | µs |
| `QueryBench.fullScanSparql` | 49,167 ± 909 | 49,723 ± 915 | 48,801 ± 636 | µs |
| `QueryBench.distinctObjects` | 91,033 ± 1,297 | 94,671 ± 1,649 | 95,644 ± 2,647 | µs |

## Synthetic store, native binary

| | master | falcon2 HDF5 | falcon2 Zarr |
|---|---|---|---|
| Startup (`-version`) | 0.044 s | 0.041 s | 0.041 s |
| Build, `-method 3` (2 runs) | 38.4 s | 38.7 s | 38.2 s |
| `-verify -deep` | 30.1 s | 42.2 s | 41.4 s |
| `-export NQ` | 5.68 s | 7.52 s | 7.35 s |

| Native endpoint (ms) | master | falcon2 HDF5 | falcon2 Zarr |
|---|---|---|---|
| Ready after start | 522 | 518 | 506 |
| 200 graphs dumped (801 rows each) | 4,203 | 2,557 | 2,819 |
| 1,000 subject lookups, graph not given | 25,547 | 37,497 | 37,392 |
| 100 two-hop joins in one graph | 884 | 511 | 633 |
| Count of every quad | 1,348 | 2,261 | 2,271 |
| Quads grouped by predicate | 2,585 | 3,550 | 3,443 |

## Real data, JVM

| | master | falcon2 HDF5 | falcon2 Zarr |
|---|---|---|---|
| Store on disk | 3,165,134,031 bytes | 2,549,441,441 bytes | 2,549,466,785 bytes in 131 files |
| Build, `-method 4 -spatial` (one run) | 667 s | 660 s | 658 s |
| Peak memory of the build | 13.1 GB | 12.8 GB | 12.3 GB |
| `-verify -deep` (one run, all pass) | 593 s | 570 s | 430 s |
| `-export TRIG` (one run, 3,869,724,679 bytes from each) | 292 s | 305 s | 309 s |

| Warm JVM, 200 cycles (ms) | master | falcon2 HDF5 | falcon2 Zarr |
|---|---|---|---|
| Open | 1.92 | 5.26 | 7.22 |
| First query | 4.60 | 32.66 | 12.16 |
| Close | 0.03 | 2.82 | 4.41 |
| Whole cycle | 6.55 | 40.92 | 23.87 |

| Fresh JVM, 5 processes (ms) | master | falcon2 HDF5 | falcon2 Zarr |
|---|---|---|---|
| Reader constructed | 718 | 726 | 733 |
| First query | 120 | 266 | 239 |
| Open to the second answer | 1,039 | 1,211 | 1,195 |

Queries in process, each phase 7 times after 2 warm-ups, three processes each (ms; median of the processes'
medians with its range, and the best round of all):

| Phase (ms) | master | falcon2 HDF5 | falcon2 Zarr |
|---|---|---|---|
| Graph list (31,311 rows) | 20.2 | 18.6 | 19.6 |
| 200 graphs dumped (90,251 rows) | 151 [140..209], best 135 | 208 [181..215], best 130 | 151 [142..198], best 133 |
| 200 graphs: quad counts | 31.5, best 22.7 | 33.2, best 22.4 | 34.4, best 27.8 |
| Count of every quad | 10,333 [6,427..10,490], best 6,338 | 10,185 [9,774..10,294], best 6,768 | 10,253 [9,709..11,441], best 9,636 |
| Quads grouped by predicate | 17,323 [12,117..18,043], best 12,070 | 17,069 [13,654..18,148], best 12,677 | 17,978 [17,800..18,395], best 17,378 |

## What explains the differences

- **Falcon writes its HDF5 files as NTFS sparse files, and a mapped sparse file is slow to touch on Windows.**
  This is measured: the first touch of a page costs about 150 microseconds in the sparse file and about 1 in a
  plain copy of it. It is why the first query after opening the falcon2 HDF5 store takes 18.9 ms where a copy
  of the same file takes 3.1 ms, and why Zarr, whose chunk files are not sparse, looks faster than HDF5 there.
  See PERFORMANCE.md, "Found on the way".
- **A Zarr store is one file per array**: opening maps 57 files here instead of one, 4 to 5 ms more, and
  closing unmaps them.
- **falcon2 unmaps a store when it is closed**; master leaves that to the garbage collector, so its close is
  nearly free and its file stays mapped for a while.
- **What remains of the gap at open on the real store** (12.2 ms against 4.6 ms for the first query in Zarr,
  where neither of the above applies) is probably format v6 reading its block directories into memory when
  an index is first used. This was not profiled.
- **The native binary's slower scans** are the read path through the memory segment, one entry at a time
  (PERFORMANCE.md, item 1). `-Dbeakgraph.ffm.threshold=2147483647` reads through a ByteBuffer view instead;
  it was not used here.
- **Faster lookups and smaller stores** are format v6: per-block widths, and searches narrowed to one block
  from a directory in memory.

## How far to trust these

- **Long scans are bimodal between JVM runs, in every configuration.** The real-data count of every quad
  takes about 6.4 s or about 10.3 s depending on the run, and can change within one. So the single-run
  `-verify -deep` times on real data (593 / 570 / 430 s) do not separate the three, and neither do
  differences of a few percent in the scan phases.
- The endpoint figures are one server run per configuration.
- The real-data builds, verifies and exports are one run each; they are long enough that a few percent is
  the resolution, not more.
- One machine, one operating system. The sparse-file effect in particular is Windows and NTFS.
- The scripts that took these measurements were kept outside the repository and are not part of it.

## What would close the gaps

1. Falcon not writing sparse HDF5 files, at least on Windows: the synthetic store's open cycle goes from
   21.3 ms to 4.2 ms with a plain copy of the same file.
2. The native binary's read path: the bulk decode still open under PERFORMANCE.md's item 1, or the
   ByteBuffer view as its default.
3. Loading format v6's directories when they are first needed, or laying them out together, if profiling
   confirms they are what the first query pays for.

## Since this was measured

The tables above are as measured on commit `bf09ba1` and are not changed. Later the same day, in the working
tree:

- **Items 1 and 2 above are done.** Falcon no longer writes sparse files, and the native binary reads
  through the ByteBuffer view by default. On the synthetic store, falcon2 in HDF5 then takes 4.0 ms to open,
  answer a first query and close (master 4.4 ms), and its native binary 30.8 s for `-verify -deep`, 5.64 s
  for `-export NQ`, 1.39 s for the endpoint's count of every quad and 24.4 s for the 1,000 subject lookups
  (master 29.5 s, 5.50 s, 1.35 s, 24.4 s). The full table is in PERFORMANCE.md, "After the speed test".
- **One finding above did not hold up: the native binary's faster dumps and joins on falcon2.** In that
  re-measurement the same falcon2 binary took 4.6 s for the 200 dumps and 1.0 s for the 100 joins, where
  the table above has 2.6 s and 0.5 s; master took about 4.1 s and 0.9 s both times. What made the earlier
  run faster was not found (not the garbage collector, which was checked). Read the native binary's dumps
  and joins as equal on the two branches.
- **The sparse-file cost varies from file to file**: the first query after an open took 44.6 ms on the
  store built for the re-measurement, 18.9 ms on the one above.
- **The endpoint tables above overstate what a request costs.** Their client opened a new connection for
  each request, which on this machine adds a median 0.8 ms and stalls of about 15 ms on a tenth to a half
  of the requests; it is the likely reason for the dumps and joins that did not reproduce. On a connection
  kept alive a dump of one graph takes 3.6 ms on the JVM, not 14. Counts, grouped counts and the lookups
  across graphs are long enough to be little affected. See PERFORMANCE.md, "The endpoint's response
  buffer", which also removes 0.7 ms from every request.
