# BeakGraph — Instructions

BeakGraph converts RDF datasets into **HDF5-backed, dictionary-encoded, columnar quad stores**
that can be queried with SPARQL (via Apache Jena) directly from the `.h5` file — locally or over
HTTP range requests — without loading the graph into memory. One `.h5` file is one self-contained,
immutable store.

---

## 1. Requirements

| Requirement | Notes |
|---|---|
| Java 25+ | The build targets current JDKs. |
| Maven 3.9+ | Standard build. |
| Native HDF5 library | **Only** for the disk-based writers (`-method 1`, `4` and `5`); a missing library is reported before any parsing starts. The in-memory writers (`-method 0/2/3`) use pure-Java jHDF and need nothing native. Platform matrix: the default JavaCPP preset (`org.bytedeco:hdf5-platform` 1.14.3-1.5.10) bundles natives for **linux-x86_64 and macos-x86_64** (they load automatically) and **windows-x86_64** (its JNI glue needs a `hdf5.dll` it does not ship: install HDF5 1.14 and put its `bin` directory, `hdf5.dll` + `hdf5_java.dll`, on `PATH`, or set `-Dhdf.hdf5lib.H5.hdf5lib=<path to hdf5_java.dll>`). **No preset published so far carries arm64 natives**: on Apple Silicon or aarch64 Linux either run an x86_64 JDK (Rosetta), or install HDF5 yourself and use the HDF Group's FFM bindings (`mvn -Dhdf5.ffm=true ...`, published for linux-x86_64, windows-x86_64, macos-x86_64 and macos-aarch64 - nothing for linux-aarch64 / windows-aarch64; the build refuses to guess a classifier for an unrecognised platform). A system install on the library path is always preferred over the bundled natives. |
| Disk workspace | For `-method 1/4/5`: free space on the order of a few times the uncompressed source (see §7). |

## 2. Building

```bash
# Library + tests
mvn clean package

# Self-contained command-line jar (all dependencies shaded)
mvn -Pcmdlinejar clean package
# -> target/BeakGraph-<version>.jar
```

Run the CLI either from the shaded jar or via your own classpath:

```bash
java -jar target/BeakGraph-<version>.jar -help
```

## 3. Quick start

```bash
# Convert every RDF file under ./data to one .h5 per file under ./out
java -jar BeakGraph.jar -src data/ -dest out/

# Convert one file
java -jar BeakGraph.jar -src data/example.ttl.gz -dest out/example.h5

# Merge an entire tree into ONE store
java -jar BeakGraph.jar -src data/ -dest all.h5 -merge

# Serve a store as a SPARQL endpoint on port 8888
java -jar BeakGraph.jar -endpoint out/example.h5 -port 8888

# Serve a whole directory of stores as W3C LWS storage (writes <dir>/beakgraph.ttl.gz)
java -jar BeakGraph.jar -endpoint out/ -port 8888
```

## 4. Command-line reference

| Option | Default | Description |
|---|---|---|
| `-src <path>` | — | Source file **or** directory tree. Directories are walked recursively; every supported RDF file (see §5) is converted. |
| `-dest <path>` | — | Destination. Required with `-src`. In per-file mode: the directory mirroring the source tree with `.h5` extensions. With `-merge`: the one store to write - a name ending in `.h5`/`.hdf5` is the file (missing parents are created); anything else (an existing directory, a trailing separator, or no such suffix) is a directory that receives `merged.h5`. |
| `-method <0-5>` | `0` | Conversion engine — see §6. |
| `-cores <n>` | `4` | Threads used **inside** one conversion by `-method 2`, `3`, `4` and `5`. |
| `-threads <n>` | `1` | Number of conversions run **at once** (per-file mode). Each conversion gets its own `-cores` budget — total CPU ≈ `threads × cores`. |
| `-merge` | off | Merge **all** sources under `-src` into ONE store at `-dest` (if `-dest` is an existing directory, writes `<dest>/merged.h5`). Blank nodes stay distinct per source document, and so do relative references: each document's `<>` and relative links are stored relative to `-src` (`<>` in `a/x.ttl` becomes `a/x.ttl`, its `<img.png>` becomes `a/img.png`), so the merged store serves them below its own URL as the source tree was laid out. Works with every `-method`. |
| `-force` | off | Per-file mode: rebuild destination `.h5` files that already exist instead of skipping them. |
| `-void` | off | Generate the VoID/SD statistics graph (`urn:x-beakgraph:void`) with **exact** in-memory counting (RAM grows with distinct terms). Mutually exclusive with `-voidsketch`. |
| `-voidsketch` | off | Generate the statistics graph with **bounded memory**: exact up to 65,536 distinct nodes per counter, then HyperLogLog estimates (~0.8% error, deterministic). Recommended for `-method 1/4/5`. Mutually exclusive with `-void`. |
| `-spatial` | off | Build the Hilbert-curve spatial index for `geo:wktLiteral` geometry (adds the `urn:x-beakgraph:Spatial` graph). |
| `-features` | off | Also derive 2-D shape features (area, axes, …) for each geometry. Implies work under `-spatial`. |
| `-jsonLdRemote` | off | Let JSON-LD sources fetch `http(s)` `@context` references (30 s timeout, cached per document). Off by default: a context is loaded from the source tree - a relative reference (`"@context": "context.jsonld"`) resolves next to the document (below `-src` for `-merge`), a `file:` reference must stay inside that tree - and a remote reference fails the file with a message naming it. |
| `-workdir <dir>` | dest dir | Spill workspace for the disk-based writers (`-method 1`, `4` and `5`). Put this on your fastest disk. |
| `-spillMB <n>` | heap/8 (method 1), heap/16 (4, 5) | Disk-based writers: megabytes of parsed terms each of the three term sorters (graph, subject, object) buffers before spilling a sorted run, **whatever the record count**. This is the bound that keeps multi-KB literals (WKT geometry, long strings) from exhausting the heap; the defaults keep all three sorters within about 3/8 of the heap. |
| `-termSpillBatch <n>` | 262144 (1), 524288 (4, 5) | Disk-based writers: (term, row) records buffered per term sorter before a run spills. A run spills at the record cap *or* the byte budget, whichever comes first. |
| `-idSpillBatch <n>` | 2M (1), 4M (4, 5) | Disk-based writers: numeric id / quad records buffered per sorter before a run spills (16 bytes each, two buffers while sorting). |
| `-mergeFanIn <n>` | 64 (1), 128 (4, 5) | Disk-based writers: spill runs merged per pass (at least 2). Bigger means fewer merge levels and more open files. |
| `-huge` | off | Legacy shorthand for `-method 1`. An explicit `-method` takes precedence. |
| `-export <fmt>` | — | **Export mode**: dump the BeakGraph(s) at `-src` back to RDF instead of converting. Formats: `NT`, `NQ`, `JSON-LD`, `TTL`, `TRIG` (case-insensitive). Output lands next to each `.h5` with the same name and the format's extension. See §5a. |
| `-compress` | off | gzip the `-export` output (adds `.gz` to the file name). |
| `-verify <path>` | — | **Verify mode**: integrity-check the BeakGraph file at `<path>`, or every `.h5`/`.hdf5` under it when `<path>` is a directory (recursive). One `OK`/`FAIL` line per file plus a summary; exit code `2` if any file is damaged. See §5b. |
| `-deep` | off | With `-verify`: additionally check the whole dictionary order, materialize every triple of every graph through GSPO, reconcile against the index-derived counts, and re-derive the same rows through GPOS (reads the bulk of each file). |
| `-status` | off | Progress bar (per-file mode) and end-of-run counters. |
| `-endpoint <file.h5 \| dir>` | — | Serve instead of converting: a single store as a SPARQL endpoint at `/rdf`, or a **directory** as W3C LWS storage (see "Serving a directory" below). |
| `-port <n>` | `8888` | HTTP port for `-endpoint`. |
| `-base <url>` | derived per request | Public base URL for the links and IRIs `-endpoint` advertises, e.g. `https://data.example.org/`. By default each response uses the scheme, host and port the client reached the server on (`Forwarded` / `X-Forwarded-*` headers from a reverse proxy are honoured); set this only behind a proxy that does not forward the original host. |
| (protocol) | — | `-endpoint` honours the SPARQL 1.1 Protocol dataset parameters `default-graph-uri` and `named-graph-uri` (repeatable); when present they replace the query's own `FROM` / `FROM NAMED` clauses, and an absent graph name yields an empty dataset rather than an error. |
| `-timeout <n>` | `30` | Per-query wall-clock limit in seconds for every SPARQL endpoint the server exposes (the single-file `/rdf` endpoint, every per-`.h5` query in directory mode, and directory mode's `/rdf` metadata dataset, where Fuseki enforces it); a query over the limit is cancelled and answered with HTTP 503. `0` disables the limit. The limit bounds time, not memory: CONSTRUCT / DESCRIBE results are streamed for Turtle and N-Triples and capped for JSON-LD and RDF/XML (`beakgraph.query.construct.max.triples`, below); Graph Store `GET /rdf/data` in directory mode returns the whole metadata graph, whose size is the number of served files, not the data. Note: queries that expand large composite (`cdt:`) literals with `UNFOLD`, or compare them by value, parse the whole literal in RAM (~1 µs per element) and can hit this limit — raise it for CDT-heavy workloads. |
| `-version` / `-v` | — | Print version and exit. |
| `-help` | — | Usage text. |

**Exit codes:** `0` success · `1` bad arguments / missing paths · `2` at least one conversion failed
(each failure is also logged with its cause; per-file mode continues past failures, and a
subdirectory the walk cannot list - an ACL-denied folder, a Windows profile junction, a
junction cycle - is logged, counted as "Unreadable (skipped)" and skipped). `-verify` uses
the same scheme: `2` means at least one damaged file.

Existing non-empty destination `.h5` files are **skipped** in per-file mode (logged, counted as
"Skipped (existing)" in the summary and *not* as successful conversions; pass `-force` to rebuild
them); `-merge` always rebuilds its destination. When `-src` is a
single file, `-dest` names the output `.h5` itself, or, if it is an existing directory, the directory
to write `<name>.h5` into. Sources that differ only by RDF extension (`a.ttl`, `a.nt`) map to the
same `a.h5`: the first in path order is converted and the rest are reported and counted as failed. All writers build into a sibling `*.tmp` file and publish with an atomic rename
(if the destination cannot be replaced — on Windows a store that a reader has memory-mapped refuses
the move — the finished build is kept as `<dest>.new` and the error says so; move it into place once
the reader is closed) —
a failed or interrupted build never corrupts a previous good store.

### Serving a directory (LWS storage mode)

`-endpoint <directory>` serves the directory tree as a
[W3C LWS](https://github.com/w3c/lws-protocol) storage at `/`: every file is retrievable (with
HTTP range support), containers are listed, and every `.h5` answers SPARQL at its own URL
(`GET <file>.h5?query=...` or a `POST` with the query) through a pool of readers
(`beakgraph.pool.*` below). A metadata model describing the tree is served by Fuseki at `/rdf`
(read-only) and `/sparql` is the query UI. On first start the server generates that model and
**writes it into the served directory** as `beakgraph.ttl.gz` - the directory must be writable
(if the write fails the freshly generated model is still served, with a warning). The model is
kept current afterwards: files added, replaced or removed while the server runs are picked up
(`beakgraph.lws.refresh.seconds`), and the cache is validated against the directory on every
start; delete the file to force a full regeneration. In single-file mode a `beakgraph.ttl.gz`
sitting next to the `.h5` is loaded as the LWS model.

### Document-relative IRIs

Stores keep document-relative references (`<>`, `<sibling.png>`, `<../up.png>`, `</root>`)
**unresolved** (SPECIFICATIONS.md §9.2) and resolve them at query time against the URL the
store is served from: in directory mode against each request's URL, so a store served at
`https://host/data/x.h5` answers `<>` as that URL and `<sibling.png>` as
`https://host/data/sibling.png`; the single-file endpoint resolves against `<base>/rdf`.
Sibling references therefore keep their meaning only when the served location mirrors the
source tree (per-file conversion mirrors `-src` into `-dest`; the layout of a storage root is
the operator's responsibility). `-merge` stores each document's references relative to `-src`
(`<>` of `a/x.ttl` becomes `a/x.ttl`), so identical relative references in different documents
stay distinct resources - unlike blank nodes, which are simply scoped per document. `-export`
writes the relative forms verbatim unless `-base <URL>` is given; N-Triples / N-Quads cannot
carry them, so exporting such a store as NT/NQ without `-base` fails with a message naming the
option (§5a).

## 5. Supported source formats

Turtle `.ttl`, N-Triples `.nt`, N-Quads `.nq`, TriG `.trig`, RDF/XML `.rdf`, JSON-LD `.jsonld` —
each also as gzip (`.ttl.gz`, …) or zip (`.ttl.zip`, …; the first non-directory zip entry is read).
Named graphs require a quad-capable syntax (TriG / N-Quads). Files with other extensions are
counted and skipped.

**JSON-LD is not streamed.** Jena has no streaming JSON-LD parser: the whole document is loaded and
expanded by Titanium before its first quad is emitted, so heap is proportional to the (decompressed)
document times its expansion factor. The disk-based engines (`-method 1/4/5`) therefore bound RAM per
JSON-LD *document*, not per quad count (they log a warning naming the file), and `-method 5` parses
JSON-LD documents one at a time rather than `-cores` at once. Convert bulk JSON-LD to N-Quads first
(`riot --output=nq big.jsonld > big.nq`).

**JSON-LD `@context` references** are loaded from the source tree: a relative reference resolves
next to the document (relative to `-src` for `-merge`, so `../shared/ctx.jsonld` works inside the
tree), and a `file:` reference must name a file inside that tree. A reference that climbs above the
tree, and any `http(s)` reference, fails the file with a message naming the reference - a downloaded
document must not turn the converter into a network or file-system client. Pass `-jsonLdRemote` to
fetch remote contexts (30 s timeout), or keep a local copy next to the document and reference it
relatively.

## 5a. Exporting a BeakGraph back to RDF

```bash
java -jar BeakGraph.jar -src data.h5 -export NQ                 # -> data.nq
java -jar BeakGraph.jar -src data.h5 -export TTL -compress      # -> data.ttl.gz
java -jar BeakGraph.jar -src stores/ -export NT                 # every .h5 under stores/
```

* `-src` may be one `.h5`/`.hdf5` file or a directory tree (every `.h5`/`.hdf5` under it exports,
  the same rule `-verify` uses).
* **Automatic quad upgrade**: if `TTL` or `NT` is requested but the store holds named
  graphs beyond the default graph, the format silently upgrades to its quad form
  (`TTL -> TRIG`, `NT -> NQ`) and the extension follows.
* BeakGraph's **internal metadata graphs** (`urn:x-beakgraph:void` statistics and the
  `urn:x-beakgraph:Spatial` index) are derived build artifacts: they are excluded from
  the export and from the named-graph decision, so a plain-triples store round-trips
  to plain triples.
* NT/NQ/TTL/TRIG exports stream (any store size); JSON-LD has no streaming writer and
  materializes the dataset in memory - use NQ/TRIG for bulk dumps.
* JSON-LD has no syntax for RDF 1.2 triple terms: a store holding them is refused for
  `-export JSON-LD` (a counted failure, no output file), exactly as the SPARQL endpoint refuses
  JSON-LD results containing them. Export such stores as NT, NQ, TTL or TRIG.
* Writes are atomic (`.tmp` then rename); an existing export is replaced.

Stores keep document-relative IRIs (`<>`, `<sib.png>`, `<../x>`) in relative form. Pass
`-base <URL the store is served from>` to resolve them on export; N-Triples / N-Quads cannot carry
relative IRIs, so exporting such a store as NT/NQ without `-base` fails with a message naming the
option, while TTL/TriG/JSON-LD write them as stored (with a warning). BeakGraph's own metadata graphs
(`urn:x-beakgraph:void`, `urn:x-beakgraph:Spatial` and the spatial `urn:x-beakgraph:grid:*` tiles)
are never exported and never force a triples store up to a quads syntax.

## 5b. Verifying BeakGraph files

```bash
java -jar BeakGraph.jar -verify data.h5                 # one file
java -jar BeakGraph.jar -verify stores/                 # every .h5/.hdf5 under stores/, recursive
java -jar BeakGraph.jar -verify stores/ -deep           # + full data-level pass
```

Run this before publishing files into served storage: a truncated or partially copied
`.h5` otherwise opens fine and only fails later, at query time, when a query first
touches the damaged region (index loading is lazy).

* **Structural pass (default)**: opens each file with the real reader stack - HDF5
  superblock, `.BG` group, format version, dictionaries - then force-loads both indexes
  (`GSPO`, `GPOS`) and enumerates the graph list. This maps every dataset the readers
  use, so files cut off anywhere in metadata or data extents are caught. A non-empty
  store must contain *both* indexes (every `?s :p ?o` query needs GPOS), every index
  level must have its bitmap and id datasets with equal row counts, and a declared
  `numEntries × width` that the stored bytes cannot hold fails the open. A sample of
  each dictionary section (128 ids) must be in comparator order and findable again by
  the readers' binary search - a store sorted under a drifted comparator extracts fine
  but answers every concrete-term lookup with nothing.
* **`-deep`**: additionally checks the whole dictionary order, streams every triple of
  every graph through GSPO (resolving all terms through the dictionaries), reconciles
  the count against the index structure, and then walks GPOS: its row count, one
  predicate-bound scan per predicate and a sample of `?s p o` probes must agree with
  what GSPO produced - so corruption inside either index's data region is caught.
  Reads most of the file; budget roughly export-NT time per file.
* Output: one `OK`/`FAIL` line per file (failures list their reasons), then
  `Verified N file(s): M OK, K FAILED`.
* **Exit codes**: `0` all files pass · `1` nothing to verify (or bad path) · `2` at
  least one file is damaged. Verification continues past failures, so one run reports
  every bad file in a tree.
* A file named explicitly is verified regardless of extension; directory scans pick up
  `*.h5` and `*.hdf5` (case-insensitive). Empty files are reported as damaged.

## 6. Choosing a conversion method

| `-method` | Engine | Memory | Parallelism | Use when |
|---|---|---|---|---|
| `0` | Sequential in-memory | Whole input on heap | none | Small files, maximum simplicity. |
| `1` | Disk-based (`huge`) | **Bounded** by spill batches (records *and* bytes; JSON-LD sources excepted, §5) | none | Input too big for RAM; native HDF5 required. |
| `2` | Parallel in-memory | Whole input on heap | `-cores` | Medium files, faster than 0. |
| `3` | **Ultra** in-memory | Whole input on heap | `-cores` | Fastest option **for data that fits in RAM**: parallel parse, O(1) id maps, radix-sorted packed keys, parallel index emission. |
| `4` | **hugeUltra** disk-based | **Bounded** by spill batches (records *and* bytes; JSON-LD sources excepted, §5) | `-cores` | Multi-billion-quad builds: the `-method 1` pipeline on parallel machinery — background radix-sorted spills, packed primitive keys, grouped term runs, concurrent stages. Native HDF5 required. |
| `5` | **plaid** disk-based | **Bounded** by spill batches (records *and* bytes; JSON-LD sources excepted, §5) | `-cores` | Method 4 **plus parallel multi-file ingest**: up to `-cores` source documents parse concurrently. The fastest option for `-merge` over many files. Native HDF5 required. |

Rules of thumb: fits comfortably in heap → `-method 3`. Doesn't fit and merging many files →
`-method 5`; doesn't fit, single giant file → `-method 4`.
Methods 0/1/2 remain for compatibility, minimal-dependency, and low-memory-machine cases.

For per-file batch conversion of MANY small files, prefer `-threads N` (parallel conversions)
over large `-cores`; for one big file, all the parallelism comes from `-cores`.

## 7. Very large builds (`-method 4`/`5`, 10⁹–10¹¹ quads)

```bash
java -Xmx32g -jar BeakGraph.jar \
     -src shards/ -dest giant.h5 -merge \
     -method 5 -cores 32 -workdir /nvme/scratch -status
```

* **Workspace**: budget roughly 3–5× the uncompressed source on `-workdir`; use NVMe.
  The workspace is a temp directory created per build and removed afterwards.
* **Heap feeds spill batches, not data**: RAM stays bounded regardless of quad count (JSON-LD
  sources excepted, §5), but bigger sort runs mean fewer merge levels and much less disk churn.
  A term run spills at `-termSpillBatch` records **or** at `-spillMB` megabytes of parsed terms,
  whichever comes first - the byte budget is what keeps multi-KB WKT literals from pinning
  gigabytes (three term sorters are live at once; the defaults keep them within ~3/8 of the
  heap). With a big heap raise `-spillMB` and `-idSpillBatch` (each id record costs 16 bytes × 2
  buffers while sorting); programmatic users have `setTermSpillBytes` / `setTermSpillBatch` /
  `setIdSpillBatch` / `setMergeFanIn` on every disk-based builder. A sorter level of
  `-method 4/5` merges at most `min(cores, 1024 / (fanIn + 1))` run groups at a time
  (`setMergeConcurrency`), so open files stay near 1024 and live records near
  `concurrency x termSpillBatch` however many runs a level holds.
* **Shard your input**: `-merge` over many files is the natural way to feed 100B quads.
* **Paths**: the native HDF5 library receives the output path through JNI. Keep `-dest` and
  `-workdir` to characters of the Basic Multilingual Plane (accents are fine, emoji and CJK
  Extension B are not) and, on Windows, under 260 characters; the writers probe the exact
  output path before parsing and refuse to start otherwise, instead of failing after the sort.
* **Statistics are opt-in**: no VoID/SD graph is written unless you pass `-void` (exact,
  in-memory) or `-voidsketch` (bounded memory via HyperLogLog). Readers use the VoID
  statistics for join reordering when present and fall back to a fixed heuristic when
  absent - for big builds, `-voidsketch` buys statistics-driven query optimization at
  ~64 KiB per counter instead of holding the dictionary on the heap.
* Blank nodes are scoped per source document (labels are not preserved in the output format;
  readers regenerate labels from dictionary ranks).
* **Native binary** (`mvn -Pcmdlinenative`): it supports the in-memory engines only
  (`-method 0/2/3`) and takes Substrate VM's default heap; raise it per run with
  `beakgraph -XX:MaxHeapSize=16g ...` (the equivalent of `java -Xmx16g`).

## 8. Programmatic use

Every engine is a `BeakGraphWriter` with the same builder shape:

```java
new / *Writer*.Builder()
    .setSource(new File("data.ttl.gz"))      // or .setSources(List<File>) for a merge
    .setDestination(new File("data.h5"))
    .setSpatial(false)
    .build()
    .write();
```

Classes: `hdf5.writers.HDF5Writer` (0) · `huge.HugeHDF5Writer` (1) ·
`hdf5.writers.parallel.ParallelHDF5Writer` (2) · `hdf5.writers.ultra.UltraHDF5Writer` (3) ·
`hdf5.writers.hugeUltra.HugeUltraHDF5Writer` (4) · `hdf5.writers.plaid.PlaidHDF5Writer` (5).

Reading:

```java
try (BeakGraph bg = new BeakGraph(new HDF5Reader(new File("data.h5")))) {
    Dataset ds = bg.getDataset();   // query with Jena/ARQ as usual
}
```

### Reader tuning (system properties)

The read path caches aggressively over the immutable store; defaults suit most
workloads, and each knob trades heap for repeated-lookup speed:

| Property | Default | What it bounds |
|---|---|---|
| `beakgraph.nodetable.cache.size` | `1000000` | Node ⇄ NodeId **entry-equivalents** per direction, per open reader (query bind/materialize path). Ordinary terms cost 1; an oversized literal costs 1 per 256 chars of lexical form, so composite (`cdt:`) literals — whose cached nodes retain their parsed values (~4.5× the text) — cannot silently pin unbounded heap. |
| `beakgraph.dict.search.cache.size` | `65536` | Term → dictionary-position **entry-equivalents** per dictionary section (locate/search results, hits and misses; same oversized-literal weighting as above). |
| `beakgraph.fcd.cache.blocks` | `4096` | Decoded front-coded string blocks per FCD section, in blocks of ordinary strings (each block holds `blockSize`, typically 16, strings). The bound is by weight: a string costs 1 plus 1 per 256 characters, so a section of large literals (WKT polygons, long text) keeps proportionally fewer blocks resident. |
| `beakgraph.ffm.threshold` | `2147483647` | Dataset size in bytes above which BeakGraph FFM-maps the region itself instead of using jHDF's ByteBuffer. |
| `beakgraph.scan.parallel.threshold` | `65536` | Minimum index position range for a scan-shaped first pattern (`?s ?p ?o`, or `?s <p> ?o`) to run as a chunked PARALLEL scan on the shared worker pool. `0` (or negative) disables parallel scanning. Chunks stop on query timeout/cancel and on early close (LIMIT), and a scan whose consumer is dropped without closing it is stopped once the iterator is garbage-collected. Only a top-level execution parallelizes: a pattern re-executed per outer row (inside `OPTIONAL`, `EXISTS`, `GRAPH ?g`) runs sequentially, and a store read through an HTTP channel is never chunked (every read is serialized behind the channel's lock). |
| `beakgraph.scan.parallel.minchunk` | `16384` | Smallest chunk (index positions) a parallel scan is split into. Lower it only to force multi-chunk scans on small stores (tests). |
| `beakgraph.jsonld.remote` | `false` | What `-jsonLdRemote` sets: allow JSON-LD `@context` references to be fetched over http(s) (30 s timeout). |
| `beakgraph.parser.chunk` | Jena default (100000) | Quads per chunk the RDF parser thread hands to the ingest. |
| `beakgraph.parser.queue` | Jena default (10) | Chunks the parser thread may run ahead of the ingest; with the chunk size this bounds the parsed-but-not-ingested quads held in RAM. |
| `beakgraph.fcd.maxFragmentBytes` | `268435456` | Largest decoded size accepted for one compressed dictionary fragment (one RDF term); a header claiming more is treated as corruption before any allocation. |
| `beakgraph.pool.perKey` | `8` | Endpoint reader pool: readers a single store can have in use at once - one is held for the whole of a query's result streaming, so this is the number of concurrent queries per `.h5`. Readers are thread-safe; the cap bounds memory (each instance carries its own caches), not correctness. |
| `beakgraph.pool.maxTotal` | `256` | Endpoint reader pool: readers across all stores. |
| `beakgraph.pool.maxWait.seconds` | `5` | Endpoint reader pool: how long a query waits for a free reader before the endpoint answers `503` with `Retry-After`. |
| `beakgraph.http.readahead` | `8` | Remote reads (`HTTPSeekableByteChannel`): blocks (128 KiB each) fetched in one range request once two consecutive cache misses show a sequential scan; `0` fetches one block per request. Scattered reads always fetch single blocks. |
| `beakgraph.query.construct.max.triples` | `1000000` | Largest CONSTRUCT / DESCRIBE result served as JSON-LD or RDF/XML, formats that must be held in memory before serialization; a larger result is answered `413` with a hint to request `text/turtle` or `application/n-triples`, which are streamed without limit. `0` disables the cap. |
| `beakgraph.pool.idle.seconds` | `300` | Endpoint reader pool: idle time before an unused reader is closed and its file mapping released. |
| `beakgraph.lws.refresh.seconds` | `30` | Directory mode (`-endpoint <dir>`): how often the served directory is re-scanned for added, removed or replaced files. The cached `beakgraph.ttl.gz` metadata is validated against the directory at start-up and rewritten after every change. `0` disables the periodic scan; a request for a path that exists on disk but is not yet listed still triggers an immediate re-scan (once per new file). |
| `beakgraph.export.fastpath` | `true` | `-export NT`/`NQ` streams straight off the GSPO index with per-id text memoization (byte-identical output to the generic writer). `false` falls back to the generic StreamRDF writer. |
| `beakgraph.export.textcache` | `262144` | Object-text memo entries for the index export (cleared wholesale when full). |
| `beakgraph.log.dir` | `logs` | Directory of the rolling `beakgraph.log` the shaded jar's log4j configuration writes (50 MB per file, ten kept, gzip-rotated). |

JMH benchmarks for the read path live in `benchmarks/` (see its README) - use
them to validate any tuning against your own store shape.

### Replacing a store while it is being served

Pooled readers compare the file's identity (file key, size, modification time) with what they
opened on every borrow, so a store replaced on disk is reopened on the next query without a
restart. On Linux and macOS the usual atomic `mv store.new.h5 store.h5` is enough. On Windows a
move over a memory-mapped file is refused (access denied) while a reader maps it, but renaming the
mapped file away is allowed: `move store.h5 store.old.h5` then `move store.new.h5 store.h5`. Readers
still mapping the old file are discarded on their next borrow or after `beakgraph.pool.idle.seconds`;
`store.old.h5` may be deleted at any point (Windows lets the delete proceed and removes the file once
the last reader closes).

## 9. Output guarantees

* One HDF5 format, one reader stack, for every method (format version 5).
  v4 added the optional `langDirs` dataset for RDF 1.2 base-direction literals
  (`"x"@en--ltr`); v5 adds the optional `tripleTerms` component store for
  RDF 1.2 triple terms (`<<( s p o )>>`). v3/v4 files remain fully readable,
  while newer files are rejected by older builds with an "Upgrade BeakGraph"
  error. A store containing neither feature is byte-identical in shape to v3
  output.
* Methods 0/2/3 produce **byte-identical** stores for the same single source on
  the same platform (one format, one jHDF write sequence; the parity tests
  compare the files byte for byte - the only platform-dependent bytes are zstd
  frames, which the native and pure-Java codecs may encode differently), and
  structurally identical stores (same datasets, sizes, attributes) for merges
  and for VoID-enabled builds, where blank-node labels differ; methods 1/4/5
  produce **isomorphic** stores (blank-node labels are rank-derived rather than
  relabelled).
* See `SPECIFICATIONS.md` (repo root) for the precise on-disk format — enough
  to re-create BeakGraph files without this source tree — and
  `docs/BeakGraph-HDF5-Architecture.pptx` for the original design slides.

## Format versions

Every store records its on-disk format version (`formatVersion` on the `.BG` group; files written
before versioning count as v1). `-verify` prints it on each verdict line (`OK    path  (format v5)`),
which is how to find stores that predate a change and must be rebuilt from source. A newer version
than the running build supports is refused at open.

| Version | BeakGraph | What changed | Rebuild needed when |
|---|---|---|---|
| v1, v2 | ≤ 0.16 | first versioned layouts | the rank/select directory is incompatible: reads fall back to a linear `select1` scan (slow, correct) |
| v3 | 0.17.0 | corrected rank/select directory, wider superblock entries | never for correctness; rebuild for speed |
| v4 | 0.18.0 | `langDirs` for `rdf:dirLangString`; composite (`cdt:`) literals ordered lexically | the source contained base-direction literals (a v3 store holds them as plain `"x"@en`) or `cdt:` literals (a pre-v4 store holding them is refused at open) |
| v5 | 0.18.0 | RDF 1.2 triple terms (`tripleTerms` component store) | the source contained triple terms (a pre-v5 build could not store them) |
