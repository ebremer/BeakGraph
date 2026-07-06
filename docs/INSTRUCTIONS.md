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
| Native HDF5 library | **Only** for the disk-based writers (`-method 1` and `-method 4`). Bundled through the `hdf5-backend-*` Maven profiles / JavaCPP artifacts; the in-memory writers (`-method 0/2/3`) use pure-Java jHDF and need nothing native. |
| Disk workspace | For `-method 1/4`: free space on the order of a few times the uncompressed source (see §7). |

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
```

## 4. Command-line reference

| Option | Default | Description |
|---|---|---|
| `-src <path>` | — | Source file **or** directory tree. Directories are walked recursively; every supported RDF file (see §5) is converted. |
| `-dest <path>` | — | Destination file or directory. Required with `-src`. In per-file mode the source tree structure is mirrored with `.h5` extensions. |
| `-method <0-4>` | `0` | Conversion engine — see §6. |
| `-cores <n>` | `4` | Threads used **inside** one conversion by `-method 2`, `3`, and `4`. |
| `-threads <n>` | `1` | Number of conversions run **at once** (per-file mode). Each conversion gets its own `-cores` budget — total CPU ≈ `threads × cores`. |
| `-merge` | off | Merge **all** sources under `-src` into ONE store at `-dest` (if `-dest` is an existing directory, writes `<dest>/merged.h5`). Blank nodes stay distinct per source document. Works with every `-method`. |
| `-void` | off | Generate the VoID/SD statistics graph (`urn:x-beakgraph:void`) with **exact** in-memory counting (RAM grows with distinct terms). Mutually exclusive with `-voidsketch`. |
| `-voidsketch` | off | Generate the statistics graph with **bounded memory**: exact up to 65,536 distinct nodes per counter, then HyperLogLog estimates (~0.8% error, deterministic). Recommended for `-method 1/4/5`. Mutually exclusive with `-void`. |
| `-spatial` | off | Build the Hilbert-curve spatial index for `geo:wktLiteral` geometry (adds the `urn:x-beakgraph:Spatial` graph). |
| `-features` | off | Also derive 2-D shape features (area, axes, …) for each geometry. Implies work under `-spatial`. |
| `-workdir <dir>` | dest dir | Spill workspace for `-method 1` and `-method 4`. Put this on your fastest disk. |
| `-huge` | off | Legacy shorthand for `-method 1`. An explicit `-method` takes precedence. |
| `-export <fmt>` | — | **Export mode**: dump the BeakGraph(s) at `-src` back to RDF instead of converting. Formats: `NT`, `NQ`, `JSON-LD`, `TTL`, `TRIG` (case-insensitive). Output lands next to each `.h5` with the same name and the format's extension. See §5a. |
| `-compress` | off | gzip the `-export` output (adds `.gz` to the file name). |
| `-status` | off | Progress bar (per-file mode) and end-of-run counters. |
| `-endpoint <file.h5>` | — | Serve the store as a SPARQL endpoint instead of converting. |
| `-port <n>` | `8888` | HTTP port for `-endpoint`. |
| `-timeout <n>` | `30` | Per-query wall-clock limit in seconds for `-endpoint`; a query over the limit is cancelled and answered with HTTP 503. `0` disables the limit. |
| `-version` / `-v` | — | Print version and exit. |
| `-help` | — | Usage text. |

**Exit codes:** `0` success · `1` bad arguments / missing paths · `2` at least one conversion failed
(each failure is also logged with its cause; per-file mode continues past failures).

Existing non-empty destination `.h5` files are **skipped** in per-file mode; `-merge` always rebuilds
its destination. All writers build into a sibling `*.tmp` file and publish with an atomic rename —
a failed or interrupted build never corrupts a previous good store.

## 5. Supported source formats

Turtle `.ttl`, N-Triples `.nt`, N-Quads `.nq`, TriG `.trig`, RDF/XML `.rdf`, JSON-LD `.jsonld` —
each also as gzip (`.ttl.gz`, …) or zip (`.ttl.zip`, …; the first non-directory zip entry is read).
Named graphs require a quad-capable syntax (TriG / N-Quads). Files with other extensions are
counted and skipped.

## 5a. Exporting a BeakGraph back to RDF

```bash
java -jar BeakGraph.jar -src data.h5 -export NQ                 # -> data.nq
java -jar BeakGraph.jar -src data.h5 -export TTL -compress      # -> data.ttl.gz
java -jar BeakGraph.jar -src stores/ -export NT                 # every .h5 under stores/
```

* `-src` may be one `.h5` file or a directory tree (every `.h5` under it exports).
* **Automatic quad upgrade**: if `TTL` or `NT` is requested but the store holds named
  graphs beyond the default graph, the format silently upgrades to its quad form
  (`TTL -> TRIG`, `NT -> NQ`) and the extension follows.
* BeakGraph's **internal metadata graphs** (`urn:x-beakgraph:void` statistics and the
  `urn:x-beakgraph:Spatial` index) are derived build artifacts: they are excluded from
  the export and from the named-graph decision, so a plain-triples store round-trips
  to plain triples.
* NT/NQ/TTL/TRIG exports stream (any store size); JSON-LD has no streaming writer and
  materializes the dataset in memory - use NQ/TRIG for bulk dumps.
* Writes are atomic (`.tmp` then rename); an existing export is replaced.

## 6. Choosing a conversion method

| `-method` | Engine | Memory | Parallelism | Use when |
|---|---|---|---|---|
| `0` | Sequential in-memory | Whole input on heap | none | Small files, maximum simplicity. |
| `1` | Disk-based (`huge`) | **Bounded** by spill batches | none | Input too big for RAM; native HDF5 required. |
| `2` | Parallel in-memory | Whole input on heap | `-cores` | Medium files, faster than 0. |
| `3` | **Ultra** in-memory | Whole input on heap | `-cores` | Fastest option **for data that fits in RAM**: parallel parse, O(1) id maps, radix-sorted packed keys, parallel index emission. |
| `4` | **hugeUltra** disk-based | **Bounded** by spill batches | `-cores` | Multi-billion-quad builds: the `-method 1` pipeline on parallel machinery — background radix-sorted spills, packed primitive keys, grouped term runs, concurrent stages. Native HDF5 required. |
| `5` | **plaid** disk-based | **Bounded** by spill batches | `-cores` | Method 4 **plus parallel multi-file ingest**: up to `-cores` source documents parse concurrently. The fastest option for `-merge` over many files. Native HDF5 required. |

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
* **Heap feeds spill batches, not data**: RAM stays bounded regardless of quad count, but bigger
  sort runs mean fewer merge levels and much less disk churn. Programmatic users can raise
  `setIdSpillBatch` / `setTermSpillBatch` on `HugeUltraHDF5Writer.Builder` (defaults: 4M id
  records / 512K term records per run; each id record costs 16 bytes × 2 buffers while sorting).
* **Shard your input**: `-merge` over many files is the natural way to feed 100B quads.
* **Statistics are opt-in**: no VoID/SD graph is written unless you pass `-void` (exact,
  in-memory) or `-voidsketch` (bounded memory via HyperLogLog). Readers use the VoID
  statistics for join reordering when present and fall back to a fixed heuristic when
  absent - for big builds, `-voidsketch` buys statistics-driven query optimization at
  ~64 KiB per counter instead of holding the dictionary on the heap.
* Blank nodes are scoped per source document (labels are not preserved in the output format;
  readers regenerate labels from dictionary ranks).

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
| `beakgraph.nodetable.cache.size` | `1000000` | Node ⇄ NodeId entries per direction, per open reader (query bind/materialize path). |
| `beakgraph.dict.search.cache.size` | `65536` | Term → dictionary-position entries per dictionary section (locate/search results, hits and misses). |
| `beakgraph.fcd.cache.blocks` | `4096` | Decoded front-coded string blocks per FCD section (each block holds `blockSize`, typically 16, strings). |
| `beakgraph.ffm.threshold` | `2147483647` | Dataset size in bytes above which BeakGraph FFM-maps the region itself instead of using jHDF's ByteBuffer. |
| `beakgraph.scan.parallel.threshold` | `65536` | Minimum index position range for a scan-shaped first pattern (`?s ?p ?o`, or `?s <p> ?o`) to run as a chunked PARALLEL scan on the shared worker pool. `0` (or negative) disables parallel scanning. Chunks stop on query timeout/cancel and on early close (LIMIT). |

JMH benchmarks for the read path live in `benchmarks/` (see its README) - use
them to validate any tuning against your own store shape.

## 9. Output guarantees

* One HDF5 format, one reader stack, for every method (format version 3).
* Methods 0/2/3 produce **structurally identical** stores for the same single source
  (same datasets, sizes, attributes); methods 1/4 produce **isomorphic** stores
  (blank-node labels are rank-derived rather than relabelled).
* See `docs/BeakGraph-HDF5-Architecture.pptx` for the on-disk format design.
