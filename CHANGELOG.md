# Changelog

Notable changes to BeakGraph and its on-disk format. The format history is
normative (SPECIFICATIONS.md §4.1); the rest is a summary of what a release
brought and what a reader must rebuild.

## 0.20.0 - format v6 (2026-11)

* **Every pyramid level is on the integer grid, a repaired one too**
  (SPECIFICATIONS.md, spatial (B/C)). A part that is invalid after integer
  snapping is repaired (JTS `buffer(0)`, keeping the largest lobe), and the
  repair nodes a self-crossing ring where its edges cross, rarely at a whole
  pixel: such a level was written with fractional vertices, e.g.
  `POLYGON ((5838.666666666667 3535.5, 5842 3533, ...))`, which Halcyon's tile
  renderer (it read integers only) threw on, dropping the rest of that tile's
  grid cell. The repaired polygon is now reduced to the integer grid by
  snap-rounding (`GeometryPrecisionReducer`, which keeps it valid; the largest
  part is kept if that splits it, and a part with nothing left on the grid ends
  its pyramid with a warning). The format is unchanged and so are the stores of
  geometries that needed no repair; rebuild a store to replace fractional
  levels (Halcyon reads both since next `6bb5d1e`).

## 0.19.0 - format v6 (2026-11)

* **Zarr: one file, over HTTP, in a storage directory, and past 1 GiB**
  (SPECIFICATIONS.md §3a; docs/INSTRUCTIONS.md §4a). Four things the Zarr
  container did not do, and HDF5 did:
  - **A Zarr store in ONE file.** `-zip`, or a destination named `*.zip`
    (by convention `name.zarr.zip`), writes the store as a ZIP archive whose
    entries are the directory store's files, none compressed
    (`Container.ZARR_ZIP`). An entry is then the chunk's bytes, and the
    reader memory-maps each where it lies in the archive: the same views,
    answers and speed as the directory. Every engine writes it, and the same
    archive byte for byte; every command reads it; `unzip` gives the
    directory store back. An archive with a compressed chunk is refused with
    a message that names the rule. A store is found by what is at its path
    (a directory, a ZIP archive, an HDF5 file), whatever it is called.
  - **A Zarr store over HTTP, through a cache.** `BG.getBeakGraph(URI)` and
    the reader pool open a URL whose path ends in `.zarr` as a Zarr store:
    one document to open it, then each array by ranges, a block at a time,
    through ONE block cache for the whole store
    (`beakgraph.zarr.cache.block`, `beakgraph.zarr.cache.blocks`; 32 MiB by
    default). Each chunk is read through an `HTTPSeekableByteChannel` - its
    retries, its read-ahead, its pinned validator, so a chunk replaced on
    the server fails loudly - and the channels share the cache and one
    client. A chunk's channel is told its size (`HTTPSeekableByteChannel
    .sized`), which the array declares, so no request is spent on looking
    at a chunk before reading it. Before, `ZarrStore.open(store, name)` sent a request for every
    read; it now reads any Falcon store through the same cache.
  - **A Zarr store in an LWS storage directory is a store.** `-endpoint
    <directory>` listed a `*.zarr` directory as a container holding its
    `zarr.json` (the `.BG` directory is hidden) and answered no query for
    it. It is now one data resource, queried with SPARQL at its URL as an
    `.h5` file is; a GET of it returns its root `zarr.json`; and its keys
    (metadata documents and chunks, nothing else in the directory) are
    served by range, so another BeakGraph can read the store in place. A
    `*.zarr.zip` archive in a storage directory answers queries too.
  - **An array of several chunks is read where its values lie.** An array
    past 1 GiB is cut into chunks, and was read through its mappings a
    range at a time, copying each value out through a scratch buffer
    (`ChannelBytes`). `SegmentedBytes` reads each value straight from the
    chunk it lies in, and such an array is no longer treated as remote (its
    accelerators are used).
  Needs Falcon 0.3.0 (`ZipStore.map`: an uncompressed entry as a
  mapped view of the archive). Not done: a ZIP archive read over HTTP; a
  check, while a remote Zarr store is open, that the store as a whole was
  not replaced (each chunk is checked from the moment it is first read).

* **`GROUP BY` and the aggregates are computed on ids, by the threads that
  read the rows.** Jena grouped every row on the one thread that asked, as
  terms: however many workers read a pattern's rows, they handed them to
  that thread one by one. `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` and
  `COUNT(DISTINCT ?v)`, with plain variables as keys, over patterns the
  engine walks (also under `FILTER`, `OPTIONAL` and `UNION`) in one graph,
  under `GRAPH <iri>`, under `GRAPH ?g` or in a union of those, are now
  computed by `GroupOnIds`: a group is found by the row's ids, counts are
  taken from the ids, and sums and extremes are kept by Jena's own
  accumulators in each worker and combined when the workers are done.
  Workers evaluate the block's filters themselves, read values by id
  instead of through Jena's JVM-wide cache of values (where they would wait
  for each other), and a predicate's scan is divided among them by its rows
  where its objects are too few to divide by. On a store of 4.0M quads in
  5,000 graphs a scan of every quad is counted in 20 ms where it took
  411 ms, 800,000 values are summed in 14 ms (113 ms), counted distinct in
  6 ms (110 ms) and grouped by tag in 5 ms (118 ms), and a join of 200,000
  rows is counted in 15 ms (91 ms). On one graph of 2.0M quads a filtered
  count takes 14 ms (349 ms) and the rows of each type are counted in
  3.5 ms (68 ms). A query that reads little stays on one thread
  (`beakgraph.group.handoff`); lookups, dumps and the counts the index
  holds take what they took. The answers are Jena's - the tests hold them
  to its group over the same rows and to plain ARQ, on 375 queries - with
  one thing to know: workers add a floating-point `SUM` or `AVG` in another
  order than one thread does, so its last digits can differ. Still Jena's
  group, on one thread: a key that is an expression, `SAMPLE`,
  `GROUP_CONCAT`, `DISTINCT` inside an aggregate other than
  `COUNT(DISTINCT ?v)`, the union graph and `FROM` lists.
  `beakgraph.group.fastpath=false` switches it off.
* **A cancelled query is noticed while a block reads.** A walk over the
  named graphs asked for the cancel signal when it gave its consumer a row;
  a join or a filter that read for long without one - millions of rows
  turned away - ran to the end of its graph first. It now asks every
  thousand rows it puts to a later pattern or a filter.
* **The term of an id is found through pages of consecutive ids, not a
  Caffeine cache.** Every value a query reads is the term of an id, asked of
  the node table. That direction of the table was a Caffeine cache keyed by a
  boxed id, which kept its eviction policy up to date on every read and did
  its upkeep on the JVM's common pool. It is now a table of pages
  (`IdNodeCache`): the terms of 16 consecutive ids share an array, found
  through a table that is itself in the order of the ids, so a query that
  goes through ids in order reads memory in order, and a hit writes nothing.
  On a store of 4.0M quads `-verify -deep` takes 4.0 s where it took 5.8 s,
  on 17 s of processor time where it took 36 s, and in the native binary
  11.4 s where it took 28.4 s; a count of 800,000 values at the endpoint
  takes 110 ms (122 ms). Lookups, dumps, group-bys and `-export` take what
  they took. One lookup by id costs 6 to 39 ns where it cost 8 to 75 ns, and
  no more from 16 threads at once, where it cost 36 to 77 ns. `beakgraph.nodetable.cache.size` bounds the table as before, in the
  same weights; over the bound whole pages go, a tenth of the bound at a
  time, where Caffeine chose entries by how often and how lately they were
  asked. Term to id is still a Caffeine cache.
* **`GRAPH ?g { ... }` is walked on ids with a `FILTER`, `OPTIONAL` or `UNION`
  inside it too.** The walk over the named graphs took a plain pattern only;
  any other block ran graph by graph through Jena. It now evaluates the
  block's patterns, filters, optionals and unions itself in each graph (and
  what ARQ makes of `FILTER(?x = <iri>)` and `FILTER(a || b)`). On a store of
  5,000 graphs a subject lookup with a filter in the block takes 1.9 ms where
  it took 8.4 ms, one with an optional 1.4 ms (6.4 ms), one with a union
  2.5 ms (10.1 ms); 200,000 rows with an optional are counted in 54 ms
  (655 ms). The rows are the same. Still Jena's: a block with `MINUS`, `BIND`,
  a sub-select, a property path or `EXISTS`, an optional or join ARQ does not
  run row by row, a filter on `geof:sfIntersects` in a store with the spatial
  index, and a block that is more than a pattern and names `?g` itself - a
  filter inside the block must not see `?g`, which SPARQL joins afterwards.
* **A walk over many graphs hands them to parallel workers** once it has
  produced `beakgraph.scan.parallel.threshold` rows (65,536), where a row
  costs more than reading it: a join, an optional. Parallel scans were planned
  for one graph of that many positions, so a store of thousands of small
  graphs ran its joins on one thread: a join of 200,000 rows across 5,000
  graphs now takes 88 ms where it took 223 ms. A single pattern's rows stay
  on one thread - workers made such scans 13 to 17% faster at best and 12%
  slower at worst, since one thread consumes the rows either way - and so do
  a lookup, a `LIMIT` met early, and a block whose filter is not its
  outermost operator.
* **Rows bind their new variables without looking for them first.** Every row
  an iterator emitted asked, for each of its variables, whether the binding
  under it already held one; the answer is the same for every row of the
  iterator, and is now found once. A scan of 4.0M quads across 5,000 graphs
  takes 0.51 s where it took 0.84 s (workers not involved), a lookup without
  a graph 4 to 9% less. The reader also looks a pattern position up in the
  binding once where it looked twice.
* **`GRAPH ?g { ... }` over a plain pattern is one walk over the named graphs
  on ids.** Jena executed the pattern anew in every graph, building a graph
  view, an execution context and an iterator pipeline for each; a store of
  tiled data holds thousands of graphs, and for a pattern found in few of them
  that setup was the query. The walk binds `?g` to each graph's id and reads
  the pattern with the id-level iterators. On a store of 5,000 graphs a
  subject lookup without a graph takes 2.2 ms where it took 7.0 ms, a two-hop
  join from a subject 1.7 ms (9.9 ms). The rows are the same: a block with a
  `FILTER`, `OPTIONAL` or `UNION` inside it, a `FROM NAMED` dataset and a row
  that binds `?g` already run as before, and
  `-Dbeakgraph.graphvar.fastpath=false` leaves every such pattern to Jena.
* **Counts over every named graph, and counts by graph or by predicate, come
  from the index.** `COUNT(*)` over `GRAPH ?g { ?s ?p ?o }` (with
  `COUNT(?x)`, `COUNT(DISTINCT *)`, `COUNT(DISTINCT ?g)` and
  `COUNT(DISTINCT ?p)`), and `GROUP BY ?p`, `GROUP BY ?g` and
  `GROUP BY ?g ?p` over one graph's or every named graph's `?s ?p ?o`, are
  read off the two indexes without visiting a row: a count of the 4.0M quads
  of 5,000 graphs in 3 ms where it took 0.64 s, the quads by predicate in
  7 ms (1.1 s), by graph in 18 ms (0.68 s). Other groups and counts
  (`GROUP BY ?s`, a count of distinct subjects across graphs, a `FILTER`)
  are computed from the rows as before.
* **The endpoint gathers a response in a 64 KiB buffer**
  (`beakgraph.http.output.buffer`), where Fuseki's setting gave every response
  a 5 MiB one. The server's buffer pool keeps no buffer that large, so each
  response - the 133 bytes of an ASK too - had 5 MiB allocated, cleared and
  freed for it: about 0.7 ms of every request, and 5 MiB held while it ran. On
  a connection kept alive an `ASK { }` now takes 0.26 ms where it took 0.93 ms,
  a lookup in one graph 0.35 ms (1.05 ms), a two-hop join 0.53 ms (1.21 ms)
  and a dump of 801 rows 2.9 ms (3.6 ms); the native binary gains the same
  0.7 ms a request. Serving a store's file or its byte ranges in directory
  mode costs what it did. What changes besides: a response is on the wire once
  64 KiB of it are written, where it was 5 MiB, so a failure that surfaces
  later than that - a timeout deep into a large result - ends in an aborted
  transfer where it was still answered with a status code. One within the
  first 64 KiB is answered with its status as before.
* **The native binary reads a mapped store through a ByteBuffer view** of its
  memory segment, as it did before the move to Falcon: `beakgraph.ffm.threshold`
  defaults to `2147483647` in the GraalVM native binary and stays `0` on a JVM.
  Since that move the binary had been 30-60% slower at whatever scans a store,
  most of it from reading through the segment: on 4.0M quads `-verify -deep`
  took 41.1 s and now takes 30.8 s (29.5 s before Falcon; 37.4 s through the
  segment on a store that is not sparse, see the next entry), `-export NQ`
  7.52 s and now 5.64 s (5.50 s), and the endpoint's count of every quad 2.18 s
  and now 1.39 s (1.35 s). Arrays past 2 GiB are read through the segment as
  before, and `-Dbeakgraph.ffm.threshold=0` brings it back for all. A JVM reads
  as it did.
* **A store is no longer written as an NTFS sparse file.** Falcon's HDF5 writer
  asked for one, and on Windows a file created sparse stays slow to read through
  a memory mapping, with every byte of it written: the first query after opening
  a 62 MB store took 19 to 45 ms where it takes 3 ms now, as it did before
  Falcon. Queries after the first, and Zarr stores, were not affected. An HDF5
  store written on Windows by an unreleased build since the move to Falcon is
  made fast by copying it (a copy, not a move). Needs Falcon 0.2.0 as it is now
  (its P2 WF12); build times and the bytes of a store are unchanged.
* **One store, two containers: Zarr beside HDF5** (SPECIFICATIONS.md §3a). A store
  - groups, named byte arrays, integer attributes - can be written as a Zarr v3
  directory store instead of an HDF5 file: `-zarr`, or a destination named
  `*.zarr`, with every engine. It is the same store, the same arrays byte for
  byte and the same attributes, so the format version does not change; a test
  holds the two containers to each other through Falcon's general HDF5 and Zarr
  readers. Each array is a `uint8` Zarr array stored with the `bytes` codec
  alone, in one chunk up to 1 GiB (several beyond), which a reader memory-maps
  as it maps an HDF5 dataset, so queries read both alike. `BG.getBeakGraph`,
  `-verify`, `-export`, `-endpoint` (one store) and the pool take a `*.zarr`
  directory as one store. The readers and writers now work on
  `com.ebremer.beakgraph.store` (`OpenStore`, `StoreGroup`, `StoreArray`,
  `WritableStore`, `Container`) and no longer see an HDF5 library. A Zarr
  store is built beside its destination and moved into place whole, and a
  build replaces no directory but a Zarr store or an empty one. Not there
  yet: Zarr stores read over HTTP or from an object store through the
  endpoint and the pool; Zarr stores served from inside an LWS storage
  directory; and an array past 1 GiB is read slower than in HDF5. Needs
  Falcon 0.2.0 (`Store.map`, `ZarrArray.chunkSegment`).
  docs/INSTRUCTIONS.md §4a.
* **Format v6: block-structured id sequences** (SPECIFICATIONS.md §5.1a). The id
  sequences of both indexes (`Ss`/`Sp`/`So`) and the dictionary's role lists
  (`graphs`, `subjects`, `objects`) are stored in blocks of 1,024 entries
  (more for sequences past four billion entries), each with a base and a
  width of its own, with a directory dataset `<name>_blocks` beside each
  that a reader keeps in memory. An entry is still one read; a search in a
  long sorted run is narrowed to one block without reading the file; and a
  run of nearby ids costs the bits of their spread, not of the largest id
  in the store. The rank/select directory's superblock counters are kept in
  memory from a level's first lookup on. Stores of format v3-v5 read as
  before; a store written now is not readable by earlier releases, which
  refuse it by its version. All six engines write the same bytes for the
  same sequence, as before. See PERFORMANCE.md for what this is the first
  step of, and what it measured.
* Jena 6.2.0.
* The HDF5 library of every reader and every writer is
  [Falcon](https://github.com/ebremer/falcon) 0.1.0, in place of jHDF for the
  readers and the in-memory writers (`-method 0/2/3`) and of the native HDF5
  library for the disk-based ones (`-method 1/4/5`). No HDF5 library has to
  be installed on any platform, and none is bundled. jHDF remains a test
  dependency (the tests' independent reader). The on-disk format is
  unchanged: earlier releases read the stores written now, and the other
  way round.
  A store opened from a local file is memory-mapped once, whole, and
  unmapped when its reader closes: the file is free to replace or delete
  from then on, on Windows too, and a reader must not be closed while a
  query is still reading through it. Every dataset of a mapped store is read
  through its FFM memory segment (`beakgraph.ffm.threshold` now defaults to
  `0`; it was 2 GiB): the linear bitmap scans of format v2 stores run faster
  that way, and current-format stores as fast as before. The in-memory
  writers write each
  dataset to the file as the next is added, and force the file to disk
  before it is published. Falcon is not on Maven Central yet: install it
  locally before building (docs/INSTRUCTIONS.md §2).
* The disk-based writers (`-method 1/4/5`, `-huge`) assemble their store
  through Falcon, a piece of a dataset at a time, where they called the
  native HDF5 library (`hdf.hdf5lib`, the JavaCPP preset or the HDF Group's
  FFM bindings). They no longer depend on a platform's native library (the
  JavaCPP preset had none for arm64 and needed a system `hdf5.dll` on
  Windows), and they run in the GraalVM native binary, which now runs every
  engine. `-dest` and `-workdir` may be any path the file system takes
  (the library took paths across JNI: no characters beyond the Basic
  Multilingual Plane, 260 characters on Windows). The store is written
  beside its path and forced to disk before it is published, like the
  in-memory writers'. Their stores hold the same bytes in every dataset
  as before; only the HDF5 metadata around them differs. Gone with the
  library: the `org.bytedeco:hdf5-platform` dependency and its natives in
  the shaded jars, the `hdf5-backend-*` and `hdf5-ffm-*` build profiles,
  `-Dhdf5.ffm`, and `com.ebremer.beakgraph.huge.NativeHdf5File`
  (`FalconHdf5File` takes its place behind `StreamingHdf5`).
* LWS storage mode follows the LWS Protocol editor's draft of 2026-11-09 for
  everything a read-only storage serves. Every GET/HEAD response links the
  storage URI (`/description`) with `rel="https://www.w3.org/ns/lws#storage"`
  (was `…#storageDescription`); the storage URI serves the storage
  description as `application/lws+cid`, a Controlled Identifier document
  (`@context` `[cid/v1, lws/v1]`, `id` the storage URI) with the mandatory
  `StorageRoot` service beside the SPARQL endpoint. Container items carry
  `format` (was `mediaType`) and the JSON body no longer mirrors pagination
  as `as:first`/`as:next`/… (Link headers only). A request for
  `application/ld+json` or `text/turtle` with the LWS profile
  (`profile="https://www.w3.org/ns/lws/v1"`) gets a representation RDF
  isomorphic to the `application/lws+json` one, with the profile in its
  `Content-Type`; plain Turtle remains the model listing with navigation and
  is not labelled with the profile. Linksets hold only link relations (type, up,
  linkset, storage) and, like the description, carry ETag and Last-Modified.
  PUT, PATCH, DELETE, TRACE and POST (except a SPARQL query to a store)
  answer 405 with `Allow`; OPTIONS reports it; errors are RFC 9457 problem
  details; If-Match and If-Unmodified-Since are evaluated (412); a
  container's HTML, Turtle and JSON listings have distinct ETags. Single-file
  mode no longer serves a storage description - it has no storage. The
  metadata model (`beakgraph.ttl.gz` and the directory-mode `/rdf` dataset)
  uses the LWS vocabulary's `dcterms:format`, `dcterms:modified` and
  `lws:totalItems` instead of `as:mediaType`, `as:updated` and
  `as:totalItems`: a cache written with the old terms is regenerated at
  start-up, and SPARQL queries over `/rdf` naming them must be updated.
* Vendored zstd: the pure-Java compressor keeps one compression context per
  window size and reuses it across frames (hash/chain tables, sequence store
  and entropy workspaces used to be allocated for every compressed dictionary
  fragment); frames are byte-identical to before, and a compressor instance
  is now explicitly single-threaded (one per writer, one per reader thread,
  as before).
* Housekeeping: the bit-packed buffer's word fetch dispatches on the sealed
  `RandomAccessBytes` implementation (a JVM that had opened small, FFM-mapped
  and remote datasets paid a megamorphic call on every id lookup: +57% on a
  random get, +42% on a binary search in the benchmark); the buffer's
  write-side constructor is `(Path, int bitWidth)` and its cursor reads,
  `HDTBitmapDirectory.getIds()`, `DataOutputBuffer.writeInt`,
  `NodeSorter.sort2` and UTIL's binary-dump helpers are gone; the in-memory
  buffers declare no checked exceptions on construction or close; `Stats`
  reports "-" and the allocated width instead of its seed sentinels; the
  native HDF5 backend names the full path in a failed create; dead code and
  stale javadoc removed (BGVoIDSD, OpExecutorBG).
* Alternative writer engines: one publish discipline (AtomicPublish.build)
  for all six engines, one spill/merge scaffolding for the two -method 4
  sorters, one index level-emission rule for methods 0/1/2 and one id-space
  helper for the three dictionary writers; the packed sorters reject an id
  wider than its declared width; the ultra index pads with word-wise range
  fills; a remote JSON-LD context is fetched once per process, shared and
  coalesced across concurrent parses; the ultra ingest no longer shadows the
  base builder's configuration and refuses build(); the disk builders share
  a base with documented defaults; docs state what runs outside `-cores`.
* Default writer engine: the GSPO/GPOS indexes sort and scan dictionary ids
  resolved once per quad instead of re-comparing nodes and re-searching the
  dictionaries (same bytes, much less work); the dictionary build refuses two
  entries the comparator cannot separate; a spatial literal's later parts keep
  their index cells when an earlier part's pyramid fails; a missing language
  tag fails the build instead of storing "no tag"; the VoID `sd:Dataset` IRI
  is configurable (`-voidbase`, `setVoidDatasetIri`) and defaults to
  `urn:x-beakgraph:dataset` instead of the author's domain; a writer snapshots
  its builder and an ingest builder is single-use; parse and guard failures
  keep their own message instead of "I/O error while reading RDF source";
  exports use a unique temp name; predicates are guarded at ingest.
* Core library and utils: a zip source's document is chosen by name over the
  whole archive (`__MACOSX/` and dot-file entries skipped, an RDF-named entry
  preferred, several candidates rejected) instead of taking the first file
  entry; source streams are buffered and a rejected `.gz` releases its file
  handle; an empty polygon member no longer indexes as Hilbert cell 0; VByte
  rejects sequences over nine bytes; a corrupt zstd frame surfaces as one
  `IllegalArgumentException` whichever codec (native or Java) decoded it, and
  the docs state that compressed bytes depend on the codec
  (`io.airlift.compress.v3.disable-native=true` forces the Java one).
* The reference comparator is a strict total order: numbers of every XSD
  numeric datatype order by exact value (range pushdown widens its bounds to
  ARQ's promoted comparison), dateTime and the g* kinds share one instant
  space with a fixed kind rank, and cross-store bindings are re-resolved.
  Stores built by earlier 0.18.0 builds that mix numeric datatypes or g* and
  dateTime literals under one predicate should be rebuilt.
* Relative IRIs resolve against a deep sentinel base, so `<../x>` and `</x>`
  keep their parent / path-absolute forms; `-merge` stores each document's
  references relative to `-src`. Earlier stores collapsed them onto the child
  form (SPECIFICATIONS.md §9.2).
* Composite (cdt:) literal constants no longer drive range pushdown (their
  dictionary order is lexical, ARQ's is by value).
* Disk-based writers: term sorters spill on a byte budget as well as a record
  count, and the spill sizing is on the command line (`-spillMB`,
  `-termSpillBatch`, `-idSpillBatch`, `-mergeFanIn`); JSON-LD sources, which
  Jena parses whole in memory, are warned about and parsed one at a time
  under `-method 5`; every engine sorts literals through a per-sort
  memoizing comparator; the native HDF5 backend writes attributes through
  the `byte[]` entry points, so the `-Dhdf5.ffm=true` binding works.
* `-verify` requires both indexes of a non-empty store, checks every index
  level's datasets and declared sizes, and probes dictionary order and
  searchability (all of it under `-deep`, which now also re-derives every
  graph's rows through GPOS); the `-src` walk skips unreadable subtrees and
  junction cycles instead of aborting the run.
* `-method 4/5`: a failed build drains its spill, merge and parse workers
  before removing the workspace (no stranded `.bghugeultra-*` /
  `.bgplaid-*` directories); merge levels run at most
  `min(cores, 1024 / (fanIn + 1))` groups at a time (`setMergeConcurrency`);
  the HDF5 backend is probed before parsing. `-method 3` releases its packed
  key arrays and rank maps before writing the file.
* Disk-based writers: a failed stage cancels its sibling stages at once; a
  spill run or record file cut short is reported instead of read as a
  shorter run; every workspace file is closed on failure; the exact output
  path is probed before parsing; constructors release what they opened. All
  engines share one relativizer, one literal-stats routing, one spatial
  augmenter and one dictionary node encoder. Every builder's `build()`
  names a missing source or destination.
* CLI: a bare or mode-less invocation fails with usage (exit 1) instead of
  exiting 0 silently; `-port` is validated; `-merge` treats a trailing
  separator or a suffix-less `-dest` as a directory; `-export` and the LWS
  servlet accept `.hdf5` like `-verify`; `-export JSON-LD` refuses triple-term
  stores; `-force` rebuilds existing destinations and skips no longer count
  as successes; `-verify` reports a lost or unreadable `formatVersion` and,
  under `-deep`, rows exceeding `numQuads`.
* The endpoints honour the SPARQL Protocol `default-graph-uri` /
  `named-graph-uri` parameters; `-export -base`; `-verify` of degenerate
  stores; the empty-store export fast path; locale-independent identifiers
  and case mapping throughout.
* Query engine: `FROM` / `FROM NAMED` (and the protocol dataset parameters)
  now run on BeakGraph views - a graph-set view for several `FROM` graphs -
  instead of Jena's `GraphUnionRead`, keeping id-level joins, pushdown and
  the fast paths; `?s ?p <o>` is answered by per-predicate GPOS probes
  instead of a graph scan; range and spatial-index hints survive filter
  placement after join reordering (they were dropped whenever the placed
  filter ended up inside an OpSequence). SPECIFICATIONS.md §8.5 gains the
  access-path table. A parallel scan whose consumer is dropped unclosed is
  stopped once it is garbage-collected (its workers used to pin it), and
  scans are planned only for a top-level execution - not per outer row of an
  OPTIONAL / EXISTS / `GRAPH ?g` sub-pattern - and never for a store read
  through an HTTP channel; the spatial-index candidate collection runs at
  the first row and honours query timeout / abort; the ARQ-global stage
  generator (general datasets holding BeakGraph views) reorders BGPs like
  the BG executor; a dictionary read failure while snapping a range filter's
  bound now fails the query instead of silently narrowing the range. Range
  pushdown skips mixed-duration constants (`"P1M35D"`), whose XSD order the
  months-first dictionary order can reverse. FFM-mapped datasets (over the
  `beakgraph.ffm.threshold`) are mapped through jHDF's open channel, so a
  store rebuilt in place while a reader is open stays consistently on the
  old file instead of decoding old metadata against new bytes.
* Query engine: FILTER range hints are resolved once per store and pattern
  shape (`RangeBounds`, signed with clamped floors) and shared by every
  iterator, join input row and parallel-scan chunk, replacing four drifted
  copies; the object-side iterator binary-searches a subject hint; a chunk
  of a closed scan skips its setup; the first-level and child block-range
  rules live in one place; the export's text memo clamps an oversized cache
  setting and indexes predicate text by id; the node table resolves a miss
  with one cache operation; a polygon outside the Hilbert domain keeps its
  clamped cover; the third `geof:sfIntersects` argument is refused instead
  of ignored; row bindings compute their own variables once.
* Server: the LWS servlet's public base and storage root are per instance;
  `Accept` is negotiated by media range and quality (a plain
  `application/json` request is labelled as such); `If-None-Match` accepts
  lists, weak validators and `*`; data responses open the file before deriving
  the validator, are served from that handle, carry `Cache-Control: no-cache`
  and an RFC 6266/8187 `Content-Disposition`; a stored entry named `*.meta`
  is reachable and only the exact `HalcyonStorage` segment is an alias;
  container listings are memoised per metadata snapshot and report live file
  sizes and times; hidden entries and root entries named like a fixed route
  are not indexed; `void:entities` counts a multi-typed entity once; the
  user-profile JSON-LD frame is applied by the response filter instead of a
  JVM-global writer hook, never fetches a context and never closes the
  response stream; HDF5 files are typed `application/x-hdf5` and the common
  RDF, text and image extensions get their media types. INSTRUCTIONS gains
  "Deployment and trust model".
* Writers: JSON-LD `@context` references load from the source tree (a
  relative reference next to the document, or below `-src` for `-merge`,
  used to resolve against the sentinel base and fail); remote contexts are
  refused unless `-jsonLdRemote` is given, and then fetched with a timeout;
  a `file:` context must stay inside the tree. A failed ingest closes the
  parser stream, so Jena's parser thread no longer stays parked on its
  queue. Document-relative datatype IRIs (`"7"^^<scoreType>`) are stored
  relative and served resolved instead of leaking the sentinel host; a
  relative IRI inside a composite (`cdt:`) literal is rejected at ingest
  like a blank node. Spatial indexing covers every leaf of a nested
  GEOMETRYCOLLECTION (a nested MULTIPOLYGON was unfindable), and the tile
  pyramid skips levels over 65,536 tiles instead of enumerating a wide
  geometry for hours.
* Comparator: a literal whose value cannot be built is classified as
  unparseable against every partner and logged once per datatype; a
  comparison that fails is an error. The former catch-all ordered that one
  pair by term - the cyclic value/term mix the CDT and language branches
  avoid. The cross-space rank table (SPECIFICATIONS.md §6.2) and the
  `DataType` ordinal table (§7.4) are pinned by tests.
* Endpoints: `-timeout` now also bounds directory mode's `/rdf` metadata
  dataset; CONSTRUCT / DESCRIBE stream as Turtle and N-Triples and are capped
  for JSON-LD and RDF/XML (`beakgraph.query.construct.max.triples`); ASK
  answers carry the mandatory `head` member and honour `Accept` (XML by
  default, like SELECT); a query may name the served document by relative
  reference (`<>`, `<image.png>`); the storage description advertises `/rdf`
  as the SPARQL service and `/sparql` redirects to `/sparql/`; LWS ids, Link
  targets and RDF subjects are percent-encoded; the JSON-LD user-profile
  frame applies to SPARQL responses only; single-file mode no longer loads a
  parent directory's metadata cache; an unreadable cache is regenerated; an
  unreadable entry no longer aborts the metadata scan; `void:vocabulary` is
  derived from predicate and class namespaces (object paths were unbounded).
* Core API: a closed BeakGraph reports `isClosed()` and refuses reads, and a
  closed reader fails every storage call with `ClosedException` instead of
  serving stale buffers or a jHDF error; writes are denied with Jena's
  `AddDeniedException` / `DeleteDeniedException` in both call forms; a
  named-graph view's dataset answers the view's graph as its default graph
  in `find`, `findNG` and `listGraphNodes`; a row whose ids cannot be
  resolved is dropped by the dataset graph as it is by the graph; the
  constructor's document base is honoured - the query engine rewrites
  absolute IRIs under it to the stored relative form and resolves every
  result row, and the dataset graph and `find()` do the same - with a
  remote (http) store resolving against its own URL by default;
  `RelativeIRIResolver` moved to `core` (the `core.fuseki` class is a
  deprecated alias); the stage-generator director is re-installed when
  another component replaces the global slot and rides in every BeakGraph
  dataset context; `setDataset(Dataset)` is gone from the writer builders
  (inputs are files); the writers no longer implement the read-side
  dictionary interfaces (the `extract*` methods are gone, `DictionaryWriter`
  gained `locate`); VoID reorder statistics use the distinct-subject and
  distinct-object role lists; the reader pool accepts http(s) keys and
  names the schemes it accepts; the HTTP channel follows redirects once at
  open and re-resolves an expired target, requires and fully checks the
  Content-Range (and the Content-Length) of a 206, treats a zero
  Content-Length on HEAD as unknown, retries with exponential backoff and
  the server's Retry-After (up to five attempts), and counts every range
  request it issues.
* Readers: one profile helper opens every dataset and attribute, so a
  chunked dataset or a 32-bit `numEntries` is reported by name instead of a
  cast error, and integer attributes of any width are accepted; a file with
  `.BG` but no dictionary, an index level whose bitmap and id list differ in
  length, an FCD group whose block count, offsets or flags do not match its
  entries, or a triple-term store that disagrees with the datatypes column
  fails the open; language tags are decoded once at open and must be in
  Jena's formatted form; a pre-v3 store logs the linear-select fallback it
  runs under. The union / any-graph fan-outs drive the per-graph reads from
  the columnar graph ids, the object dictionary of an all-IRI store answers
  the true insertion point for literal probes (range hints empty the scan
  instead of walking it), `isGraph` is a binary search over the stored list,
  a literal search reuses the tier terms' values and builds numeric probe
  values from the packed numbers, FCD blocks decode without per-entry
  copies, bit-packed and spill buffers write in chunks, and the last word of
  a bitmap is read in one piece. The on-disk attribute and section names are
  constants in `Params`; the writers' `Types` enum is replaced by
  `DictionarySection`. SPECIFICATIONS §7.9 now describes the empty store's
  actual layout (index groups with only their seeded directories).
* Readers: a remote (HTTP) dictionary section no longer builds the sampled
  tier index on its first search - it downloaded the whole section - and the
  tier is built outside the search cache's lock; the decoded front-coded
  block cache is weight-bounded (large literals keep fewer blocks); the Zstd
  decompressor is one per thread per JVM instead of per reader and thread;
  `-verify` prints each store's format version and `BGReader` exposes it.
* Core API: named-graph views share the dataset's reorder statistics (a
  `GRAPH ?g` query loaded them once per graph); `Graph.stream()` works
  (it threw); property functions registered after the first store opened are
  visible to BeakGraph datasets; promotable read transactions (`begin()`,
  `Txn.execute`) run as reads instead of throwing. The HTTP channel reads
  ahead on sequential scans (`beakgraph.http.readahead`), pins the server's
  validator and sends `If-Range` so an in-place replacement fails loudly, and
  keeps query strings (presigned-URL signatures) out of messages, logs and
  the graph's identity.
* Build: no unused dependencies or resolution repositories, dependency pins
  synced with Jena 6.2.0, value-based `-Dhdf5.ffm=true` backend selection, a
  rolling log file, regenerated native-image metadata, CI packages the jars
  and the benchmarks module and can build the native binary.

## 0.18.0 - format v5 (2026-07)

* **Format v5**: RDF 1.2 triple terms (`<<( s p o )>>`, nested) stored
  term-exactly in a `tripleTerms` component store in the literals section
  (SPECIFICATIONS.md §7.6a). v3/v4 files read unchanged; v5 files are rejected
  by 0.17.0 and earlier with an "Upgrade BeakGraph" error.
* **Format v4**: `rdf:dirLangString` base directions in an optional
  `langDirs` dataset (§7.5.3). **Versions up to 0.17.0 silently stored
  `"x"@en--ltr` as `"x"@en`**; rebuild affected sources - the file cannot
  reveal the loss.
* Composite (cdt:) literals order lexically in the dictionary (term identity
  is lexical identity; the value comparison was not a total order). Stores
  built by 0.17.0 or earlier that contain them must be rebuilt.
* Blank nodes inside composite literals are rejected at ingest.
* Six writer engines (`-method 0..5`) targeting one format; the disk engines
  (1/4/5) bound RAM by spilling and need the native HDF5 library.
* `-export`, `-verify [-deep]`, `-merge`, per-file `-threads`, VoID
  statistics opt-in (`-void` / `-voidsketch`), reader tuning properties.

## 0.17.0 and earlier

* Format v3: rank/select directory corrected (v1-2 directories are ignored by
  readers, which fall back to linear bitmap scans).
* HDF5 container replacing the original Apache Arrow / RO-Crate design.

## Format v5 design notes

Code comments cite these by name; they record decisions made while adding
RDF 1.2 support, so the rationale stays in the repository.

* **One classifier**: whether a pattern term is concrete, a variable, or a
  triple term with embedded variables is decided in exactly one place
  (`BGIteratorMaster`); a second, subtly different classification in an
  iterator once returned every row for a bound variable.
* **syntaxARQ at the endpoint**: the endpoint parses queries with Jena's ARQ
  grammar, which already covers SPARQL 1.2 triple terms and the CDT
  `FOLD`/`UNFOLD` surface; `syntaxSPARQL_12` drops `UNFOLD`. The conformance
  cost is re-checked on every Jena upgrade
  (src/test/resources/w3c/sparql12/README.md).
* **Interior terms are dictionary members**: an IRI, blank node or literal
  that occurs only inside a triple term still gets a dictionary id, so the
  component store can reference it; the writers collect interior terms
  during the same pass as top-level terms.
* **Component resolution**: triple terms are stored as component ids into
  the entities, predicates and literals sections and resolved recursively;
  the disk engines resolve components with a reference join over the sorted
  term runs rather than an in-memory map.
* **No widened structs**: the fixed-stride component record is never
  widened for a special case (repeated variables, nesting); such cases are
  handled by the matcher, keeping every engine's writer identical.
* **Mirror topology hazard**: six engines implementing one rule by hand is
  the recurring source of divergence; shared helpers (`RdfSources.parser`,
  `Params.gridGraph`, `RelativeIris`, `WriterEngines` in tests) exist to
  keep the rule in one place.
