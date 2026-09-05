# Changelog

Notable changes to BeakGraph and its on-disk format. The format history is
normative (SPECIFICATIONS.md §4.1); the rest is a summary of what a release
brought and what a reader must rebuild.

## Unreleased (branch rdf12andcdt, after 0.18.0)

* Jena 6.2.0, jHDF 0.13.0.
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
