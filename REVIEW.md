# BeakGraph code review — full repo

*Branch `next`, 2026-07-01. Five subsystem deep-reviews (storage/HDF5 I/O, core write pipeline, SPARQL query engine, spatial/Hilbert/geometry, services/pool/CLI) over ~16k LOC; key findings reproduced at runtime and mechanisms re-verified in code. Test suite at review time: 155/155 green (1 skipped). After the fixes below: 211/211 green (1 skipped).*

**Overall:** the storage core is in genuinely good shape — the bit-packing, rank/select directories, front-coded dictionaries, VByte, and writer↔reader symmetry were traced end-to-end and came back clean. The bugs cluster at the *integration seams*: the comparator contract with Jena's value ordering, the spatial index's variable binding, Jena's `BindingBase` parent invariant, and the HTTP/IRI layer. Two findings are critical; both produce silently wrong query results or failed builds on realistic data.

## Checklist

**Critical**
- [x] 1. `NodeComparator` total-order violation (mixed-timezone temporals, durations)
- [x] 2. Constant-subject `sfIntersects` silently returns zero rows

**High**
- [x] 3. `BindingBG` double-exposure breaks `DISTINCT`
- [x] 4. Negative coordinate space excluded from the spatial index
- [x] 5. Invalid stored geometries unqueryable (`Intersects` throws)
- [x] 6. `absoluteToStorage()` rewrites absolute-stored IRIs into guaranteed misses
- [x] 7. CLI `-src` without `-dest` silently "succeeds"

**Medium**
- [x] M1. Failed rebuild deletes the previous good artifact
- [x] M2. CRS-prefixed WKT literals get no derived features
- [x] M3. LWS RDF listings emit doubled-slash URIs
- [x] M4. Percent-encoding mismatch 404s encodable filenames
- [x] M5. Link headers/linksets advertise the canonical base, not the live one
- [x] M6. Content negotiation ignores `*/*` (and container POST answered 406)
- [x] M7. `-version`/`-v` prints nothing
- [x] M8. Hilbert-layer external-API traps (`isRangeRelevant`, `getGridCells`)

**Low**
- [x] L1. `DefaultGraphNode` sentinel collapses with the default-graph sentinel
- [x] L2. Pool never invalidates failed readers; validation checks only `isOpen()`
- [x] L3. Committed-response failures: truncated 200 + double `sendError` ISE
- [x] L4. `SPARQLEndPoint.getDataset()` NPE; `file:///` resolution base leak
- [x] L5. `LWSMetadataGenerator` indexes its own metadata cache
- [x] L6. `SparqlWebPageServlet` classpath lookup lacks dot-segment rejection
- [x] L7. `FileCounter` summary double-subtracts zero-length files
- [x] L8. Feature-math edge cases (thin polygons, INF literals, holed-polygon PCA bias, axis-convention mismatch)
- [x] L9. Mixed `GEOMETRYCOLLECTION` non-polygon members unindexed
- [x] L10. Coordinates ≥ 2³¹ silently alias in the Hilbert curve
- [x] L11. `VoidStats` per-predicate counts overwritten across graph partitions
- [x] L12. RDF-star guard asymmetry (silent dictionary desync landmine)
- [x] L13. `isInTransaction()` unconditionally true
- [x] L14. `BGIteratorSPO_All` ignores a pre-bound graph variable

**Packaging & design notes**
- [x] P1. Self-contained fat jar by owner decision (verified standalone: convert + serve + query); trade-offs documented
- [x] P2. CI matrix with a Linux leg (symlink-escape test finally runs)
- [x] P3. Dead/misleading API pruned; `FCDWriter` block-size guard
- [x] P4. Large-store performance items (lazy per-graph iterators, graph-id set, negative-lookup caching, compact union dedup key, lazy tiered index)

## Critical

- [x] **1. `NodeComparator` is not a total order for mixed timezoned/naive `xsd:dateTime` — breaks every sort and binary search built on it** — `NodeComparator.java:52`
  `NodeValue.compareAlways` uses value order when XSD comparison is determinate but falls back to lexical term order when it's *indeterminate* (tz vs no-tz within ±14h). Mixing the two orderings pairwise creates cycles — hand-verified 3-cycle: `"2020-01-02T00:00:00"` < `"2020-01-02T08:00:00+14:00"` < `"2020-01-01T20:00:00Z"` < the first. Reproduced against the built project: a 400-triple file of ordinary mixed local/UTC timestamps **fails to build** (`IllegalStateException: Cannot resolve Object (not in dictionary)`), and on files that do build, binary search missed 2550/3000 stored literals — i.e., **silent missing rows**. Same hazard applies to `xsd:date`/`xsd:time` (also optionally timezoned) and `xsd:duration` (year-month vs day-time indeterminacy). Everything sits on this comparator: `NodeSorter`, `PositionalDictionaryWriter`, the GSPO/GPOS quad sorts and their dedup, `NodeSearch.findPosition`, `searchFAST`, and the `ValueCluster` range pushdown.
  *Status: **FIXED** (2026-07-01). Timezone-sensitive temporal spaces (dateTime/date/time/g\*) now order by a UTC-pinned timeline key and durations by (total months, total seconds), both with exact-term tie-breaks; every other value space stays on `compareAlways`, whose cross-space ordering was probe-verified to be a consistent value-space rank. The new order agrees with XSD value order on every determinate pair, so the `ValueCluster` pushdown stays over-inclusive and value-equal terms stay adjacent; healthy existing files keep an identical sort order (no format bump — files with mixed-timezone temporals were already unsearchable and need a rebuild). Regression: `TemporalTotalOrderTest` (comparator totality incl. the reproduced cycles, plus mixed-tz dateTime and mixed-kind duration build/query round-trips — all failing before the fix, the round-trips with the original `Cannot resolve Object` error).*

- [x] **2. Spatial `sfIntersects` with a constant subject silently returns zero rows** — `PatternMatchBG.java:47-53` + `SpatialIndexIterator.java:76`
  When the trigger triple's subject is concrete (`ex:small geo:asWKT ?w FILTER(geof:sfIntersects(?w, …))`), `varToBind` stays the geometry variable, and the spatial iterator binds it to candidate **subject** IDs (`NodeId(id, NodeType.SUBJECT)`). The subsequent pattern solve substitutes the WKT-literal position with a subject URI, which can never match — `ASK` returns false for a geometry that genuinely intersects. Found independently by two reviewers, both runtime-reproduced.
  *Status: **FIXED** (2026-07-01). `findTriggerTriple` now only returns a triple with the geometry variable in the object position AND a variable subject, and that subject is the only thing ever seeded (recall-safe for any predicate, since the writer indexes the subject of every wktLiteral-object quad). With a concrete subject the index is skipped and the always-present `sfIntersects` filter verifies — correct, just unaccelerated. This also closes the adjacent latent case where the geometry variable found in a subject/predicate position would have been seeded with subject ids. Regression: `SpatialConcreteSubjectTest` (pinned-subject SELECT/ASK, disjoint negative, VALUES-pre-bound subject, variable-subject control — SELECT/ASK failing before the fix).*

## High

- [x] **3. Input bindings are double-exposed through `BindingBG`, breaking `DISTINCT`** — `SolverLibBeak.java:41`, `BindingBG.java:23`
  `convert()` copies every var of the incoming binding into the `BindingNodeId` map, and `BindingBG` *also* mounts the original binding as its `BindingBase` parent. Jena assumes a child never re-binds a parent var: `size()` inflates (3 for 2 vars), `vars()` yields duplicates, and `BindingLib.equal` fails on the size mismatch — runtime-reproduced: `SELECT DISTINCT` returned two identical rows. Hits any BGP fed by `VALUES`, `BIND`, subqueries, or joins across graphs.
  *Status: **FIXED** (2026-07-01). `BindingBG` now exposes only vars the parent does not bind (`ownVar` filter on `get1/vars1/size1/contains1`); parent-copied vars — including "does not exist" terms — resolve through the parent, which carries the identical term the ids were derived from. Regression: `InputBindingShapeTest` (binding size/vars shape, DISTINCT merge for VALUES and BIND, missing-term ride-through — all failing before the fix).*

- [x] **4. Negative coordinate space is excluded from the spatial index on both sides** — `SpatialIndexIterator.java:99-104`, writer at `PositionalDictionaryWriterBuilder.java:296-302`
  Fully-negative geometries are skipped at build (warning only), straddling geometries are clamped, and query regions with a negative max bail out to zero candidates — probe-verified silent false negatives. The iterator's own recall proof states `0 ≤ coord < 2^31`; fine for slide imagery, but the README advertises GeoSPARQL, where negative lon/lat is most of the planet.
  *Status: **FIXED** (2026-07-01). Both sides now clamp bboxes into the non-negative Hilbert domain with the same monotone projection (never skip, never bail): overlap survives the clamp, so recall is preserved — negative-space geometry indexes coarsely at the axis cells and the exact JTS verification removes the clamp's false positives. No format change; old files simply lack the previously-skipped geometries until rebuilt. Regression: `SpatialNegativeCoordinateTest` (negative/straddling regions and geometries, origin-clustering false-positive check — failing before the fix).*

- [x] **5. Topologically invalid stored geometries are indexed but permanently invisible** — `Intersects.java:74-76`
  The verification stage throws on `!isValid()` (Jena drops the row) for exactly the self-intersecting polygons the write path deliberately indexes — `BadPolygonResilienceTest`'s own comment says real pathology data contains them. Probe-verified: a bowtie polygon is never returned by any `sfIntersects` query, with no warning.
  *Status: **FIXED** (2026-07-01). Invalid geometries (stored or query region) are repaired with JTS `GeometryFixer.fix` before the intersection test — chosen over `buffer(0)` because it preserves the point set (bowtie → its two triangles) and doesn't discard lines/points. Regression: two new tests in `BadPolygonResilienceTest` (bowtie returned by a query over its real area; invalid query region answers instead of emptying — both failing before the fix).*

- [x] **6. `absoluteToStorage()` rewrites same-host absolute IRIs into forms the dictionary never contains** — `RelativeIRIResolver.java:68` (applied at `BGSparqlService.java:92`)
  Jena's `relativize` happily returns `/other/thing` and `../other.png` (both `isRelative()`), but the writer's storage forms — source-relative IRIs re-relativized against the one-segment sentinel base — can never start with `/` or `../`. So a query naming an absolute same-host IRI that's stored *absolute* gets rewritten into a guaranteed dictionary miss: zero rows while `?s ?p ?o` shows the triple.
  *Status: **FIXED** (2026-07-01). `absoluteToStorage` now takes a dictionary-membership probe and rewrites only when the relative form IS stored and the absolute form is NOT (both stored → the exact term the query named wins). `BGSparqlService` supplies the probe from the BG node table (one binary search — deliberately not a per-graph `find()`), via `BGDatasetGraph.getBeakGraph()`; non-BG datasets never rewrite. Regression: `RelativeIRIResolverTest` gained absolute-stored, `../`-form, and both-forms cases; existing relativization tests updated to the probe API.*

- [x] **7. CLI: `-src` without `-dest` "succeeds" while converting nothing** — `BeakGraphCLI.java:178` + `:126`
  `params.dest` is never validated; `FileProcessor.call()` NPEs on `params.dest.toPath()` *before* its try/catch, inside a Future that's discarded at the `engine.submit()` call. Result: no log, no failure count, progress bar advances, summary says "Successful Conversions: N", exit code 0, zero files written.
  *Status: **FIXED** (2026-07-01). `main()` refuses `-src` without `-dest` (stderr + usage + exit 1), the missing-src message now also goes to stderr with exit 1, `FileProcessor.call()` counts and logs every failure from the top of the method (nothing escapes into the discarded Future), and a run with failed conversions exits 2. Regression: `BeakGraphCLIConversionTest` (bad source counted while good source still converts; dest-less processing fails loudly into the counter).*

## Medium

- [x] **M1. Failed rebuild deletes the previous good artifact** — `HDF5Writer.java:58`. The cleanup deleted the destination even when the failure (parse error, finding #1…) happened before anything was written, destroying a pre-existing valid `.h5`.
  *Fixed (2026-07-01): builds go to a sibling `.tmp` file and are atomically moved over the destination only on success; failure cleans up the temp file and never touches the destination, and readers never observe a half-written file at the published path. Regression: `WriteErrorHandlingTest.failedRebuildPreservesThePreviousGoodArtifact` (byte-identical artifact after a failed rebuild; no `.tmp` left behind) — failing before the fix.*
- [x] **M2. CRS-prefixed WKT literals get no derived features** — `PositionalDictionaryWriterBuilder.java:331-336`. `addSpatial` strips the `<CRS>` prefix (the earlier fix) but `addFeatures` re-read the *unstripped* lexical form; JTS threw, both feature generators skipped. Probe-verified.
  *Fixed (2026-07-01): `addFeatures` now receives the CRS-stripped WKT computed once in `addSpatial`. Regression: `FeatureGenerationResilienceTest.crsPrefixedGeometryGetsFeatures` — failing before the fix.*
- [x] **M3. LWS RDF listings emit doubled-slash URIs that 404** — `LWSStorageServlet.java:331` (also 368–388). `BASE` ends with `/` and the canonical remainder starts with `/`; the HTML branch was correct, the machine-readable branches weren't. The paged container's self URI also disagreed in shape with its own `first`/`last`/`prev`/`next` values, which were emitted as plain literals.
  *Fixed (2026-07-01): all canonical→live mapping goes through `toLiveUri` (strips the leading slash) — item URIs, per-item property objects, and page URIs; navigation links are now resources with the same URI shape as the page's self URI. Regression: `LWSStorageServletTest.toLiveUriMapsCanonicalUrisWithoutDoubledSlashes`.*
- [x] **M4. Percent-encoding mismatch makes encodable filenames permanently 404** — `LWSStorageServlet.java:243`. Raw `getRequestURI()` was compared against model URIs built from raw filenames — the servlet's own encoded listing links couldn't be followed for `a b.h5`.
  *Fixed (2026-07-01): `doGet`/`doPost` decode the raw request URI via `decodePath` (java.net.URI semantics — no `+`→space damage); a malformed URI answers 400. The traversal guard now sees the real decoded path, strengthening it against encoded dot-segments. Regression: `LWSStorageServletTest.decodePathDecodesPercentEncodingAndRejectsGarbage`.*
- [x] **M5. Link headers/linksets advertise the canonical base, not the live one** — `LWSStorageServlet.java:283`. On any non-default port every advertised linkset URI pointed at the wrong server; `/description`'s advertised `.meta` 404'd even on defaults.
  *Fixed (2026-07-01): the Link header and the linkset's `anchor`/`up` are mapped to the live base via `toLiveUri`, and `GET /description.meta` is answered directly (the description document is not in the metadata model).*
- [x] **M6. Content negotiation ignores `*/*`** — `LWSStorageServlet.java:287`. `curl` (default `Accept: */*`) got 406 on containers; RFC 9110 requires `*/*` to match. `doPost` also answered 406 where 405 was meant.
  *Fixed (2026-07-01): `acceptsHtmlRepresentation` treats `*/*`, `text/*`, and an absent Accept header as HTML-capable; POST to a container answers 405. Regression: `LWSStorageServletTest.wildcardAcceptHeadersGetARepresentationNotA406`.*
- [x] **M7. `-version`/`-v` prints nothing** — `BeakGraphCLI.java:86`. Only reachable inside the `ParameterException` catch; a bare `-v` parsed fine and fell through silently.
  *Fixed (2026-07-01): handled on the success path immediately after parse — prints the version and exits 0. Verified by running the CLI (`beakgraph -v` → `beakgraph - Version : 0.15.0`).*
- [x] **M8. Latent external-API traps in the Hilbert layer** — `HilbertPolygon.isRangeRelevant` kept ranges only if the polygon covers the endpoint cell *corner points* (a sub-cell polygon produced an EMPTY cover; diagonal strips lost interior cells), and `PolygonScaler.getGridCells` double-applied the scale factor, disagreeing with the writer-persisted tiles for every s≥1. Neither is on the in-repo query path, but both are the public Polygon→Hilbert API surface.
  *Fixed (2026-07-01): `isRangeRelevant` now tests whole-cell intersection across the range's cells (capped at 64 cells; longer ranges are kept unexamined — superset-safe), and `getGridCells` uses the constant `GRIDTILESIZE` on the already-scaled polygon, matching the writer's `generateGridURNs` grid exactly. Regressions: `HilbertSpaceFixesTest` (sub-cell polygon cover, interior-overlap range) and `PolygonScalerGridCellTest` — all failing before the fix.*

## Low

- [x] **L1.** `urn:x-arq:DefaultGraphNode` as a data term compares equal to the default-graph sentinel → two distinct terms collapse in the dictionary (`NodeComparator.java:30,83-86`).
  *Fixed (2026-07-01): both sentinels still rank first, but the pair is now ordered by exact term (null ≡ the default-graph IRI, which keeps the lowest rank). Regression: `DefaultGraphSentinelTest` (comparator distinctness + sentinel-URI-as-data round-trip) — failing before the fix.*
- [x] **L2.** Pool never `invalidateObject`s after a failure and `validateObject` only checks `isOpen()` (`LWSStorageServlet.java:93`, `BeakGraphPoolFactory.java:47`).
  *Fixed (2026-07-01): `handleSparqlQuery` invalidates on any execution/infrastructure failure (via `BGSparqlService.execute`'s new health signal), and `validateObject` probes real mapped data (one dictionary `extract`) under the existing `testOnBorrow`, so an open-but-broken reader is culled on next borrow. Regression: `BeakGraphPoolValidationTest` (pins probe wiring; the open-but-broken scenario isn't portably reproducible on Windows).*
- [x] **L3.** Committed-response error handling: mid-stream failure yields a complete-looking truncated 200, then two `sendError` calls throw `IllegalStateException` (`BGSparqlService.java:132`).
  *Fixed (2026-07-01): `execute` throws `QueryExecutionFailedException` when the response is already committed; callers invalidate the reader, touch the response only when uncommitted, and let the exception propagate so the container aborts the connection — the honest signal for a truncated transfer. No servlet-mock harness exists, so this is code-reviewed rather than test-pinned.*
- [x] **L4.** `SPARQLEndPoint.getDataset()` NPEs in single-file mode; single-file `/rdf` uses the local file URI as resolution base, disclosing `file:///…` paths (`SPARQLEndPoint.java:155,134`).
  *Fixed (2026-07-01): the endpoint holds its Dataset directly (no registry lookup), and the `/rdf` servlet resolves against the served URL (`BASE_URL + "rdf"`). Requires a running server to test; code-reviewed.*
- [x] **L5.** Re-running `LWSMetadataGenerator` indexes its own previous `beakgraph.ttl.gz`, exposing the raw metadata with `owl:sameAs <file:///…>` paths (`LWSMetadataGenerator.java:71`).
  *Fixed (2026-07-01): the walk skips `CACHE_FILE_NAME`. Regression: `LWSMetadataGeneratorTest` — failing before the fix.*
- [x] **L6.** `SparqlWebPageServlet` concatenates decoded `pathInfo` into classpath lookups with no dot-segment rejection (`SPARQLEndPoint.java:203`).
  *Fixed (2026-07-01): paths containing `..` or `\` answer 404 before touching the classpath — containment no longer depends on Jetty compliance defaults. Private servlet, no harness; code-reviewed.*
- [x] **L7.** `FileCounter` summary math double-subtracts zero-length files (can print negative "Successful Conversions") (`FileCounter.java:74`).
  *Fixed (2026-07-01): `getSuccessfulConversionCount()` = RDF − failed (empties never entered the RDF count). Regression: `FileCounterTest`.*
- [x] **L8.** Feature-math edge cases: sub-0.5-unit polygons lost ALL features (`ShapeAnalysis.java:234`); zero-area geometry emitted `"INF"^^xsd:double` literals (`ShapeAnalysis.java:98`); holed polygons biased the PCA centroid/axes (`MajorMinor.java:31`); drawn axis (2·σ vertex cloud) disagreed with `PYR.MajorAxisLength` (4·σ pixel cloud).
  *Fixed (2026-07-01): raster dimensions come from the internal envelope clamped to ≥1px (degenerate envelopes no longer throw); non-finite feature values are skipped per-feature; `MajorMinor` now computes exact polygon AREA moments (Green's theorem, shell minus holes) with the pyradiomics 4·√λ axis convention — the same definition the raster feature uses. Regression: `FeatureEdgeCasesTest` (thin polygon, bowtie INF, donut centroid — the pre-fix bias reproduced exactly as POINT(1.7778 1.7778) — and the axis convention).*
- [x] **L9.** Mixed `GEOMETRYCOLLECTION`: non-polygon members unindexed when any polygon part exists (`PositionalDictionaryWriterBuilder.java:238`).
  *Fixed (2026-07-01): every non-polygonal member gets its expanded-envelope stand-in, per member instead of only as a no-polygon fallback. Regression: `SpatialGeometryCollectionTest` — point-member query failing before the fix.*
- [x] **L10.** Coordinates ≥ 2³¹ silently alias (the Hilbert curve masks to 31 bits, no guard) (`HilbertSpace.java:31`).
  *Fixed (2026-07-01): `HilbertSpace.clampToDomain` clamps into [0, 2³¹) on the writer, query, and `Polygon2Hilbert` sides — the same monotone-projection recall argument as the negative-space fix, and no more reliance on the library's undefined masking. Regression: `SpatialHugeCoordinateTest` (pins the defined behavior; the old masking happened to alias consistently in this scenario).*
- [x] **L11.** `VoidStats` per-predicate counts overwritten (not summed) across graph partitions (`VoidStats.java:84`).
  *Fixed (2026-07-01): `merge(…, Long::sum)`. Regression: `VoidStatsTest` — 2-vs-5 failing before the fix.*
- [x] **L12.** Guard asymmetry landmine: an RDF-star triple term in the subject path desynchronizes the dictionary silently (`MultiTypeDictionaryWriter.java:190`, builder `:494`).
  *Fixed (2026-07-01): the subject branch and `addNodeInternal` now throw like the graph/object branches. Unreachable via current parsers (that's the point of the guard); code-reviewed.*
- [x] **L13.** `isInTransaction()` unconditionally returns true (`BGDatasetGraph.java:253`).
  *Fixed (2026-07-01): transaction lifecycle delegates to Jena's `TransactionalLock` (MRSW) — real per-thread state; write transactions still rejected. Regression: `TransactionStateTest` (lifecycle, `Txn.executeRead`, WRITE rejection) — all three failing before the fix.*
- [x] **L14.** `BGIteratorSPO_All` ignores a graph variable pre-bound in the `BindingNodeId` (`BGIteratorSPO_All.java:62`).
  *Fixed (2026-07-01): resolves the graph from the binding exactly like the other three iterators behind the dispatcher. Regression: new case in `VariableGraphScanTest` — 0-vs-20 rows failing before the fix.*

## Packaging & design notes

- [x] **P1.** The default `lib` profile **shades all dependencies unrelocated into the main artifact** and embeds `log4j2.yml` at the classpath root — duplicate-class conflicts and logging hijack for library consumers.
  *Resolved by owner decision (2026-07-01): the BeakGraph jar is DELIBERATELY self-contained — one jar that runs with nothing else on the classpath. The `lib` profile keeps its fat, executable jar (dependencies, `log4j2.yml`, SPARQL web UI, merged service files, Main-Class). Verified end-to-end with the bare artifact from a neutral directory: `-v`, a spatial TTL→HDF5 conversion, and a booted `-endpoint` answering an HTTP SPARQL query. Retained hygiene: the log4j backend + YAML-config Jackson modules stay `optional` in the POM (accurate — main code logs only through slf4j; shade still embeds them). Accepted residual trade-off, documented in the POM: a consumer that ALSO brings its own Jena/JTS sees duplicate unrelocated classes, and the embedded `log4j2.yml` takes over that application's logging. Operational note: package profiles with `clean` (as the README shows) — switching profiles over a stale `target/` can reuse the other profile's jar.*
- [x] **P2.** The LWS symlink-escape test auto-skips on Windows, so that protection is never exercised on this dev machine.
  *Fixed (2026-07-01): `.github/workflows/ci.yml` runs `mvn -Plib clean test` on an ubuntu-latest + windows-latest matrix (Temurin 25, Maven cache). The Linux leg exercises the symlink test; the workflow itself needs a push to GitHub to be exercised.*
- [x] **P3.** Dead/misleading API pruned: `eRange` deleted (zero callers; its `join()` was a hull with no overlap precondition); `HDTBitmapDirectory.getId(rank)` (indexed by row, not rank), `rank1`, and `getBitCount` deleted (no callers); `UTIL.getBytes` (matched a nonexistent class name), `subBuffer`, `skipNullTerminatedStrings`, `readNullTerminatedString` deleted (no callers); `BindingNodeId.putAll` deleted (would have thrown if ever used); `FCDWriter` now rejects `blockSize < 2` at construction instead of writing an unreadable dictionary.
- [x] **P4.** Performance items for large stores, all addressed (2026-07-01):
  - `containsGraph`/`isGraph` answers from a lazily-built id set instead of decoding the whole columnar graphs list per call;
  - negative node-table lookups are cached (the store is immutable, so absence is permanent — a repeated foreign-term miss no longer re-runs two binary searches);
  - `readUnion` chains per-graph iterators lazily and dedups on a record of three primitive longs instead of a boxed `List<Long>` (the set itself is inherent to union set-semantics);
  - the unbound-graph scan (`BGIteratorMaster`), `find(ANY,…)`, and `findNG(ANY,…)` build each graph's sub-iterator lazily as iteration reaches it, and the master scan pre-binds the graph id from the columnar list (no `extract→locate` round trip; the rare `?g`-in-predicate shape keeps the substitution+compatibility-filter path because predicate ids live in an isolated id-space);
  - the dictionary's tiered search index builds lazily on first search (volatile-published, race-benign) instead of doing `numEntries/1024` full extracts at every file-open.
  *All behavior pinned by the existing suite (union/DISTINCT/variable-graph-scan/concurrent-read/reader-lifecycle tests) — 211/211 green after the changes.*

## Verified sound

Worth stating, since it's most of the risk surface: MSB-first bit-packing round-trips at all edge widths, the rank/select directory math (pinned exhaustively by `RankSelectDirectoryTest`), FCD front-coding including UTF-16 prefix handling and surrogate back-off, VByte, the L0 padding/select1 addressing scheme, the index-selection matrix across all four iterators, repeated-variable handling via `putCompatible`, `ValueCluster` pushdown (over-inclusive only, filter always re-applied), reader thread-safety (absolute-position reads, immutable dictionaries), pool borrow/return discipline at the single borrow site, and HDF5 resource cleanup on error paths.
