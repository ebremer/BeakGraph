# BeakGraph<br>
<img
  src="https://github.com/ebremer/BeakGraph/raw/master/beakgraph.png?raw=true"
  align="left"
  width="300" height="300"
  hspace="20" vspace="10"
  alt="BeakGraph"
  title="BeakGraph">

BeakGraph is an [Apache Jena](https://jena.apache.org/) Graph implementation of [RDF HDT](https://www.rdfhdt.org/) technology pumped into a [HDF5](https://www.hdfgroup.org/solutions/hdf5/) file and extended to support a full RDF Dataset.

<br clear="all">

## Building

Requires Java 25 and Maven 3.9 (see [docs/INSTRUCTIONS.md](docs/INSTRUCTIONS.md) §1-2 for the
native HDF5 library the disk-based engines need).

```
mvn -Plib clean package             # self-contained library jar: target/BeakGraph-<version>.jar
mvn -Pcmdlinejar clean package      # the same jar, as the command-line tool
mvn -Pcmdlinenative clean package   # GraalVM native binary: target/beakgraph
```

The native binary runs the in-memory engines only (`-method 0/2/3`; the disk engines need JNI
metadata for the HDF5 library that the image does not carry) and takes Substrate VM's default
heap - raise it per run with `beakgraph -XX:MaxHeapSize=16g ...` (the equivalent of
`java -Xmx16g`). The builder JVM needs a large heap (`-Dnative.build.heap=32G` by default).
Its reachability metadata under `src/main/resources/META-INF/native-image` is generated with the
tracing agent and must be regenerated after CLI or dependency changes:

```
java -agentlib:native-image-agent=config-merge-dir=src/main/resources/META-INF/native-image \
     -jar target/BeakGraph-<version>.jar <a representative run: -method 0..5, -merge, -export, -verify, -endpoint>
```

## Command line

`java -jar target/BeakGraph-<version>.jar -help` lists every option; the full reference is
[docs/INSTRUCTIONS.md](docs/INSTRUCTIONS.md). In short:

* `-src <file|dir> -dest <file|dir>` converts RDF to `.h5` (`-method 0..5` selects the engine:
  in-memory 0/2/3, disk-based 1/4/5 for inputs larger than RAM; `-merge` folds a tree into one store).
* `-src <file.h5|dir> -export NT|NQ|TTL|TRIG|JSON-LD` dumps stores back to RDF.
* `-verify <file.h5|dir> [-deep]` integrity-checks stores before publishing them.
* `-endpoint <file.h5|dir> -port 8888` serves one store, or a whole directory as W3C LWS storage,
  over SPARQL.

## Benchmarks

JMH benchmarks for the read path live in [`benchmarks/`](benchmarks/) as a standalone module:

```
mvn -DskipTests install          # install BeakGraph into the local repo
cd benchmarks && mvn package     # build the benchmarks jar
java -jar target/benchmarks.jar  # run (see benchmarks/README.md for options)
```

## Using BeakGraph in your code

### Creating a BeakGraph from your data

The source syntax is detected from the file name - Turtle, N-Triples, N-Quads,
TriG, RDF/XML and JSON-LD, each also as `.gz` or `.zip` - so named graphs can be
loaded from the quad-capable formats (TriG, N-Quads).

```java
BG.getBGWriterBuilder()
    .setSource(new File("mydata.ttl"))
    .setDestination(new File("mydata.ttl.h5"))
    .setSpatial(true)    // only needed if GeoSPARQL spatial data is present
    .setFeatures(false)  // optional: derive 2D shape features for geometries
    .build()
    .write();
```

### Using a BeakGraph with Apache Jena

```java
File file = new File("mydata.ttl.h5");
try (BeakGraph bg = BG.getBeakGraph(file)) {
    Dataset ds = bg.getDataset();
    ds.getDefaultModel().write(System.out, "NTRIPLE");
}
```

SPARQL runs with BeakGraph's own execution (index-driven joins, filter pushdown, spatial seeding) whether it is
issued against the dataset or against a Model over the graph (`ds.getDefaultModel()`,
`ModelFactory.createModelForGraph(bg)`).

### Querying a remote store in place

A store on a web server or object store is queried through HTTP range requests, without
downloading it (block-cached, retried on transient failures):

```java
try (BeakGraph bg = BG.getBeakGraph(
        new HTTPSeekableByteChannel(URI.create("https://example.org/data.h5")))) {
    Dataset ds = bg.getDataset();
}
```

Document-relative IRIs (`<>`, `<sibling.png>`) are stored unresolved and resolved against the
URL a store is served from; see [docs/INSTRUCTIONS.md](docs/INSTRUCTIONS.md) "Document-relative
IRIs" and SPECIFICATIONS.md §9.2.

BeakGraph is a [Apache Jena](https://jena.apache.org/) Graph implementation backed by [HDF5](https://www.hdfgroup.org/solutions/hdf5/).
Beakgraph's HDF5 design is heavily inspired by [RDF HDT](https://www.rdfhdt.org/).

### Limitations

* BeakGraph files are read-only. The in-memory engines (`-method 0/2/3`) hold
  the whole input on the heap; the disk-based engines (`-method 1/4/5`, native
  HDF5 library required, spill space under `-workdir`) bound RAM by spilling and
  are the way to build stores larger than memory
  ([docs/INSTRUCTIONS.md](docs/INSTRUCTIONS.md) §6-7).
* GeoSPARQL support covers `geof:sfIntersects` only. It is fully functional: a
  recall-safe Hilbert cell-cover index produces candidate geometries and every
  candidate is verified with real JTS geometry, so results are exact. Other
  GeoSPARQL functions are not implemented.
* Coordinate reference systems are not interpreted: the optional `<crs>` prefix
  of a `geo:wktLiteral` is stripped on both the index and the verification
  side, every geometry in a store and every query constant is assumed to share
  one Cartesian CRS (the intended domain is slide/pixel coordinates), and no
  axis-order or datum transformation is performed. An unprefixed literal adopts
  the other operand's CRS; a comparison between two literals that both name a
  CRS and disagree is an evaluation error (the row is dropped) rather than a
  silent raw-coordinate comparison.
* `.h5` files written before the spatial-index redesign carry the old
  corner-based index entries, which the query side no longer reads - rebuild
  them from source for spatial queries (their spatial answers were unsound
  anyway; non-spatial queries are unaffected).
* Numeric literals typed `xsd:int`, `xsd:long`, `xsd:float` or `xsd:double` are
  stored by value and canonicalized at ingest: `"01"^^xsd:int` is stored - and
  matched - as `"1"^^xsd:int`.
* A store carries exactly two quad indexes, `GSPO` and `GPOS`
  ([SPECIFICATIONS.md](SPECIFICATIONS.md) §8); there is no object-first index.
  A pattern that binds only the object (`?s ?p <o>`) is answered with one
  `GPOS` probe per predicate present in the graph - cheap for the usual
  handful of predicates, proportional to the predicate count otherwise. A
  pattern binding neither subject nor predicate nor object is a scan of the
  graph. Property paths are evaluated by Jena's generic path engine over
  `Graph.find`: each step is one index lookup (object-seeded steps such as
  `?s :p+ <o>` use the per-predicate probes above), but an ungrounded `*` or
  `+` path (`?s :p* ?o`) makes Jena collect **every subject and object node of
  the graph** in memory before the first row, and under
  `urn:x-arq:UnionGraph` each step costs one lookup per named graph. Ground
  one end of such paths, or run them inside `GRAPH <g>`.
* Dataset clauses (`FROM`, `FROM NAMED`, and the SPARQL protocol's
  `default-graph-uri` / `named-graph-uri`) keep the BeakGraph engine: a single
  `FROM <g>` is the graph's own view and several `FROM` graphs are a set-union
  view de-duplicated on ids (the whole-graph `DISTINCT ?p` / `COUNT(*)`
  shortcuts apply to a single graph only). A dataset with a BeakGraph default
  graph that is *not* a BeakGraph dataset (a Jena `Model` over the graph) takes
  Jena's generic construction instead.
* RDF 1.2 support is at the spec's **full conformance** level (format v5):
  base-direction literals (`"x"@en--ltr`, `rdf:dirLangString`; format v4) and
  triple terms (`<<( s p o )>>`, including the reifier/annotation sugar and
  nesting; format v5) are stored and matched term-exactly across every writer
  engine, and SPARQL 1.2 triple-term patterns — embedded variables included —
  are answered from the index. Evidence: the vendored W3C RDF 1.2 suites (215
  executed, 0 failures) and SPARQL 1.2 suites (266 executed, 0 failures) run in
  CI over real stores, on every writer engine (`W3CRdf12SuiteTest`, `W3CSparql12SuiteTest`; see
  [RDF_1.2-compliance.md](RDF_1.2-compliance.md)). Versions ≤ 0.17.0 silently
  stored base-direction literals as plain `"x"@en` — rebuild affected stores.
  `-verify` prints each store's format version: anything below v4 must be
  rebuilt if its source held base-direction literals, below v5 if it held
  triple terms ([docs/INSTRUCTIONS.md](docs/INSTRUCTIONS.md), "Format versions").
* SPARQL-CDT composite literals (`cdt:List`/`cdt:Map`; the
  [spec](https://awslabs.github.io/SPARQL-CDTs/spec/latest.html) is an
  **Unofficial Draft**) are stored term-exactly and queryable with Jena's
  `cdt:` functions, `FOLD`, and `UNFOLD` — the spec's own 659-test suite runs
  against BeakGraph stores in CI (`SparqlCdtSuiteTest`). Caveats: the
  dictionary orders composite literals lexically, so stores built by ≤ 0.17.0
  that contain them must be rebuilt; blank nodes inside composite literals are
  rejected at ingest (labels regenerate from rank, which would silently sever
  their co-reference); and composite elements are **opaque to the index** —
  filtering or joining inside a list/map parses the whole value in RAM, so
  model data as triples when you need to query it and as composite literals
  when you need compact, exact round-tripping.

### Design decisions

* **One self-contained jar.** The published library jar shades every dependency (Jena, Fuseki,
  jHDF, JTS, the HDF5 natives, log4j and its `log4j2.yml` at the classpath root), so a consumer
  gets one artifact that runs on an empty classpath. The trade-off: a consumer that also brings
  its own Jena or JTS sees duplicate, unrelocated classes, and the embedded logging
  configuration takes over that application's log4j setup (it writes a rolling
  `logs/beakgraph.log`; `-Dbeakgraph.log.dir` moves it). `mvn -Dthin` builds the plain jar.
* **Six engines, one format.** Every `-method` targets the same on-disk format
  (SPECIFICATIONS.md); the RAM-family engines produce byte-identical stores, the disk family
  isomorphic ones. Rules shared by all engines live in one helper each
  (`RdfSources.parser`, `RelativeIris`, `Params.gridGraph`, `NodeComparator`), because six
  hand-mirrored copies of a rule were the recurring source of divergence.
* **Locale-independent code.** Identifiers, protocol strings and file names are case-mapped
  and formatted with `Locale.ROOT` (never the JVM default: `"trig".toUpperCase()` is `TRİG` under
  tr-TR, `%d` prints Arabic-Indic digits under ar-EG); `LocaleRuleTest` scans the sources for
  violations.
* Format history and the RDF 1.2 design notes: [CHANGELOG.md](CHANGELOG.md).

### License

Apache License 2.0 ([LICENSE](LICENSE)). Source files without a header are BeakGraph's own,
copyright 2021-2026 Erich Bremer; the vendored zstd codec (`io.airlift.compress.v3.zstdFFM`)
and the Jena-derived GeoSPARQL vocabularies keep their original headers - see [NOTICE](NOTICE).

### Author's notes
The first iteration of BeakGraph was backed by Apache Arrow instead of [HDF5](https://www.hdfgroup.org/solutions/hdf5/).  An Apache Arrow version will return.  Reasons for this are varied with some of these reasons being just experimentation.
The general idea of BeakGraph is a read-only, searchable, indexed set of binary [succinct data structures](https://en.wikipedia.org/wiki/Succinct_data_structure) to represent an [RDF Dataset](https://www.w3.org/TR/rdf11-datasets/).
What these succinct data structures are stored in, is somewhat immaterial, but the choice of container has its pros and cons.  HDF5 treats multi-dimensional arrays as first class citizens, and has a free viewer for 
HDF5 files called [HDFView](https://www.hdfgroup.org/download-hdfview/).  HDFView provides a nice way to debug the succinct data structures during development.  There are other perks to HDF5 which will become apparent in time.

Spatial indexing based on [GeoSPARQL](https://github.com/opengeospatial/ogc-geosparql) is supported for `geof:sfIntersects` (see Limitations above).

The full list of containers under consideration are:
* [HDF5](https://www.hdfgroup.org/solutions/hdf5/)
* [Apache Arrow](https://arrow.apache.org/)
* [Zarr](https://zarr.dev/)
* [Zip](https://en.wikipedia.org/wiki/ZIP_(file_format))
* [DICOM](https://www.dicomstandard.org/)
* [LWS](https://github.com/w3c/lws-protocol)

### Historical
The original BeakGraph was an [Apache Jena](https://jena.apache.org/) Graph implementation backed by [Apache Arrow](https://arrow.apache.org/)
wrapped in a [Research Object Crate (RO-Crate)](https://www.researchobject.org/ro-crate/) inspired by [HDT](https://www.rdfhdt.org/).

Developed to power [Halcyon](https://github.com/halcyon-project/Halcyon).  See [Arxiv](https://arxiv.org/) paper at http://arxiv.org/abs/2304.10612

<img
  src="https://github.com/ebremer/BeakGraph/raw/master/beakgraph.png?raw=true"
  width=300px height=300px
  alt="BeakGraph"
  title="BeakGraph"
  style="display: inline-block; margin: 0 auto; max-width: 150px">