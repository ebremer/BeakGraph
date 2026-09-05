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

Configuration file generation for native-image (already generated for current source code.  Only needed if extensive changes have been made)
```
java -Xmx16G -agentlib:native-image-agent=config-output-dir=src\main\resources\META-INF\native-image -jar target\BeakGraph-0.15.0.jar
```
Native Command-line
```
mvn -Pcmdlinenative clean package
```
Jar Command-line
```
mvn -Pcmdlinejar clean package
```
Core Library Jar Library
```
mvn -Plib clean package
```

## Benchmarks

JMH benchmarks for the read path live in [`benchmarks/`](benchmarks/) as a standalone module:

```
mvn -DskipTests install          # install BeakGraph into the local repo
cd benchmarks && mvn package     # build the benchmarks jar
java -jar target/benchmarks.jar  # run (see benchmarks/README.md for options)
```

## Using BeakGraph in your code

### Creating a BeakGraph from your data

The source syntax is detected from the file name (Turtle, TriG, N-Quads,
N-Triples; `.gz` compression is handled), so named graphs can be loaded from
quad-capable formats.

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

BeakGraph is a [Apache Jena](https://jena.apache.org/) Graph implementation backed by [HDF5](https://www.hdfgroup.org/solutions/hdf5/).
Beakgraph's HDF5 design is heavily inspired by [RDF HDT](https://www.rdfhdt.org/).

### Limitations

* BeakGraph files are read-only; the writer builds them in one pass and holds
  the working set in RAM (very large datasets may need a correspondingly large
  heap).
* GeoSPARQL support covers `geof:sfIntersects` only. It is fully functional: a
  recall-safe Hilbert cell-cover index produces candidate geometries and every
  candidate is verified with real JTS geometry, so results are exact. Other
  GeoSPARQL functions are not implemented.
* `.h5` files written before the spatial-index redesign carry the old
  corner-based index entries, which the query side no longer reads - rebuild
  them from source for spatial queries (their spatial answers were unsound
  anyway; non-spatial queries are unaffected).
* Numeric literals typed `xsd:int`, `xsd:long`, `xsd:float` or `xsd:double` are
  stored by value and canonicalized at ingest: `"01"^^xsd:int` is stored - and
  matched - as `"1"^^xsd:int`.
* RDF 1.2 support is at the spec's **full conformance** level (format v5):
  base-direction literals (`"x"@en--ltr`, `rdf:dirLangString`; format v4) and
  triple terms (`<<( s p o )>>`, including the reifier/annotation sugar and
  nesting; format v5) are stored and matched term-exactly across every writer
  engine, and SPARQL 1.2 triple-term patterns — embedded variables included —
  are answered from the index. Evidence: the vendored W3C RDF 1.2 suites (213
  executed, 0 failures) and SPARQL 1.2 suites (259 executed, 0 failures) run in
  CI over real stores (`W3CRdf12SuiteTest`, `W3CSparql12SuiteTest`; see
  [RDF_1.2-compliance.md](RDF_1.2-compliance.md)). Versions ≤ 0.17.0 silently
  stored base-direction literals as plain `"x"@en` — rebuild affected stores; a
  version that rejects or correctly stores them is the detector.
* SPARQL-CDT composite literals (`cdt:List`/`cdt:Map`; the
  [spec](https://awslabs.github.io/SPARQL-CDTs/spec/latest.html) is an
  **Unofficial Draft**) are stored term-exactly and queryable with Jena's
  `cdt:` functions, `FOLD`, and `UNFOLD` — the spec's own 658-test suite runs
  against BeakGraph stores in CI (`SparqlCdtSuiteTest`). Caveats: the
  dictionary orders composite literals lexically, so stores built by ≤ 0.17.0
  that contain them must be rebuilt; blank nodes inside composite literals are
  rejected at ingest (labels regenerate from rank, which would silently sever
  their co-reference); and composite elements are **opaque to the index** —
  filtering or joining inside a list/map parses the whole value in RAM, so
  model data as triples when you need to query it and as composite literals
  when you need compact, exact round-tripping.

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
  src="https://github.com/ebremer/BeakGraph/blob/develop/src/main/resources/beakgraph.png"
  width=300px height=300px
  alt="BeakGraph"
  title="BeakGraph"
  style="display: inline-block; margin: 0 auto; max-width: 150px">