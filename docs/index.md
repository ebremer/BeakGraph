---
title: Home
layout: default
nav_order: 1
permalink: /
---

# BeakGraph
{: .fs-9 }

RDF datasets as self-contained HDF5 files, queried with SPARQL in place.
{: .fs-6 .fw-300 }

[Get started](instructions/){: .btn .btn-primary .fs-5 .mb-4 .mb-md-0 .mr-2 }
[View on GitHub](https://github.com/ebremer/BeakGraph){: .btn .fs-5 .mb-4 .mb-md-0 }

---

BeakGraph is an [Apache Jena](https://jena.apache.org/) graph implementation of
[RDF HDT](https://www.rdfhdt.org/) technology, stored in an
[HDF5](https://www.hdfgroup.org/solutions/hdf5/) file and extended to a full RDF dataset. It
converts RDF into **dictionary-encoded, columnar quad stores**: one `.h5` file is one
self-contained, immutable store, queried with SPARQL directly from the file — locally or over
HTTP range requests — without loading the graph into memory.

## Highlights

- **Six conversion engines, one format.** In-memory engines (`-method 0/2/3`) for data that fits
  in RAM; disk-based engines (`-method 1/4/5`) for inputs larger than memory, up to
  10⁹–10¹¹ quads. Every engine writes the same on-disk format.
- **Query in place.** A store on a web server or object store is queried through HTTP range
  requests without downloading it.
- **SPARQL endpoint and LWS storage.** `-endpoint` serves one store as a read-only SPARQL
  endpoint, or a whole directory of stores as [W3C LWS](https://github.com/w3c/lws-protocol)
  storage.
- **RDF 1.2.** Base-direction literals and triple terms are stored and matched term-exactly;
  the W3C RDF 1.2 and SPARQL 1.2 test suites run in CI against every writer engine.
- **GeoSPARQL.** `geof:sfIntersects` is answered from a Hilbert-curve spatial index, with every
  candidate verified against the real geometry, so results are exact.
- **An open format.** The [file format specification](specification/) is precise enough to
  write BeakGraph stores without the BeakGraph sources.

## Quick start

Requires Java 25 and Maven 3.9. The disk-based engines also need the native HDF5 library —
see [Requirements](instructions/#1-requirements).

```bash
# Build the self-contained command-line jar
mvn -Pcmdlinejar clean package

# Convert every RDF file under ./data to one .h5 per file under ./out
java -jar target/BeakGraph-<version>.jar -src data/ -dest out/

# Serve a store as a SPARQL endpoint on port 8888
java -jar target/BeakGraph-<version>.jar -endpoint out/example.h5 -port 8888
```

From Java, open a store and query it with Jena as usual:

```java
try (BeakGraph bg = BG.getBeakGraph(new File("mydata.ttl.h5"))) {
    Dataset ds = bg.getDataset();
    ds.getDefaultModel().write(System.out, "NTRIPLE");
}
```

## Documentation

| Page | What it covers |
|---|---|
| [Instructions](instructions/) | Building, the command-line reference, choosing a conversion engine, very large builds, exporting, verifying, serving, and using BeakGraph from Java. |
| [File format](specification/) | The on-disk format of a BeakGraph store: HDF5 profile, encodings, term order, dictionary, quad indexes and ingest rules. |
| [RDF 1.2 compliance](rdf-1.2-compliance/) | Conformance to RDF 1.1, RDF 1.2 and SPARQL-CDT, and the documented deviations. |
| [Benchmarks](benchmarks/) | The JMH benchmarks for the read path. |
| [Changelog](changelog/) | Release notes and the format version history. |
| [Architecture slides](BeakGraph-HDF5-Architecture.pptx) | The original HDF5 design (PowerPoint). |

## Background

The general idea of BeakGraph is a read-only, searchable, indexed set of binary
[succinct data structures](https://en.wikipedia.org/wiki/Succinct_data_structure) representing an
[RDF dataset](https://www.w3.org/TR/rdf11-datasets/). It was developed to power
[Halcyon](https://github.com/halcyon-project/Halcyon); see the paper at
[arXiv:2304.10612](https://arxiv.org/abs/2304.10612).

BeakGraph is licensed under the
[Apache License 2.0](https://github.com/ebremer/BeakGraph/blob/master/LICENSE).
