# BeakGraph JMH Benchmarks

[JMH](https://github.com/openjdk/jmh) benchmarks for the BeakGraph **read path**. Standalone
Maven project on purpose: it depends on the *installed* BeakGraph artifact, so benchmark code
never touches the main build or its shaded artifact.

## Build

```bash
# 1. Install the current BeakGraph into the local Maven repo (repository root)
mvn -DskipTests install

# 2. Build the benchmarks jar (this directory)
cd benchmarks
mvn package
```

`beakgraph.version` in `benchmarks/pom.xml` must equal the root POM's `<version>` (CI
builds this module against the freshly installed root artifact, so a stale pin fails
there); `-Dbeakgraph.version=<version>` overrides it for one build.

## Run

```bash
# Everything (takes a while)
java -jar target/benchmarks.jar

# One benchmark class / method, by regex
java -jar target/benchmarks.jar QueryBench
java -jar target/benchmarks.jar "BitPackedBufferBench.randomGet"

# List what's available
java -jar target/benchmarks.jar -l
```

The first run of the store-backed benchmarks (`SelectBench`, `DictionaryBench`, `QueryBench`)
generates a deterministic synthetic store into `target/jmh-data/store-<subjects>.h5` and
reuses it afterwards. Delete that directory (or `mvn clean`) to force a rebuild.

### Useful knobs

```bash
# Store size (subjects; the store holds 4 triples per subject + subjects/4 tagged quads)
java -jar target/benchmarks.jar QueryBench -p subjects=200000

# Quick-and-dirty iteration while developing (not for reported numbers)
java -jar target/benchmarks.jar QueryBench.pointLookup -f 1 -wi 2 -i 3 -p subjects=5000

# Allocation rates per op (watch this when attacking per-row garbage)
java -jar target/benchmarks.jar QueryBench.chainJoin -prof gc

# Flame graphs, if async-profiler is installed
java -jar target/benchmarks.jar QueryBench.predicateScan -prof "async:output=flamegraph"

# JSON results for before/after comparison (results/local/ is git-ignored)
java -jar target/benchmarks.jar -rf json -rff results/local/results.json
```

For trustworthy numbers: plug in the laptop, close the browser, and prefer `-f 2` (two forks)
or more for anything you plan to quote.

## What is measured

| Class | Level | Targets |
|---|---|---|
| `BitPackedBufferBench` | primitive | `BitPackedUnSignedLongBuffer.get()` (random + sequential), `binarySearch`, the streaming decoder — the innermost decode loop everything else sits on; `profile=mixed` first drives the `get()` call site through all three `RandomAccessBytes` implementations, as a JVM that opened small, FFM-mapped and remote datasets has (BG-260) |
| `SelectBench` | primitive | `select1` via the rank/select directory vs the linear fallback, plus the adjacent-pair pattern the iterators actually issue |
| `DictionaryBench` | dictionary | `locate` (entity/object/predicate hits, misses), `extract`, and the Caffeine-cached node-table path |
| `QueryBench` | end-to-end | SPARQL shapes: point lookup, star join, predicate scan, FILTER range pushdown, chain join (per-binding iterator reconstruction), `GRAPH ?g` scan, and a Graph-API full scan |

Benchmarks are single-threaded by default; the store and its readers are safe for concurrent
reads, so `-t <n>` is a valid experiment for contention questions (the probe cursors are
deliberately racy — they only pick probe values).

## Baseline discipline

When working on a read-path optimization:

1. Record a baseline first: `java -jar target/benchmarks.jar <relevant regex> -rf json -rff results/local/baseline.json`
   (`results/local/` is git-ignored; results are not committed - the few reference
   runs kept under `results/` are named by date, commit and machine, see `results/README.md`)
2. Make the change in the main project, then `mvn -DskipTests install` at the root and
   `mvn package` here again (the jar embeds BeakGraph classes — rebuilding the benchmarks
   jar is required to pick up main-project changes).
3. Re-run the same selection and compare against the baseline, ideally with `-prof gc` when
   the change is about allocation.
