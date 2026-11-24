# The rdf-array contrib suite (vendored)

Vendored from https://github.com/rdf-array/rdf-tests-contrib, branch `rdf-undef`
(`rdf/`, `sparql/`), commit in `rdf-tests-contrib.commit`, on 2026-11-24, by way of
the copy in Jena's `rdf-array` branch (`jena-arq/testing/rdf-array`). Do not edit
the copy: change rdf-tests-contrib and copy again.

Distributed under the W3C test-suite dual licence, as the W3C suites are (the
repository's LICENSE.md:
https://www.w3.org/Consortium/Legal/2008/04-testsuite-copyright.html).

Runner: `com.ebremer.beakgraph.rdfarray.RdfArrayContribSuiteTest`, a
manifest-driven JUnit `@TestFactory` (RDFARRY.md step 4).

- **RDF syntaxes** (Turtle, TriG, N-Triples, N-Quads). Parsing is Jena's layer: a
  negative test must be refused by the fork's parser, and an eval test's parse must
  match its expected result. Then the containment split of the W3C runner: a
  document holding a sequence subject (or a sequence inside a triple term) must
  fail its build on BeakGraph's guard, which rdf-array's data model of 2026-11-03
  asks for (SPECIFICATIONS.md §9.5); any other must build into a store isomorphic
  to it. The expected side is compared after the numeric canonicalization the
  store applies at every depth (§9.3): `1e0` comes back as `1.0E0`.
- **SPARQL.** Each query-evaluation test runs over a store built from its data, in
  an HDF5 file and in a Zarr store.

Aborted, with the reason, never dropped:

- the 7 update-evaluation tests: BeakGraph stores are read-only;
- `results-csv` (both containers): Jena's CSV writer has no sequence spelling
  (`?`), the same over an in-memory dataset; the runner checks that BeakGraph's
  output equals Jena's own before it aborts.

Currently 349 tests: 340 pass, 9 aborted.

The fork's own engine tests (`TestUndefSlots`, `TestSequenceQueries`,
`TestArraySyntaxExamples`) run over BeakGraph stores too, ported to
`src/test/java/com/ebremer/beakgraph/rdfarray/*StoreTest.java` (Apache License 2.0,
ASF headers retained). Cases that cannot run on a read-only store, or that need a
sequence subject, are `@Disabled` with the reason.
