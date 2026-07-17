# SPARQL-CDTs test suite (vendored)

Vendored from https://github.com/awslabs/SPARQL-CDTs (path `tests/`),
commit `bc8fe1a560038ce75c32b8f529216a94fef23116`, on 2026-07-16. Apache-2.0 (see LICENSE, NOTICE).

658 mf:QueryEvaluationTest entries across unfold, fold, list-functions,
map-functions, orderby, and bnodes sub-manifests.

Runner: `com.ebremer.beakgraph.cdt.SparqlCdtSuiteTest` — each test's data
builds a real BeakGraph store (cached per distinct data file) and the query
runs over it. Tests whose DATA contains blank nodes inside composite literals
assert BeakGraph's documented rejection instead (the reject-at-ingest policy;
see RDF_1.1-compliance.md).
