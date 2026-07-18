# W3C SPARQL 1.2 test suites (vendored)

Vendored from https://github.com/w3c/rdf-tests (path `sparql/sparql12/`),
commit `d3e844aaa3e2f2b5250f2d1c988ce58870d6bc86` (the same commit as the
vendored RDF 1.2 suites), on 2026-07-18.

Suites: grouping, codepoint-escapes, syntax-triple-terms-negative,
syntax-triple-terms-positive, eval-triple-terms, expression, lang-basedir,
rdf11, version, syntax.

Distributed under the W3C Test Suite License and the W3C 3-clause BSD License
(see the headers in each manifest.ttl).

Runner: `com.ebremer.beakgraph.w3c.W3CSparql12SuiteTest` — a manifest-driven
JUnit @TestFactory. Query-evaluation tests execute over REAL BeakGraph stores
built from each test's data files. To refresh, re-copy from a newer rdf-tests
commit and update the hash above; new manifest entries appear as tests
automatically.

## Runner policies (asserted or aborted-with-reason, never ignored)

- **Update tests** (`mf:*UpdateSyntaxTest`, `mf:UpdateEvaluationTest`):
  BeakGraph stores are read-only, so update *evaluation* aborts with that
  reason; update *syntax* tests still run as pure grammar checks through
  Jena's UpdateFactory (parsing is conformance evidence even where execution
  is out of scope).
- **Query syntax tests** run against `Syntax.syntaxARQ` first, because that is
  what BeakGraph's endpoint deliberately executes (switching to
  `syntaxSPARQL_12` would delete the CDT `UNFOLD`/`FOLD` surface — PLAN §4.0
  Trap 1). A positive-syntax query that only `syntaxSPARQL_12` accepts aborts
  with that reason rather than failing: it is a documented consequence of the
  ARQ-grammar choice, re-checked on every Jena upgrade.
- **Negative-syntax** queries are checked against `syntaxSPARQL_12` (the
  conformance grammar). Any that Jena 6.1.0 accepts anyway abort as upstream
  leniencies, mirroring the RDF 1.2 suite's policy.

## Result at Jena 6.1.0 / BeakGraph format v5

269 tests: 259 executed with 0 failures, 10 skipped-with-reason:

- 3 × update evaluation (read-only store; the suite's update *syntax* tests all
  execute and pass as grammar checks).
- 6 × upstream: Jena's `syntaxSPARQL_12` parser ACCEPTS a negative-syntax query
  (4 in codepoint-escapes, 2 in syntax).
- 1 × upstream: Jena REJECTS the positive-syntax GROUP BY scoping query
  ("Variable used when already in-scope: ?z in (123 AS ?z)" — the SPARQL 1.2
  scoping relaxation is not yet in Jena's grammar).

Zero queries needed `syntaxSPARQL_12` to parse: every positive-syntax and
evaluation query in the suite parses under `syntaxARQ`, so the endpoint's
deliberate ARQ-grammar choice (Trap 1) currently costs no conformance at all.
Re-check the upstream items on every Jena upgrade.
