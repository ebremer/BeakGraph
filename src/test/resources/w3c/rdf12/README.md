# W3C RDF 1.2 test suites (vendored)

Vendored from https://github.com/w3c/rdf-tests (path `rdf/rdf12/`),
commit `d3e844aaa3e2f2b5250f2d1c988ce58870d6bc86`, on 2026-07-16.

Suites: rdf-turtle (syntax, eval), rdf-n-triples (syntax, c14n),
rdf-n-quads (syntax, c14n), rdf-trig (syntax, eval).

Distributed under the W3C Test Suite License and the W3C 3-clause BSD License
(see LICENSE.md and the headers in each manifest.ttl).

Runner: `com.ebremer.beakgraph.w3c.W3CRdf12SuiteTest` — a manifest-driven
JUnit @TestFactory. To refresh, re-copy from a newer rdf-tests commit and
update the hash above; new manifest entries appear as tests automatically.

## Known upstream divergences (Jena 6.1.0)

Six negative-syntax documents are ACCEPTED by Jena 6.1.0's parser; the runner
reports them as aborted ("upstream: Jena accepts this negative-syntax
document") rather than failed, because parsing is Jena's layer and anything it
hands over still passes BeakGraph's term-kind guards:

- rdf-turtle/syntax: turtle12-surrogate-pair-bad-01, turtle12-surrogate-pair-bad-02
- rdf-n-triples/syntax: ntriples12-bad-iri-1, ntriples-langdir-bad-3,
  ntriples-langdir-bad-4, ntriples-langdir-bad-5

Re-check this list on any Jena upgrade: entries disappearing from the aborted
set mean Jena tightened its parser.
