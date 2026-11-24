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

## rdf-array (branch `rdfarray`)

Under rdf-array a Turtle collection `( … )` is one sequence term, not an
`rdf:first`/`rdf:rest` ladder, and a sequence may not be a subject (rdf-array's
data model of 2026-11-03). Two positive-syntax documents put a collection in the
subject position: rdf-turtle/syntax turtle12-inside-2 and rdf-trig/syntax
trig12-inside-2.

- Under rdf-array they hold a sequence subject. The runner's containment split
  therefore requires the build to fail on BeakGraph's subject guard, as it
  requires for any unsupported term.
- Format v7 stores a sequence as an object, on every engine (RDFARRY.md steps
  3 and 5): a document whose collections are objects is built and checked for
  isomorphism like any other.
- The executed count is unchanged.

## Known upstream divergences (Jena 6.2.0)

Four negative-syntax documents, all in rdf-n-triples/syntax, are ACCEPTED by
Jena 6.2.0's parser; the runner reports them as aborted ("upstream: Jena
accepts this negative-syntax document") rather than failed, because parsing is
Jena's layer and anything it hands over still passes BeakGraph's term-kind
guards:

- rdf-n-triples/syntax: ntriples12-bad-iri-1, ntriples-langdir-bad-3,
  ntriples-langdir-bad-4, ntriples-langdir-bad-5

Jena 6.1.0 additionally accepted rdf-turtle/syntax
turtle12-surrogate-pair-bad-01/02; 6.2.0 rejects them.

`W3CRdf12SuiteTest.knownUpstreamLeniencesStillHold` parses exactly this list
and fails when Jena starts rejecting one of them, so a Jena upgrade updates
this README and RDF_1.2-compliance.md instead of silently changing the
executed count (currently 301 tests: 215 executed, 82 c14n skipped, 4
upstream).
