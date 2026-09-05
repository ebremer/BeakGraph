# BeakGraph RDF 1.1 / RDF 1.2 Compliance Assessment

*Assessed 2026-07-02 against [RDF 1.1 Concepts and Abstract Syntax](https://www.w3.org/TR/rdf11-concepts/) (W3C Recommendation, 25 February 2014); RDF 1.2 / SPARQL 1.2 sections updated 2026-07-18 against the Candidate Recommendation snapshots.*

## Verdict

**RDF 1.2 fully conformant** as a dataset store queried through SPARQL (see the RDF 1.2 section below for the claim and its test-suite evidence), and substantially compliant with RDF 1.1 for the core data model. Three deliberate, documented deviations exist, of which one (numeric literal canonicalization) affects term identity.

## Compliant areas

### Term model and positions
- Subjects are IRI/blank node, predicates IRI, objects IRI/blank node/literal/triple term, graph names IRI/blank node — enforced with loud failures rather than silent acceptance of generalized RDF (`PositionalDictionaryWriterBuilder.ProcessQuad`, mirrored in `HugeBuildPipeline` and `UltraIngest`). Triple-term components carry the same guards (subject IRI/blank, predicate IRI).
- RDF 1.2 triple terms are stored term-exactly as of format v5 (see the RDF 1.2 section); other non-1.1 node kinds are still rejected with an exception.

### Literals
- Every literal carries its datatype IRI, per RDF 1.1 (simple literals are `xsd:string`; identification inherited from Jena 6).
- `rdf:langString` is fully supported: the `langs`/`langTags` datasets store language tags separately and literals reconstruct term-exact (`MultiTypeDictionaryWriter` / `MultiTypeDictionaryReader`). Language-tag case is settled by Jena's `NodeFactory` at Node construction (see "Language tag case" below), on every path BeakGraph uses, so a lang-tagged literal reconstructs term-exact modulo the RFC 5646 formatting of its tag.
- Ill-typed literals (e.g. `"abc"^^xsd:int`) are valid RDF 1.1 terms and are preserved term-exactly through the strings path instead of being rejected or "repaired".
- Unicode: UTF-8 storage throughout; the front-coded dictionary never splits UTF-16 surrogate pairs (`FCDWriter.commonPrefixLength`).

### Blank nodes
- Blank node identifiers are not part of the RDF 1.1 abstract syntax; graphs are defined up to isomorphism. Rank-based storage (labels regenerated from dictionary ids on read) and the huge writer's keep-original-labels policy are both compliant under that rule.
- Blank node sharing across named graphs within one dataset is preserved, matching TriG/N-Quads dataset scoping.

### Datasets
- Default graph plus named graphs, with IRI and blank-node graph names supported. The default graph is held under ARQ's sentinel IRI internally — a representation detail invisible at the SPARQL level (`NodeComparator` even orders the two ARQ sentinels as distinct terms so data that uses them survives).
- Union-graph queries apply RDF-correct set semantics (explicit row dedup in `HDF5Reader.readUnion`).

### Syntax and query semantics
- Parsing/serialization and SPARQL 1.1 semantics are delegated to Jena 6.x (RIOT/ARQ); BeakGraph sits below as storage.
- The value-ordered dictionary preserves term identity: `NodeComparator` breaks value-equal ties on the exact RDF term (`"1"^^xsd:int` vs `"1"^^xsd:integer` keep distinct ids), and provides a self-consistent total order for timezone-sensitive temporal value spaces that agrees with XSD order wherever XSD order is determinate.

## Deviations

### 1. Numeric lexical canonicalization (affects term identity)
`canonicalizeNumericObject` rewrites `xsd:int` / `xsd:long` / `xsd:float` / `xsd:double` **objects** to canonical lexical form at ingest (`"01"^^xsd:int` → `"1"^^xsd:int`). Under RDF 1.1 these are distinct terms with equal values, so:

- the stored graph is value-preserving and D-entailment-equivalent to the source, but **not isomorphic** when the source contains non-canonical spellings;
- `sameTerm`-style lookups for the original spelling miss.

This is forced by the storage design: those four datatypes are stored by binary value and the reader regenerates the lexical form, so non-canonical spellings have nowhere to live. Strict term fidelity would require routing non-canonical numerics through the term-exact strings path (larger files, no numeric range pushdown for those literals). If archival round-tripping ever matters, this is the one behavior worth putting behind an off switch.

Note: `xsd:integer`, `xsd:decimal`, `xsd:dateTime`, booleans, and all other datatypes are stored term-exactly via the strings path. (The `T`-substring on `xsd:dateTime` in `ProcessQuad` feeds string-length *statistics* only; the stored literal is the full lexical form.)

### 2. Relative IRIs stored unresolved
RDF 1.1 abstract syntax requires absolute IRIs. BeakGraph deliberately stores document-relative references (`DataType.RELATIVE_IRI`) unresolved — parsed against a sentinel base, stripped back to relative form, and resolved against the serving URL at query time (`RelativeIRIResolver`). Until resolution, the in-file graph is not pure abstract syntax. This is an intentional extension serving the LWS/document use case.

### 3. Injected metadata graphs
The stored dataset is a superset of the source: VoID/SD metadata (`urn:x-beakgraph:void`) always, spatial index graphs (`urn:x-beakgraph:Spatial`, grid-tile URN graphs) when spatial indexing is enabled. Valid RDF, but `GRAPH ?g` enumerates graphs the source never contained — a faithfulness caveat rather than a spec violation. Relatedly, the `numQuads` attribute counts source quads only, excluding injected metadata.

## RDF 1.2 and SPARQL-CDT inputs

*Added 2026-07-16. Jena 6.x parses RDF 1.2 — and, with CDTs enabled (the default), SPARQL-CDT
composite literals — whether or not the store supports them, so these terms arrive at the writers
regardless. Policy: anything the format cannot represent fails the build loudly.*

> **Conformance claim: BeakGraph is fully RDF 1.2 conformant** (format v5, 2026-07-18). RDF 1.2
> Concepts §2's full conformance requires triple terms on top of basic conformance; BeakGraph
> stores every RDF 1.2 term kind term-exactly — base-direction literals since format v4, triple
> terms (including nesting) since format v5 — inherits RDF 1.2's case-insensitive language-tag
> identity from Jena, and answers the SPARQL 1.2 query surface over them (triple-term patterns
> with embedded variables included).
>
> **Evidence:**
> - The vendored W3C RDF 1.2 test suites (rdf-turtle, rdf-n-triples, rdf-n-quads, rdf-trig;
>   `W3CRdf12SuiteTest`, suites at commit `d3e844a`) run as a pure conformance oracle: **301
>   tests — 215 executed, 0 failures** (every eval and positive-syntax input, triple terms
>   included, round-trips through a real store isomorphically); 82 c14n tests skipped as out of
>   scope (canonical serialization is a serializer property, not storage), 4 skipped as upstream
>   Jena 6.2.0 lenient-parse divergences (listed in `src/test/resources/w3c/rdf12/README.md`;
>   `W3CRdf12SuiteTest.knownUpstreamLeniencesStillHold` pins that list).
> - The vendored W3C SPARQL 1.2 test suites (`W3CSparql12SuiteTest`, same commit): **269 tests —
>   266 executed, 0 failures**, query-evaluation entries executed over real BeakGraph stores; 3
>   skipped-with-reason (update evaluation — the store is read-only). The 7 grammar divergences
>   of Jena 6.1.0 were resolved upstream in 6.2.0.
> - Both suites run on every writer engine (`-Dbeakgraph.test.engine`; verified on all six).

- **Triple terms** (`<<( s p o )>>`, and the reifier/annotation sugar that expands to them):
  **stored and matched term-exactly as of format v5**, across all six writer engines. Storage is
  structural (a fixed-stride `tripleTerms` component-id store in the literals section, whose
  contiguous suffix triple terms occupy; nested terms resolve recursively), so blank nodes inside
  triple terms keep co-referring with the graph through rank relabeling — the property that had to
  be rejected for CDT literals falls out of the design here. Term-exact round-trip, export
  (including the fastpath, byte-identical), `-verify -deep`, and the SPARQL 1.2 surface
  (`TRIPLE`/`SUBJECT`/`PREDICATE`/`OBJECT`/`isTRIPLE`, patterns with embedded variables unified at
  the id level) are all covered by `RDF12TripleTermTest`, `TermFidelityTest`, `ExportTest`,
  `VerifyCommandTest`, and the two W3C suites. `TermFidelityTest` runs its triple-term fixture on
  all six engines; the W3C suites and `RDF12TripleTermTest` build through the engine named by
  `-Dbeakgraph.test.engine` (method 0 by default; CI's Linux leg re-runs them on methods 1 and 3). Stores without triple terms are byte-identical in
  shape to v4 output; v5 files are rejected by older builds via the format-version gate.
- **Base-direction literals** (`"x"@en--ltr`, `rdf:dirLangString`): **stored and matched
  term-exactly** as of format v4 — a `langDirs` column (0=none, 1=ltr, 2=rtl) beside the existing
  `langs`/`langTags` datasets, mirrored across all six writer engines, the disk writers' spill
  codec, the HLL statistics hash, and the export fastpath (`DirLangRoundTripTest`,
  `DirLangSpillCodecTest`, `TermFidelityTest`, `ExportTest`). v3 files read unchanged (no
  directions); v4 files are rejected by older builds via the format-version gate.
  **History: versions ≤ 0.17.0 silently stored these as plain lang-tagged terms** — `"x"@en`, a
  different RDF term, with nothing recording the change. `-verify` cannot detect it retroactively
  (the file is internally consistent; it is just not what the source said). Rebuilding an affected
  source under a guarded or v4 build produces the correct terms — a diff against the old store is
  the detection mechanism.
- **Language tag case**: RDF 1.2 makes language tags case-insensitive for identity. The mechanism
  is Jena's `NodeFactory.createLiteralLang` / `createLiteralDirLang`, which format every tag to
  RFC 5646 case (`LangTagX.formatLanguageTag`: `en`, `en-US`, `zh-Hant`; tags that are not
  well-formed BCP 47 fall back to a basic lower/upper-casing) at Node construction — on every path
  BeakGraph uses: RIOT for every syntax (independent of the parser's checking mode), ARQ query
  parsing, `STRLANG`, the disk engines' spill codec (`NodeCodec`) and
  `MultiTypeDictionaryReader.extract`. `Node.equals` and `NodeComparator` are then
  case-SENSITIVE on that formatted form, so `"chat"@FR` and `"chat"@fr` become one Node, one
  dictionary entry, findable by either spelling. Consequence: the stored, exported and `LANG()`
  spelling is the formatted form (`"x"@EN-us` → `"x"@en-US`) — term-preserving under RDF 1.2 but
  not byte-preserving. The invariant holds only for Nodes built through `NodeFactory`;
  `LiteralLabelFactory.createLang` bypasses the formatting and must not be used to build lookup
  keys (`LanguageTagRoundTripTest.mixedCaseTagsAreOneTermSpelledByJena` pins this).
- **Composite (cdt:) literals** (`cdt:List` / `cdt:Map`; SPARQL-CDT is an Unofficial Draft spec):
  stored term-exactly via the strings path and queryable with Jena's 16 `cdt:` functions, the
  `FOLD` aggregate, and the `UNFOLD` operator (locked in by `CdtLockInTest`). Three policies:
  - The dictionary orders composite literals by **(datatype IRI, lexical form)**, never by value:
    CDT has no canonical form, so term identity is lexical identity, and the value comparison mixed
    with its error fallback was not a total order (a verified comparator cycle;
    `NodeComparatorCdtTest`). **Stores built by ≤ 0.17.0 that contain composite literals used value
    order and must be rebuilt** — dictionary ids are comparator ranks, and the readers cannot
    version-gate a rank change.
  - **Blank nodes inside composite literals are rejected at ingest** (`CdtBlankNodeGuardTest`):
    BeakGraph regenerates blank-node labels from dictionary rank, so a label inside a literal's
    lexical form would silently stop co-referring with the graph, which SPARQL-CDT §5.2 requires.
  - Ill-formed composite lexical forms never reach BeakGraph from documents: RIOT's CDT-aware
    default profile rejects them at parse.

## Summary table

| Aspect | Status |
|---|---|
| Term positions (S/P/O/G kinds) | Compliant, enforced |
| Literal datatypes, `xsd:string` identity | Compliant |
| `rdf:langString` / language tags | Compliant |
| Ill-typed literals | Compliant (preserved term-exact) |
| Blank node semantics | Compliant (isomorphism) |
| Datasets / named graphs | Compliant |
| Generalized RDF | Rejected loudly |
| RDF 1.2 triple terms | Stored term-exactly (format v5, `tripleTerms` component store), all engines; SPARQL 1.2 patterns answered |
| RDF 1.2 base-direction literals | Stored term-exactly (format v4, `langDirs`); ≤ 0.17.0 stored them silently corrupted — rebuild |
| RDF 1.2 conformance level | **Full** (RDF suites: 215 executed, 0 failures; SPARQL 1.2 suites: 266 executed, 0 failures; Jena 6.2.0) |
| SPARQL-CDT composite literals | Stored term-exact; lexical dictionary order (≤ 0.17.0 stores need rebuild); embedded blank nodes rejected |
| Numeric literal term identity | **Deviation** — canonicalized at ingest |
| Absolute-IRI requirement | **Deviation** — relative IRIs stored, resolved at serving time |
| Dataset faithfulness | **Caveat** — metadata graphs injected |
