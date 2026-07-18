# BeakGraph RDF 1.1 Compliance Assessment

*Assessed 2026-07-02 against [RDF 1.1 Concepts and Abstract Syntax](https://www.w3.org/TR/rdf11-concepts/) (W3C Recommendation, 25 February 2014).
 
## Verdict

Substantially compliant for the core data model as an RDF 1.1 dataset store queried through SPARQL. Three deliberate, documented deviations exist, of which one (numeric literal canonicalization) affects term identity.

## Compliant areas

### Term model and positions
- Subjects are IRI/blank node, predicates IRI, objects IRI/blank node/literal, graph names IRI/blank node — enforced with loud failures rather than silent acceptance of generalized RDF (`PositionalDictionaryWriterBuilder.ProcessQuad`, mirrored in `HugeBuildPipeline.countEntityKind`).
- RDF-star quoted triples are rejected with an exception. Correct behavior for an RDF **1.1** store; quoted triples are RDF 1.2 territory.

### Literals
- Every literal carries its datatype IRI, per RDF 1.1 (simple literals are `xsd:string`; identification inherited from Jena 5).
- `rdf:langString` is fully supported: the `langs`/`langTags` datasets store language tags separately and literals reconstruct term-exact (`MultiTypeDictionaryWriter` / `MultiTypeDictionaryReader`). Tag normalization policy is inherited from Jena's parsers and applied consistently on both write and query paths.
- Ill-typed literals (e.g. `"abc"^^xsd:int`) are valid RDF 1.1 terms and are preserved term-exactly through the strings path instead of being rejected or "repaired".
- Unicode: UTF-8 storage throughout; the front-coded dictionary never splits UTF-16 surrogate pairs (`FCDWriter.commonPrefixLength`).

### Blank nodes
- Blank node identifiers are not part of the RDF 1.1 abstract syntax; graphs are defined up to isomorphism. Rank-based storage (labels regenerated from dictionary ids on read) and the huge writer's keep-original-labels policy are both compliant under that rule.
- Blank node sharing across named graphs within one dataset is preserved, matching TriG/N-Quads dataset scoping.

### Datasets
- Default graph plus named graphs, with IRI and blank-node graph names supported. The default graph is held under ARQ's sentinel IRI internally — a representation detail invisible at the SPARQL level (`NodeComparator` even orders the two ARQ sentinels as distinct terms so data that uses them survives).
- Union-graph queries apply RDF-correct set semantics (explicit row dedup in `HDF5Reader.readUnion`).

### Syntax and query semantics
- Parsing/serialization and SPARQL 1.1 semantics are delegated to Jena 5.x (RIOT/ARQ); BeakGraph sits below as storage.
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

> **Conformance claim: BeakGraph is RDF 1.2-basic conformant** (format v4). RDF 1.2 Concepts §2
> defines basic conformance as supporting graphs/datasets whose triples contain only basic RDF
> terms — i.e. everything except triple terms. BeakGraph stores base-direction literals
> term-exactly, inherits RDF 1.2's case-insensitive language-tag identity from Jena, and rejects
> triple terms loudly. Full conformance is a designed-but-deferred future phase (PLAN.md Phase 3,
> decided 2026-07-16: stand on basic).
>
> **Evidence:** the vendored W3C RDF 1.2 test suites (rdf-turtle, rdf-n-triples, rdf-n-quads,
> rdf-trig; `W3CRdf12SuiteTest`, suites at commit `d3e844a`): **301 tests — 213 executed, 0
> failures**; 82 c14n tests skipped as out of scope (canonical serialization is a serializer
> property, not storage), 6 skipped as upstream Jena 6.1.0 lenient-parse divergences (listed in
> `src/test/resources/w3c/rdf12/README.md`). Every eval and positive-syntax test without triple
> terms round-trips through a real store isomorphically; every triple-term test aborts on a loud
> term-kind guard.

- **Triple terms** (`<<( s p o )>>`, and the reifier/annotation sugar that expands to them):
  rejected with an exception at ingest, as before. Storage is planned (PLAN.md Phase 3).
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
- **Language tag case**: Jena 6 normalizes language tags case-insensitively per RDF 1.2
  (`"chat"@FR` ≡ `"chat"@fr`); BeakGraph inherits this on both the write and query paths.
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
| Generalized RDF / RDF-star | Rejected loudly (correct for 1.1) |
| RDF 1.2 base-direction literals | Stored term-exactly (format v4, `langDirs`); ≤ 0.17.0 stored them silently corrupted — rebuild |
| RDF 1.2 conformance level | **Basic** (W3C suites: 213 executed, 0 failures); full pending triple terms |
| SPARQL-CDT composite literals | Stored term-exact; lexical dictionary order (≤ 0.17.0 stores need rebuild); embedded blank nodes rejected |
| Numeric literal term identity | **Deviation** — canonicalized at ingest |
| Absolute-IRI requirement | **Deviation** — relative IRIs stored, resolved at serving time |
| Dataset faithfulness | **Caveat** — metadata graphs injected |
