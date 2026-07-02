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
| Numeric literal term identity | **Deviation** — canonicalized at ingest |
| Absolute-IRI requirement | **Deviation** — relative IRIs stored, resolved at serving time |
| Dataset faithfulness | **Caveat** — metadata graphs injected |
