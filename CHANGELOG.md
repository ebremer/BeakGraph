# Changelog

Notable changes to BeakGraph and its on-disk format. The format history is
normative (SPECIFICATIONS.md §4.1); the rest is a summary of what a release
brought and what a reader must rebuild.

## Unreleased (branch rdf12andcdt, after 0.18.0)

* Jena 6.2.0, jHDF 0.13.0.
* The reference comparator is a strict total order: numbers of every XSD
  numeric datatype order by exact value (range pushdown widens its bounds to
  ARQ's promoted comparison), dateTime and the g* kinds share one instant
  space with a fixed kind rank, and cross-store bindings are re-resolved.
  Stores built by earlier 0.18.0 builds that mix numeric datatypes or g* and
  dateTime literals under one predicate should be rebuilt.
* Relative IRIs resolve against a deep sentinel base, so `<../x>` and `</x>`
  keep their parent / path-absolute forms; `-merge` stores each document's
  references relative to `-src`. Earlier stores collapsed them onto the child
  form (SPECIFICATIONS.md §9.2).
* Composite (cdt:) literal constants no longer drive range pushdown (their
  dictionary order is lexical, ARQ's is by value).
* The endpoints honour the SPARQL Protocol `default-graph-uri` /
  `named-graph-uri` parameters; `-export -base`; `-verify` of degenerate
  stores; the empty-store export fast path; locale-independent identifiers
  and case mapping throughout.
* Build: no unused dependencies or resolution repositories, dependency pins
  synced with Jena 6.2.0, value-based `-Dhdf5.ffm=true` backend selection, a
  rolling log file, regenerated native-image metadata, CI packages the jars
  and the benchmarks module and can build the native binary.

## 0.18.0 - format v5 (2026-07)

* **Format v5**: RDF 1.2 triple terms (`<<( s p o )>>`, nested) stored
  term-exactly in a `tripleTerms` component store in the literals section
  (SPECIFICATIONS.md §7.6a). v3/v4 files read unchanged; v5 files are rejected
  by 0.17.0 and earlier with an "Upgrade BeakGraph" error.
* **Format v4**: `rdf:dirLangString` base directions in an optional
  `langDirs` dataset (§7.5.3). **Versions up to 0.17.0 silently stored
  `"x"@en--ltr` as `"x"@en`**; rebuild affected sources - the file cannot
  reveal the loss.
* Composite (cdt:) literals order lexically in the dictionary (term identity
  is lexical identity; the value comparison was not a total order). Stores
  built by 0.17.0 or earlier that contain them must be rebuilt.
* Blank nodes inside composite literals are rejected at ingest.
* Six writer engines (`-method 0..5`) targeting one format; the disk engines
  (1/4/5) bound RAM by spilling and need the native HDF5 library.
* `-export`, `-verify [-deep]`, `-merge`, per-file `-threads`, VoID
  statistics opt-in (`-void` / `-voidsketch`), reader tuning properties.

## 0.17.0 and earlier

* Format v3: rank/select directory corrected (v1-2 directories are ignored by
  readers, which fall back to linear bitmap scans).
* HDF5 container replacing the original Apache Arrow / RO-Crate design.

## Format v5 design notes

Code comments cite these by name; they record decisions made while adding
RDF 1.2 support, so the rationale stays in the repository.

* **One classifier**: whether a pattern term is concrete, a variable, or a
  triple term with embedded variables is decided in exactly one place
  (`BGIteratorMaster`); a second, subtly different classification in an
  iterator once returned every row for a bound variable.
* **syntaxARQ at the endpoint**: the endpoint parses queries with Jena's ARQ
  grammar, which already covers SPARQL 1.2 triple terms and the CDT
  `FOLD`/`UNFOLD` surface; `syntaxSPARQL_12` drops `UNFOLD`. The conformance
  cost is re-checked on every Jena upgrade
  (src/test/resources/w3c/sparql12/README.md).
* **Interior terms are dictionary members**: an IRI, blank node or literal
  that occurs only inside a triple term still gets a dictionary id, so the
  component store can reference it; the writers collect interior terms
  during the same pass as top-level terms.
* **Component resolution**: triple terms are stored as component ids into
  the entities, predicates and literals sections and resolved recursively;
  the disk engines resolve components with a reference join over the sorted
  term runs rather than an in-memory map.
* **No widened structs**: the fixed-stride component record is never
  widened for a special case (repeated variables, nesting); such cases are
  handled by the matcher, keeping every engine's writer identical.
* **Mirror topology hazard**: six engines implementing one rule by hand is
  the recurring source of divergence; shared helpers (`RdfSources.parser`,
  `Params.gridGraph`, `RelativeIris`, `WriterEngines` in tests) exist to
  keep the rule in one place.
