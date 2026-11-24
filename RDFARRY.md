# rdf-array on BeakGraph: a plan

- Written: 2026-11-24, on branch `rdfarray` (at `6d58aa2`, the same as `master`)
- Target: [rdf-array](https://github.com/rdf-array/rdf-array/tree/rdf-undef) on its `rdf-undef` branch, which
  is rdf-array plus empty slots. The plan follows these documents: `docs/rdf-array.md` (data model adopted
  2026-11-03), `docs/rdf-undef.md` (2026-11-01), `docs/array-syntax.md`, the two SPARQL 1.2 redlines
  (`sparql-query.js`, `sparql-query-undef.html`) and `docs/rdfc-array.md`
- Reference implementation: [jena-array](https://github.com/rdf-array/jena-array), branch `rdf-undef`
  (`TestUndefSlots`, `RDF-ARRAY-HANDOFF.md`)
- Status: steps 1-3 done (2026-11-24): the v7 spec, the Jena port, and v7 storage on `-method 0` and
  `-method 2`. Step 4, correct queries, is next

rdf-array adds one kind of RDF term, the **sequence**. `( 1 2 3 )` is one term holding three terms in order,
and `()` is the empty sequence. Sequences nest, and they stand where an object stands. rdf-undef adds the
**slot**: `( 1 undef 3 )` has three positions and nothing at the second. This plan has three parts:

- how BeakGraph stores both kinds structurally in its id-based format;
- how it also lays their numeric content out as **native typed arrays**: HDF5 datasets in an `.h5` store
  and Zarr arrays in a `.zarr` store, which h5py, zarr-python, numpy and xarray open directly;
- how its query engine answers SPARQL-array over them.

---

## 0. Summary

**The decisions:**

| # | Decision | Section |
|---|---|---|
| D1 | Port jena-array's `rdf-undef` branch from Jena 5.0.0-SNAPSHOT onto **Jena 6.3.0**, as the `rdf-array` branch of the local Jena clone (version `6.3.0-array-SNAPSHOT`), which BeakGraph's `rdfarray` branch builds against. BeakGraph does not downgrade, because a downgrade would change what its stores mean | §3.1 |
| D2 | **Value semantics**, as jena-array has today: equal sequences are one term and duplicate quads collapse. Sequences are allowed only as objects and as elements. The storage leaves room for occurrence counts later | §3.2 |
| D3 | **Order:** sequences are a fifth macro kind, after triple terms. They compare element by element, a slot sorts below every term, and a proper prefix comes first. This is rdf-array's own order, and it makes id order equal to SPARQL-array's `ORDER BY` | §3.3 |
| D4 | **Canonical storage (format v7):** sequences are the last rows of the literals section, `DataType.SEQUENCE = 14`. Their elements are object-space ids in two block-structured arrays (`sequenceStarts`, `sequenceElements`), with **id 0 for a slot**. Nested sequences are terms of their own | §3.4 |
| D5 | **Native arrays (optional, `-arrays`):** the numeric sequences are also written under `/.BG/arrays` as typed N-D tensors or ragged arrays, each with a mask for slots. Sequences that mix strings, numbers and dates in fixed positions become record tables, one typed array per position. Each container uses its own facilities: HDF5 datatypes and filters in HDF5; `data_type`, `dimension_names` and sharding in Zarr. The two containers must hold the same values, not the same bytes | §3.5, §3.5a |
| D6 | **Reading:** a stored sequence comes back as a **lazy `Node_Seq`**. It reports its size from `sequenceStarts` and decodes an element only when asked, so Jena's sequence functions touch only what they read | §3.6 |
| D7 | **Engine in two layers.** First, correct everywhere by handing sequence patterns to jena-array's solver. Then, fast on ids where it pays. Slot handling costs nothing on a store that holds no slot (`numSlots = 0`) | §3.7 |
| D8 | **Versioning:** v7 holds sequences, slots and the optional arrays. v8, after measurement, takes packed numeric sequences out of the dictionary | §3.8 |

**The plan at a glance.** P0 blocks everything; P1 is the first usable store; P2 completes it and adds the
native arrays; P3 is performance; P4 is gated on measurement.

| Step | What | Priority | Size | Needs | Delivers |
|---|---|---|---|---|---|
| 1 | Spec text for v7, the open decisions, the `SeqTerms` facade | P0 | S | — | `SPECIFICATIONS.md` draft, ordinal 14 reserved |
| 2 | jena-array on Jena 6.3.0, plus a lazy `Node_Seq` constructor | P0 | L | — | Turtle-array, SPARQL-array and the results formats on 6.3.0 |
| 3 | v7 storage on `-method 0`: ingest, dictionary, extract, locate, `-verify`, `-export` | P1 | M | 1, 2 | stores with sequences and slots |
| 4 | Correct queries through jena-array, guards, slot-aware variable analysis | P1 | M | 3 | the rdf-array contrib suites pass: **milestone A** |
| 5 | The other engines: `-method 2, 3`, then `1, 4, 5` | P2 | L | 3 | engine parity, CI matrix |
| 6 | Native arrays `/.BG/arrays` in HDF5 and Zarr | P2 | L | 3 (5 for the disk engines) | tensors, ragged arrays and record tables (strings, dates) that h5py, zarr-python, pandas and xarray read: **milestone B** |
| 7 | Id-native sequence matching, `UNNEST` and functions | P3 | L | 4 | speed |
| 8 | Sequence statistics in VoID; an optional element index | P3 | M | 7 | containment queries, join costs |
| 9 | v8: packed numeric sequences stored only as arrays | P4 | XL | 6, 7, a measurement | numeric leaves leave the dictionary |
| 10 | Later: occurrences, sequences inside triple terms, ladder folding, … | — | — | — | — |

---

## 1. What rdf-array and rdf-undef ask of a store

1. **A fifth term kind.** A sequence is a finite, ordered, possibly empty list of terms of any kind: IRIs,
   blank nodes, literals, triple terms and sequences.
2. **Positions.** A sequence may be the object of a triple or an element of a sequence. It may never be a
   subject, a predicate or a graph name.
   - This is the data model adopted on 2026-11-03.
   - jena-array still accepts a sequence as a subject.
3. **Equality and identity.**
   - Equality: two sequences are equal when their elements are equal pairwise.
   - Identity, as the 2026-11-03 model has it: each sequence written is an occurrence of its own, and a
     triple holding one may occur more than once.
   - No implementation follows that identity rule yet, jena-array included: equal sequences are still one
     term. The spec lists "the cost of occurrence joins" as open, with joins by equality as its fallback.
4. **Order.** Sequences come after literals and triple terms. Two of them compare element by element, and
   the shorter of a prefix pair comes first. Under rdf-undef, a slot sorts before every term.
5. **Slots (rdf-undef §1).**
   - `undef` occurs only as an element, at any depth.
   - A slot is not a term and not an unknown value: `( 1 2 3 )` and `( 1 undef 3 )` do not entail one
     another.
   - It counts toward the length, and it equals only a slot.
6. **Ladders are gone.** `rdf:first`, `rdf:rest` and `rdf:nil` are ordinary IRIs, and `()` is not `rdf:nil`.
   A Turtle collection is now one sequence term.
7. **SPARQL-array.**
   - Sequence patterns may hold constants, variables, nested patterns, the gap `...`, the named gap
     `...?rest` and `undef`.
   - `UNNEST(expr AS ?x AT ?i)`.
   - Functions and operators: `isSEQ`, `SEQLEN`, `SEQ(…)`, `s[i]`, `s[m...n]`, `?x IN ?seq`, and `=` and `<`
     on sequences.
   - `ORDER BY` uses the order above, and `DISTINCT` / `GROUP BY` compare by equality.
   - `CONSTRUCT` writes sequences.
8. **Slots in SPARQL (rdf-undef §3).**
   - In patterns: a variable at a slot stays unbound, and a constant does not match a slot. `undef` in a
     pattern matches only a slot, and the length must still match.
   - Joins: a variable bound elsewhere joins through a slot, as with `VALUES`.
   - Gaps: `( ... ?m ... )` and `UNNEST` give a solution at a slot, with the element unbound. A named gap
     keeps slots.
   - Functions:
     - `s[i]` at a slot is an error; slices keep slots.
     - `SEQLEN` counts slots, `IN` skips them, and `=` lines slots up with slots.
     - `SEQ(?unbound)` makes a slot.
   - Output: an unbound variable inside a `CONSTRUCT` template's sequence writes a slot.
9. **Results.**
   - JSON: `{"type": "seq", "value": [ … ]}`, with `{"type": "undef"}` for a slot.
   - XML: `<seq>`, with `<unbound/>` for a slot.
   - CSV: the sequence as one cell. TSV: the Turtle spelling.
10. **Canonicalization (RDFC-array).** Blank nodes inside sequences take part. BeakGraph's isomorphism tests
    need this.

BeakGraph is read-only, so the spec's update rules (`INSERT`/`DELETE`, occurrence doubling) reach it only
through `CONSTRUCT`.

---

## 2. Where BeakGraph starts

### 2.1 The format

- **Ids and the object space.**
  - A term's id is its rank in the §6 total order.
  - The object space is `entities ⊕ literals`, and it is sorted.
  - RDF 1.2 triple terms are stored **structurally**, as the literals section's last rows (`DataType.TRIPLE_TERM
    = 13`) with a stride-3 component store, `tripleTerms`.
  - That is the template a sequence follows, with variable arity in place of three components.
- **A sequence is refused today.** Every engine refuses one at build time:
  - `NodeComparator.getMacroType` throws;
  - so do `DictionaryNodeEncoder`, `NodeCodec`, `ProcessQuad`, `UltraIngest` and `HugeBuildPipeline.acceptRow`.
- **On the query side a sequence pattern fails loudly, never with wrong rows.** It is taken for a bound
  constant and throws in `locate`.
- **The suffix assumption.** The reader assumes triple terms are the **last** rows of the literals section,
  in `PositionalDictionaryReader` and `MultiTypeDictionaryReader.checkTripleTermSuffix`. The iterator clamps
  set only a minimum.
- **`DataType` width.** The column is 5 bits wide, and 15 ordinals still fit. Every reader takes the width
  from the `width` attribute (`StoreProfile.packed`).

### 2.2 Jena

BeakGraph's `master` is on Jena 6.2.0, and its `rdfarray` branch has moved to 6.3.0 (§3.1). jena-array
is Jena **5.0.0-SNAPSHOT** (fhircat/jena `all-rdf` @744357a559).

- **The fork is behind 6.x:**
  - It has the old RDF-star names (`isNodeTriple`, `createTripleNode`, `getLiteralTextDirection`).
  - It has no `org.apache.jena.cdt` and no `LangTagX` / `TitaniumJsonLdOptions`.
  - Its grammar has no `<<( )>>`. In 5.0, Turtle `<< >>` makes a triple term, where 6.x makes a reifier.
- **Downgrading BeakGraph would cost:**
  - about 65 edits in main code and 110 in tests;
  - losing SPARQL-CDT and RDF 1.2;
  - building different stores from the same Turtle.

  It is not an option.

### 2.3 Engine coupling

The coupling is low, and it fails safe:

- **`OpExecutorBG`** overrides only `OpDistinct`, `OpConditional`, `OpGroup`, `OpGraph`, `OpFilter` and
  `OpBGP`, and calls `super` for any shape it does not take.
- **`OpUnnest` would fall through** to the fork's `OpExecutor`. That executor evaluates the sub-pattern
  first, and the sub-pattern comes back into BeakGraph's engine, on ids.
- **Expressions:** all evaluation is Jena's (`GraphVarBG` uses `isSatisfied`, and placed filters stay as the
  check). `NodeValues.of` wraps a non-literal as `NodeValueNode`, as the fork does for a sequence. The
  sequence functions therefore work over BeakGraph rows unchanged.
- **Results:** they are written by Jena: `ResultSetFormatter` JSON, CSV and XML in `BGSparqlService`,
  `StreamRDFWriter` for N-Triples and Turtle, and Fuseki's own `/rdf` service. They gain `seq` and `undef`
  with the fork.
- **Delegating patterns:** `HDF5Reader.graphBaseFind` already turns a non-concrete term into a wildcard, so
  jena-array's `SolverRX3`/`SolverRX4` can match sequence patterns over `find(s, p, ANY)`.
- **The limit:** `BindingNodeId` holds only `(Var, long id)` pairs. A named gap's run, an `UNNEST`
  position, or a value that is not in the dictionary has no id.

### 2.4 Falcon 0.3.0, which BeakGraph pins

| | HDF5 | Zarr v3 |
|---|---|---|
| Typed N-D arrays (int8–64, uint, float32/64, bool), either byte order | yes | yes |
| Chunking; filters or codecs | deflate, shuffle, zstd, blosc, … | zstd, gzip, blosc, crc32c, `sharding_indexed` |
| Zero-copy mapping | contiguous, unfiltered datasets of any type and rank (`storageSegment`) | raw chunks of any dtype and rank (`hasRawChunks`, `chunkSegment`) |
| Variable-length numeric rows | vlen sequences. The payload sits uncompressed in the global heap and cannot be mapped | **none** (only vlen-utf8 / vlen-bytes) |
| Dimension labels | **no dimension scales**: an explicit non-goal, and not fakeable, because `DIMENSION_LIST` needs vlen of references, which the writer refuses | `dimension_names` |
| References | object and region references | none |
| Ragged / Arrow-style list layouts | nothing built in | nothing built in |

BeakGraph's store abstraction (`WritableStore`, `OpenStore`) is narrower than Falcon:

- It knows **1-D byte arrays and int/long attributes only**.
- `ZarrStoreWriter` writes its chunk files itself and uses Falcon only for metadata.
- `Hdf5Store.array()` throws on a non-contiguous dataset.
- `ZarrStore.array()` throws on anything but rank-1 uint8 with raw chunks.

---

## 3. Design

### 3.1 The dependency (D1)

**Where the port lives (decided 2026-11-24).**

- **The branch.** It is the `rdf-array` branch of the local Jena clone (the `ebremer/jena` fork), started at
  the `jena-6.3.0` tag. Its first commit (`7ee350db0b`) renames the version to `6.3.0-array-SNAPSHOT`, so
  installing it never replaces stock 6.3.0 in a local repository.
- **The build.** The reactor that `apache-jena-libs` and `jena-fuseki-main` pull in is built and installed
  from that branch.
  - Fuseki UI's Node and yarn steps fail on Windows (files locked by the scanner), and BeakGraph never
    loads the UI. That module is therefore built with
    `-Dskip.installnodecorepack=true -Dskip.corepack=true -Pui-skip-tests`.
- **The pins.** BeakGraph's `rdfarray` branch sets `jena.version` to `6.3.0-array-SNAPSHOT` and re-syncs
  the pins that 6.3.0 moved (slf4j 2.0.20, caffeine 3.3.1, commons-lang3 3.22.0, commons-codec 1.23.0;
  BG-195). `master` stays on stock 6.2.0.

**What the port carries.** jena-array's changes to jena-core and jena-arq, renamed to 6.x names:

- jena-array is 15 commits over fhircat/jena `all-rdf` @744357a559.
- In jena-core, jena-arq and jena-base they come to about 105 main files. Six of those are generated
  JavaCC parsers, about 9,000 of the lines, which are regenerated rather than merged. The hand-written part
  is about 2,500 lines.
- Tests and test data add about 330 more files, mostly new.
- **Left out:**
  - **jena-shex**: its rdf-array changes sit on all-rdf's ShEx rework (Iovka Boneva's 121 commits),
    which upstream Jena does not have.
  - **The `jena-rdfarray` demo module.**
  - **jena-shacl**: optional; BeakGraph does not use it.

**What the trial merge found (2026-11-24).** I squashed `rdf-undef` over its branch point and
cherry-picked it onto the branch with rename detection, in a throwaway worktree:

- **Conflicts.** 62 files in jena-core and jena-arq conflict.
  - Most of them are generated: the parsers, `arq.jj`, `sparql_12.jj`. These are regenerated from
    `main.jj` with JavaCC 7.0.13 and GNU `cpp`, which is installed.
  - About 55 hand-written files remain, with about 100 conflicting blocks. Many are one-method
    additions to visitor interfaces, where both sides keep their method.
  - The heavier ones are `ItemLift` (7), `SolverRX3` (5), `OpVars` (5), `SolverLib` (4) and `main.jj`
    (6).
- **Moved files.** Seven files that jena-array changes were moved or removed in 6.x, and their changes
  must be re-applied where the code now lives:
  - `ResultSetCompare`, `PatternVarsVisitor` and `RowSetReaderJSON_V1`;
  - `FmtEltLib`, which jena-array deletes and 6.x changed;
  - two test classes;
  - `Grammar/sparql_12.txt`.

  Rename detection carried the rest, `IsoAlg` among them.

**The layers the fork touched** (its `RDF-ARRAY-HANDOFF.md`):

- core: `Node_Seq`, `Node_Undef`, `NodeFactory`, `NodeVisitor.visitSeq` / `visitUndef`;
- RIOT parsers and writers;
- the SPARQL grammar and its regenerated parsers;
- the solver (`SolverRX3`, `SolverRX4`, `SolverLib`);
- `UNNEST` (`ElementUnnest`, `OpUnnest`, `QueryIterUnnest`), `Node_SeqGap` and the `E_Seq*` functions;
- `Substitute`, `NodeTransformLib`, SSE and `NodeCmp`;
- `IsoAlg` and `GraphMatcher`;
- the JSON and XML RowSet writers and readers, and `ResultSetCompare`;
- `VarFinder` and `OpVars.fixedVars` (slots make variables optional).

**Where the conflicts will be:** in `main.jj` and the generated parsers, since 6.x added `<<(`, reifiers,
annotations and FOLD/UNFOLD; and in the solver and the RowSet writers, which 6.x reworked for RDF 1.2.

**Three additions BeakGraph wants in the fork:**

1. **A lazy constructor for `Node_Seq`.** Today it copies its elements (`List.copyOf`). A factory method
   that trusts an immutable `RandomAccess` list lets BeakGraph hand Jena a sequence that decodes on demand
   (§3.6). It is the cheapest large win in the plan.
2. **A CSV writer that spells a sequence out**, as rdf-array.md's results table says, rather than `?`.
   Optional.
3. **Nothing else.** ShEx-array and SHACL-array come along with the reactor, and BeakGraph does not use them.

**A facade on BeakGraph's side.** All sequence access goes through one class, `core/lib/SeqTerms`
(`isSeq`, `elements`, `create`, `UNDEF`, `isUndef`). Upstream renames, or a later adoption in stock Jena,
then touch one file. Storage work (step 3) can start before step 2 finishes, against a test-only `SeqTerms`
built on Jena 6.2's `Node_Ext`. That stand-in needs `isConcrete()` overridden, because the default is
`false`, and about ten BeakGraph sites read a non-concrete node as a wildcard. It also needs structural
`equals`/`hashCode`, because every dedup set and the disk merge use `equals`.

### 3.2 The term model and the ingest guards (D2)

- **Value semantics, as jena-array and the contrib suites have today.**
  - One dictionary term per distinct sequence.
  - Duplicate quads collapse (§9.7).
  - The occurrence model (§1, item 3) is deferred to step 10, and the format leaves room for it: a count
    column beside the last index level.
- **New §9.5 guards**, each refused loudly:
  - **A sequence as a subject, predicate or graph name.**
    - This follows the 2026-11-03 data model.
    - jena-array accepts subject sequences, so contrib tests that use them are **flagged with the reason,
      never silently dropped** (the rdf-array organisation's convention).
  - **A sequence inside a triple term.**
    - jena-array and N3.js-array reject one, and rdf-array lists the question as open (#5).
    - The storage already supports one: a triple term's object component is an object-space id. Lifting
      the guard later costs a guard.
  - **`Node.UNDEF` anywhere but as an element.**
  - **Nesting deeper than a fixed bound (1,000).**
    - Extract, compare and verify may then recurse safely.
    - Input to a build is local, but a stack overflow in a parallel build is a poor error.
- **Normalization recurses into sequences** as it does into triple terms: relative IRIs (§9.2), numeric
  canonicalization (§9.3, so `( "042"^^xsd:int )` is stored as `( 42 )`), and blank-node scoping per
  document (§9.4).
  - Today's helpers recurse only into triple terms: `TripleTerms.walk`/`map`, `RelativeIris`,
    `RelativeIRIResolver`, `alignBnodes`, `HugeBuildPipeline.scopeNode`.
  - Generalize them into one compound-term walker that covers both kinds and either nesting inside the
    other.
- **Blank nodes inside a sequence are entity ids**, as inside a triple term. Co-reference survives the
  rank-derived labels, and nothing needs rejecting. This is unlike `cdt:List`, which must reject them.
- **Unchanged:**
  - `cdt:List` / `cdt:Map` stay lexical literals.
  - Ladders (`rdf:first`/`rdf:rest` triples) stay ordinary triples.
  - JSON-LD `@list` still reads as a ladder, because jena-array does not map it.
  - Conversions are optional extras (step 10).

### 3.3 The order (D3)

§6.1 gains a fifth macro kind: blank node (1) < IRI (2) < literal (3) < triple term (4) < **sequence (5)**.
Two sequences compare element by element with the whole §6 comparator, recursing as triple terms do. A
**slot sorts below every term**, and when one sequence is a proper prefix of the other, the shorter comes
first. `TermKey` mirrors it with a kind constant, as it does `TRIPLE_TERM = 4`.

Consequences:

- **Sequences are the literals section's contiguous suffix**, and triple terms become a band just before
  them.
  - The reader's suffix arithmetic becomes a `[first, last]` band: `firstTripleTermObjectId`,
    `checkTripleTermSuffix`, and the minimum-only clamps in `BGIteratorPOS`, `BGIteratorSO` and
    `BGIteratorSPO_All`.
  - "Is this object a sequence" becomes one comparison, `id ≥ firstSequenceObjectId`.
- **Stored sequences compare on ids.**
  - The combined object space is sorted, and a slot is stored as 0, below every id.
  - So two stored sequences compare exactly as their element-id arrays compare lexicographically, shorter
    prefix first.
  - `locate` for a sequence first locates the probe's elements, then binary-searches the suffix on id
    arrays, without materializing a candidate.
  - If an element of the probe is not in the dictionary, the sequence is not either, and the answer is a
    fast miss.
- **A constant prefix is a range.** Sequences that begin with the same terms are contiguous, so
  `( 1 2 ... )` is an object-id range that `GPOS` can narrow with `lowerBound`/`upperBound`, as range
  filters already do.
- **Id order is rdf-array's `ORDER BY` order** for sequences, which the open "ORDER BY / DISTINCT on ids"
  candidates in `PERFORMANCE.md` can rely on.
  - Within the literal elements, §6 is BeakGraph's order and not ARQ's.
  - So the existing caveats about range narrowing (§10.4) apply to sequences whose elements are literals.
- **Rejected:** a rank between literals and triple terms.
  - It would keep the "triple terms are last" code.
  - It would also make id order disagree with rdf-array's order, and lose the single suffix test.

### 3.4 Canonical storage, format v7 (D4)

```
.BG/dictionary/literals/
  datatypes            ... 14 (SEQUENCE) on every sequence row       (the section's last rows, §3.3)
  offsets              ... k, the row's 0-based ordinal among the section's sequences
  typedLiterals, langTags, langDirs                     0 on sequence rows (parallel-array invariant)
  tripleTerms          unchanged (stride 3)
  sequenceStarts       block-structured (§5.1a), numSequences + 1 entries, non-decreasing, first 0:
                       the elements of sequence k are sequenceElements[starts[k], starts[k+1])
  sequenceStarts_blocks
  sequenceElements     block-structured (§5.1a): object-space ids; 0 is a slot
     @numSlots         int64: how many elements are slots (0 when the store holds none)
  sequenceElements_blocks
  sequenceArrays       block-structured, numSequences entries: the class of §3.5, or 0     (only with -arrays)
  sequenceArrays_blocks
```

- **`DataType.SEQUENCE = 14`** is the last ordinal that keeps the column at 5 bits. A slot needs no ordinal,
  because it is never a row.
- **Ordinals and lengths.** `offsets = k` mirrors triple terms. A sequence's length is
  `starts[k+1] − starts[k]`, and element `j` is one block-structured read.
- **The starts compress well.** `sequenceStarts` is monotone, so §5.1a's per-block base and width cost each
  entry only the bits of its block's spread.
- **Nested sequences are terms of their own.** They are rows of the same suffix, referenced by id, so an
  inner sequence that occurs many times (a recurring `( x y )` point) is stored once.
  - Like a triple term's components, inner sequences, and terms that appear only inside a sequence, get
    dictionary entries but do not join the `graphs`/`subjects`/`objects` role lists.
  - A sequence that is an object somewhere is in `objects`, so `SELECT DISTINCT ?o` and the per-predicate
    fast paths need nothing new.
- **The quad indexes do not change.** A sequence is an ordinary object id at `GSPO`'s `So` level and
  `GPOS`'s `Sp`→`So` level.
- **Edge cases.**
  - If every sequence is `()`, `sequenceElements` is not written (§5.1a writes no empty sequence), while
    `sequenceStarts` is: its blocks have width 0, so it is the 8-byte tail alone.
  - A store without sequences writes none of these datasets. It is shape-identical to v6, though stamped 7.
- **Container parity holds byte for byte**, as for the rest of `.BG`.
- **Rejected alternatives:**
  - **Turtle text in the `strings` FCD.** It cannot be matched structurally, a probe needs a re-parse, and
    blank-node labels would sever co-reference, which is the reason `cdt:` literals reject blank nodes.
  - **Ladders.** They are what rdf-array removes.
  - **Flattening the nesting into multi-level offsets inside the canonical store.** Inner sequences would
    stop being addressable terms, and hash-consing would be lost. The native view of §3.5 flattens,
    because there it pays.

Appendix A works one small store through, row by row.

### 3.5 Native arrays: `/.BG/arrays` (D5)

**Purpose.**

- A store's numeric sequences can be read as **typed arrays by tools that know nothing of RDF**: h5py,
  zarr-python, numpy, xarray, napari.
- Examples: embeddings and feature vectors, a roster's seven daily hours, matrices, polygons as rows of
  `( x y )`, and (§3.5a) lists of mixed tuples such as FHIR's `( code name ( value unit ) )`.
- BeakGraph itself reads element values from these arrays instead of decoding a dictionary literal per
  element.

In v7 the arrays are a **second copy**: the canonical store of §3.4 stays complete, readers may ignore
`/.BG/arrays`, and the subtree is optional (`-arrays`, off by default). Step 9 decides whether packed
sequences drop the first copy.

**Classification.** It depends only on the store's set of sequences, never on input order or on the
engine, so all six engines agree.

- Only **top-level** sequences, the members of `objects`, are classified. An inner sequence belongs to the
  array of the sequence that holds it.
- Every leaf must be a literal of **one packable datatype**, or a slot. Slots may occur only at the leaf
  level: a slot where a row should be leaves the sequence unpacked.
- Packing must be lossless:

  | Datatype | Array type | Packs when |
  |---|---|---|
  | `xsd:double` | float64 | always (§9.3 already stores the value) |
  | `xsd:float` | float32 | always |
  | `xsd:long` | int64 | always |
  | `xsd:int` | int32 | always |
  | `xsd:integer` | int64 | the lexical form is canonical and the value fits in 64 bits (Turtle's `( 1 2 3 )` is this) |
  | `xsd:boolean` | bool | the lexical form is `true` or `false` |
  | `xsd:decimal` | not in v7 | later: scaled int64 with a CF `scale_factor` (step 10). Turtle's `0.5` is a decimal, so decimal vectors in Turtle do not pack yet |

- **The key.** The levels below the outermost must be rectangular. The key is then `(type, inner shape)`:
  `( 1 2 3 )` is `(i64, [])`, the matrix `( ( 1 2 3 ) ( 4 5 6 ) )` is `(i64, [3])`, and a polygon of
  `( x y )` points is `(f64, [2])`. A sequence with no key here may still be a record class (§3.5a);
  otherwise it is not packed.
- **The class.** Whether a row goes to a tensor or a ragged class depends on the other sequences with its
  key, so it is decided once every sequence is known:
  - *Tensor class*: an outer length shared by at least `-arrays-min-rows` sequences of one key (default 16)
    gets a class of its own, with `values` of shape [rows, length, inner…]: an embedding store's
    `f64_768`, a roster's `i64_7`.
  - *Ragged class*: the key's other rows share one class, with `starts`.
  - A disk engine spills rows per key and length while it encodes the suffix. It then writes each tensor
    class from its spill, and merges a key's remaining lengths into its ragged class by id.
- **Minimum size.** At least `-arrays-min` leaves, default 2.
- **Names and numbering.** A tensor class is named by its type and row shape (`f64_768`, `i64_7`,
  `i64_2x3`), a ragged class by its type and inner shape (`f64_r`, `f64_r_2`). Classes are numbered 1, 2,
  … in name order, and `sequenceArrays` gives each sequence its class number, 0 when it is not packed.

**Layout.** It is the same logical layout in both containers:

```
.BG/arrays/
  <class>/                 attributes: kind ("tensor" | "ragged"), datatype (the XSD IRI), shape
    values                 the array type, little-endian
                           tensor: [rows, d1, …, dk]        ragged: [items, inner…]
    starts                 int64 [rows + 1]                 ragged only: row r is values[starts[r], starts[r+1])
    mask                   bool, values' shape, true at a slot; only if the class holds a slot (values there are 0)
    seq                    int64 [rows]: each row's sequence object id, ascending
```

- **Rows and lookup.** Rows are in id order. A sequence finds its row by a binary search of its class's
  `seq`, so no row column is needed.
- **`seq` is the key back to the graph.** It is what a `GPOS` / `GSPO` read joins to.
- **Slots get a mask, not NaN.** NaN cannot mark a slot, because `"NaN"^^xsd:double` is a value. The mask
  is what `numpy.ma` and xarray expect.
- **Endianness.** The arrays are little-endian, because that is what numpy users get natively. These are
  the only datasets in a store whose element type a reader honours. They are the documented exception to
  "big-endian everywhere", and the container's metadata declares the order.

**Each container its own way:**

| | HDF5 (`.h5`) | Zarr (`.zarr`, `.zarr.zip`) |
|---|---|---|
| Element type | `H5T_IEEE_F64LE`, `H5T_STD_I64LE`, …; bool as the enum `{FALSE, TRUE}` over int8, which h5py reads as numpy `bool` | `data_type` `float64`, `int64`, …, `bool` |
| Default layout | **contiguous, unfiltered**: BeakGraph maps it (`storageSegment`) and reads it over HTTP by ranges, exactly as it reads `.BG` today | codecs `[bytes{endian: little}]`, chunked by whole rows, at most 2^30 bytes a chunk: mapped through `chunkSegment` |
| `-arrays-compress` | chunked by rows, **shuffle + deflate**: the filters stock h5py reads without hdf5plugin | **`sharding_indexed`**: inner chunks of rows, codecs `[bytes, zstd]`, index `[bytes, crc32c]`. Few files, and a remote read fetches the shard index and the inner chunks it needs |
| Labels | attributes only: Falcon cannot write dimension scales, so xarray needs h5netcdf's `phony_dims`. Writing the H5DS attributes is a Falcon addition (vlen of object references, under the next -SNAPSHOT), optional | `dimension_names`: `["seq", "d1", …]` on `values` and `mask`, `["seq"]` on `seq`, which xarray reads as the arrays' dimensions |
| Attributes | string attributes (the 64 KiB cap does not matter here) | JSON |
| Consolidated metadata | n/a | the arrays are consolidated with the rest |

- **Parity for this subtree is by value.** Both containers hold the same groups, shapes, types, values
  and masks; their bytes differ by chunking and codec. The container-parity test compares decoded arrays
  here and bytes everywhere else.
- **Reading from Python** (an informative example; `week` is the roster example built with
  `-arrays -arrays-min-rows 1`, since it has only a few workers):

  ```python
  import numpy as np, h5py, zarr
  with h5py.File("week.h5") as f:                       # HDF5
      c = f["/.BG/arrays/i64_7"]
      hours = np.ma.masked_array(c["values"][()], mask=c["mask"][()])
  c = zarr.open_group("week.zarr", mode="r")[".BG/arrays/i64_7"]        # Zarr
  hours = np.ma.masked_array(c["values"][:], mask=c["mask"][:])
  ```

**The store abstraction grows, in both containers:**

- **Writing.**
  - A new method on `WritableGroup`, `createTypedArray(name, type, shape, layout, dimensionNames)`,
    returns a writer that takes row boxes.
  - Attributes may now be strings and JSON as well as int and long.
  - `ZarrStoreWriter` writes typed arrays through Falcon's `ZarrArray`, since its own chunk path is
    uint8-only, and generalizes `commit()`'s per-array byte-length check. `pack()` already zips files it did
    not write.
  - Contiguous HDF5 datasets need their size at creation. Builders therefore spill each class's rows to a
    scratch file while encoding and write the dataset once its row count is known.
- **Reading.**
  - A new `StoreGroup.typedArray(name)` returns the shape and type, a mapped segment when the array is raw,
    and Falcon `select` reads when it is not.
  - `array()` stays strict.
  - `/.BG/arrays` loads lazily, so a store opens without touching it.
  - Remote compressed Zarr goes through Falcon's `HttpStore`, which lacks BeakGraph's ETag check. Back it
    with BeakGraph's `StoreRanges`, or serve compressed arrays locally only.

**Rejected:**

- **HDF5 vlen datasets for the ragged classes.** The payload is uncompressed in the global heap and cannot
  be mapped or range-read cheaply, and Zarr has no counterpart. They might make an `-export` format
  someday.
- **Compound or struct rows.** Falcon writes a Zarr struct as packed bytes, which array tools see as
  opaque, and a row of mixed types compresses worse than a column of one. Record classes (§3.5a) use one
  array per position instead.
- **Region references** from rows to datasets. Zarr has nothing like them, and BeakGraph does not need
  them.
- **vlen-bytes.** It is opaque to array tools.

### 3.5a Record classes: strings, dates and mixed types (D5)

A tensor or ragged class needs every leaf to share one numeric type. Much real data mixes types, but **by
position**: FHIR's components are `( code name ( value unit ) )`, strings and then a number and a string,
and a reading pairs a time with a value. Such a sequence becomes a *record class*: one typed array per
position, like the columns of a table. Each column holds one type, so array tools read it natively and
pandas makes a DataFrame of it. Like the rest of `/.BG/arrays`, record classes are a second copy in v7:
the canonical elements of §3.4 stay.

**Leaf types.** A column holds one of these. Any other leaf (a blank node, a triple term, an `xsd:decimal`,
…) leaves the sequence unpacked.

| Leaf | Terms | Column | Packs when |
|---|---|---|---|
| `f64` `f32` `i64` `i32` `bool` | as in §3.5 | as in §3.5 | as in §3.5 |
| `str` | `xsd:string` | variable-length UTF-8 | always |
| `lang` | `rdf:langString` | variable-length UTF-8, with a `_lang` column of uint16 codes into the column's `langs` attribute | it has no base direction (`rdf:dirLangString` stays out) |
| `iri` | IRIs | variable-length UTF-8, the stored form (§7.7): a relative reference stays relative | always |
| `dt`, `dtZ`, `dt+hh:mm` | `xsd:dateTime` with no timezone, `Z`, or that offset | int64, counted in the store's time unit from 1970-01-01T00:00:00, with CF `units` and `calendar: "proleptic_gregorian"`; a zoned value is stored as its UTC instant, and the column's `timezone` attribute names the zone | the lexical form is exactly what the packed value rebuilds to (XSD's canonical form: no trailing fractional zeros, `Z` for a zero offset), its fraction fits the time unit, and its count fits int64 |
| `date` | `xsd:date` with no timezone | int32 days since 1970-01-01, CF `units: "days since 1970-01-01"` | the lexical form is canonical |

- **One time unit per store.** `-arrays-time-unit` is `s`, `ms`, `us` or `ns`, default `us` (±292,000 years
  in int64), so every `dt` column counts the same unit. A value finer than the unit is not packable.
- **Timezones are part of the type.** `"…+05:00"` and `"…Z"` may be the same instant, but they are
  different RDF terms. A column therefore holds one zone, and sequences that differ in it are different
  classes.

**Signatures.** A record class's key is its signature, the sequence's type structure:

```
signature := leaf | leaf "[" dims "]" | "(" signature ( "," signature )* ")" | "list(" signature ")"
```

- **Lists and tuples.** At the outermost level, a sequence whose elements all have one signature `T` is
  `list(T)`, a variable number of items. Any other sequence, and every inner one, is a tuple of its
  elements' signatures:
  - `( "a" 1 )` is `(str,i64)`;
  - `( "a" "b" "c" )` is `list(str)`;
  - `:obs1`'s components are `list((str,str,(i64,str)))`.

  A list below the outermost level is read as a tuple, so inner variable-length lists give one class per
  length, and the minimum below prunes the rare ones.
- **Numeric blocks.** Inside a tuple, a rectangular block of one numeric or boolean type is one N-D column,
  not a column per number: `( "name" ( 768 doubles ) )` is `(str,f64[768])`.
- **Precedence.** A sequence that a tensor or ragged class takes (§3.5) is not a record class.
- **Minimum rows.** The signature is a function of the sequence alone. A class is written only when at
  least `-arrays-min-rows` sequences share its signature, and the others are not packed. That keeps a store
  of varied tuples from making a class per shape.
- **Names.** Classes are named `rec_1`, `rec_2`, … in the order of their signatures' spellings (as in the
  grammar, without spaces), and each group's `signature` attribute holds the spelling. `sequenceArrays`
  numbers them with the other classes, in name order.

**Layout.**

```
.BG/arrays/rec_<n>/        attributes: kind "record", signature
  starts         int64 [rows + 1]       list(…) classes only: row r's items are [starts[r], starts[r+1])
  f<i>[_<j>…]    one array per leaf position: [rows] for a tuple class, [items] for a list(…) class,
                 with the block's dims appended for a numeric block (f2 of (str,f64[768]) is [rows, 768])
                 attributes: datatype (the XSD IRI), kind (the leaf); units, calendar, timezone for dates
  f<…>_lang      uint16 codes, lang columns only
  f<…>_mask      bool, true at a slot; only for a column that holds one
  seq            int64 [rows]: each row's sequence object id, ascending
```

- **Column names** are the position path from 1: `f3_1` is the first element of the third position. In a
  `list(…)` class the path is within the item.
- **Slots.** A slot may stand only at a leaf, as in §3.5. The column holds 0 there, or `""` in a string
  column, and its `_mask` is true there.

**Each container its own way**, beyond §3.5's table:

| | HDF5 (`.h5`) | Zarr (`.zarr`, `.zarr.zip`) |
|---|---|---|
| String columns | variable-length UTF-8 string datasets, which Falcon writes chunked and filtered too. The strings sit in the global heap, which filters do not compress. h5py returns bytes unless a column is read through `.asstr()` | `data_type: "string"` with the `vlen-utf8` codec. Under `-arrays-compress`, zstd follows it in the chain and does compress the strings |
| Date columns | int64 and int32 datasets with CF attributes; xarray (h5netcdf, `phony_dims`) decodes them to `datetime64` | int64 and int32 arrays with CF attributes; xarray decodes them to `datetime64` |
| BeakGraph's own reads | numeric, boolean and date columns, mapped when raw, as in §3.5. It never reads a string column: its dictionary already holds the strings | the same |

**Reading one from Python** (informative; `obs` is Appendix A.2's data):

```python
import numpy as np, pandas as pd, zarr
c = zarr.open_group("obs.zarr", mode="r")[".BG/arrays/rec_1"]
items = pd.DataFrame({k: c[k][:] for k in ("f1", "f2", "f3_1", "f3_2")})
items["seq"] = np.repeat(c["seq"][:], np.diff(c["starts"][:]))     # each item's sequence id
```

**Not in v7.** These are in step 10:

- string columns as codes into a table of distinct values;
- a fallback for sequences no class takes;
- more temporal types;
- `rdf:dirLangString`.

Mixed numeric types such as `( 1 2.5 3e0 )` are never converted to one type: the decimal `0.1` is not a
double, and the three are different terms.

### 3.6 Reading: the lazy sequence (D6)

- **`extract`'s new case.**
  - A `SEQUENCE` row becomes a `Node_Seq` through the fork's lazy constructor (§3.1).
  - Its list is `RandomAccess`. `size()` comes from `sequenceStarts`, and `get(i)` reads one element:
    - a nonzero element id goes through `extract` (an inner sequence comes back lazy too);
    - 0 becomes `Node.UNDEF`;
    - with `-arrays`, a packed sequence reads numbers, booleans and dates straight from its arrays.
      Strings and IRIs still come from the dictionary; string columns exist for other tools.
  - Jena's own `SEQLEN`, `s[i]`, slices, `IN` and `QueryIterUnnest` then decode only what they read.
    `equals` and `hashCode` still decode everything; they are rare on this path.
- **Caches.** `SimpleNodeTable.weightOf` and the `searchCache` weigher weigh a sequence by its length. A
  lazy node is small; the elements it decoded are cached as terms of their own.
- **Locate.** It runs on ids (§3.3). `ProbeComparator`'s literal-only shortcut is not involved.
- **`-verify -deep` gains these checks:**
  - `sequenceStarts` is non-decreasing and ends at `numEntries` of `sequenceElements`;
  - every element is 0 or a valid object id;
  - `numSlots` matches the count of zeros;
  - a nested sequence's id lies in the suffix, and no sequence holds itself at any depth (checked with the
    depth bound);
  - the suffix is strictly ascending on id arrays;
  - with `-arrays`, every packed row, record rows included, rebuilds exactly its canonical elements.
- **`-export`.**
  - N-Triples and N-Quads go through the fork's `NodeFormatter`, as `( … )` and `undef`.
  - JSON-LD and RDF/XML refuse sequences, as they refuse triple terms today (`BeakGraphCLI`:612,
    `BGSparqlService.containsTripleTerms`). jena-array has no spelling for them.
  - `relativeReference` must recurse into compound terms. It already misses relative IRIs inside triple
    terms, an existing gap that step 3 fixes too.

### 3.7 The query engine (D7)

#### Layer A: correct everywhere, through jena-array (step 4)

- **Delegate sequence patterns.**
  - A basic graph pattern that holds a **non-concrete** sequence (a variable, a gap or a nested pattern
    inside it) goes whole to the original stage generator: jena-array's solver over
    `HDF5Reader.graphBaseFind`.
  - A **concrete** sequence is a constant. It is located on ids (§3.3), so `?s :p ( 1 2 3 )` stays on
    BeakGraph's id path from day one.
- **Guards.**
  - `FilterBounds.orderAgreesWithArq` must not narrow a range around a sequence constant: §6's literal
    order is not ARQ's, as §10.4 notes.
  - JSON-LD and RDF/XML outputs refuse sequences.
  - `ResultCache.walk` gains `visit(OpUnnest)`, so a `RAND()` or `NOW()` inside `UNNEST` marks the query
    unstable.
  - The variables of union dedup keys (`HDF5Reader.collectTripleTermVars`) include those inside sequence
    patterns. Otherwise distinct rows would collapse.
- **Slot-aware variable analysis.** This is the subtle part.
  - Standard SPARQL binds every variable of a matched pattern, and BeakGraph's own analyses assume it.
    With slots, a variable inside a sequence pattern, and the element variable of `UNNEST`, may stay
    unbound.
  - jena-array changed `VarFinder` and `OpVars.fixedVars` for exactly this. BeakGraph's own analyses need
    the same: `BGReorderTransform.addObjectVars`, `MinusJoin.boundVars`, `PathMerge.addVars`, the filter
    placement in `OpExecutorBG.optimizeExecuteTriples`, the `GraphVarBG` plans and `OptionalHashJoin`.
    `MinusJoin`'s rewrite is only valid over variables that are certain to be bound.
  - All of it is keyed on **`numSlots > 0`**. A store without slots keeps every fast path unchanged.
- **One `SlotPolicy` class** holds rdf-undef §6's five open decisions: unbound at a slot, joining through a
  slot, a solution per slot, a template writing a slot, and a hole rather than an unknown. If the strawman
  flips one, one class changes.

#### Layer B: on ids, where it pays (step 7)

- **`SequenceMatcher`.** Generalize `TripleTermMatcher` into a compound-term matcher.
  - **Compile.** The pattern's constant elements are located once. Variables, nested sequence and
    triple-term patterns, `undef`, gaps and named gaps compile into a small program.
  - **Constant elements and "=".** The SPARQL-array redline matches a constant element with `=` for
    literals, not `sameTerm`, while jena-array's ground lookups are by term. Settle it against
    rdf-tests-contrib.
    - If `=` stands, a constant element matches its whole value-equal cluster, which §6.4 keeps
      contiguous (`ValueCluster`).
    - The common case, a singleton cluster, is still one id.
    - A whole constant sequence then matches the id range from the sequence of its clusters' minima to the
      sequence of their maxima, filtered element by element.
- **Access paths.**
  - Object runs are clamped to the sequence suffix, and further to a constant prefix's range.
  - The length is checked from `sequenceStarts` before any element is read.
  - With `p` bound, this is one `GPOS` probe and a range of the run.
- **Unification on ids.**
  - An element id binds a variable, and 0 leaves it unbound.
  - A constant and `undef` follow §1, item 8.
  - A repeated variable needs equal ids, which means the same term under value semantics.
- **Gaps.**
  - They are matched by backtracking over run lengths, as `SolverRX3.matchMembers` does.
  - The multiplicity is the number of placements, so `( ... ?x ... )` over `( 7 7 )` gives `?x = 7`
    twice.
  - A single leading or trailing gap is closed-form.
- **Values without ids.** A named gap's run, an `UNNEST` position and (from step 9) a packed element have
  no dictionary id.
  - `BindingNodeId` gains a node-valued slot beside its id slots.
  - A join on such a variable compares terms, and joins on ids are untouched.
- **No substitution into sequence patterns.** A bind join must not write a bound value into a sequence
  pattern, because a bound variable is compatible with a slot where a written constant is not
  (rdf-undef §3). It checks compatibility after matching, as Comunica's `!BOUND(?b) || sameTerm(?b, v)`
  does.
- **`UNNEST` on ids.**
  - `OpExecutorBG` overrides `execute(OpUnnest)` when the expression is a variable bound to a stored
    sequence id.
  - Element ids are read straight from `sequenceElements`.
  - `?i` is node-valued, and a slot leaves the element variable unbound.
  - Anything else falls back to Jena.
- **Other id paths.**
  - `isSEQ` is the suffix test, and `SEQLEN` is a difference of starts, both computed in `RowValue` without
    decoding.
  - Under value semantics, id equality is term equality, so `DISTINCT` and `GROUP BY` on sequence ids are
    exact.
  - Id order is the `ORDER BY` order (§3.3).

### 3.8 Versioning (D8)

| Version | Adds | A reader of the previous version |
|---|---|---|
| **v7** | `DataType.SEQUENCE`, `sequenceStarts`, `sequenceElements`, `numSlots`; optional `/.BG/arrays` and `sequenceArrays` (presence-sniffed: a v7 reader may ignore them) | refuses the store by version (`HDF5Reader`:127), as it must, since an unknown ordinal fails at `MultiTypeDictionaryReader`:250 |
| **v8** (step 9) | packed sequences may omit their canonical elements; the arrays become required to read them | refuses by version |

`Params.FORMAT_VERSION` becomes 7. A store without sequences is shape-identical to v6, though stamped 7,
which is the rule triple terms set in v5.

---

## 4. The plan, step by step

### Step 1: spec first, decisions, facade (P0, S)

**Why:** BeakGraph's format is defined by `SPECIFICATIONS.md`, and every engine is checked against it.

**Work:**

- Draft v7 in `SPECIFICATIONS.md`:
  - §4.1, a v7 row;
  - §6.1, macro rank 5 and the sequence and slot rules;
  - §7.1, the literals suffix (triple terms become a band);
  - §7.3, a routing-table row; §7.4, ordinal 14; §7.7, extract;
  - a new §7.6b, Sequences;
  - §9.3 and §9.5, recursion and guards;
  - §3 and §3a, the `/.BG/arrays` exception, once step 6 lands;
  - Appendix A.
- Update `docs/architecture/` (dictionary sections, file layout, query path).
- **Moved to step 3:** `DataType.SEQUENCE` and `core/lib/SeqTerms`.
  - `SeqTerms` needs the fork's `Node_Seq`.
  - Adding the ordinal before the encoder and reader handle it would let a reader accept a row it cannot
    extract.
- **Decisions, taken 2026-11-24:**
  - value semantics now and occurrences later (D2);
  - refusing subject sequences and sequences inside triple terms;
  - the version names: `6.3.0-array-SNAPSHOT`, and format 7;
  - `-arrays` off by default, with `-arrays-min-rows 16` and a time unit of microseconds.
- **Still open:** whether the port is offered back to the rdf-array organisation. That waits until it
  works.

**Status (2026-11-24):** the v7 draft is in `SPECIFICATIONS.md`: §4.1, §6.1, §7.1–§7.4, §7.6b, §7.7,
§9.5 and Appendix C, each passage marked *v7 draft*. The `/.BG/arrays` profile text waits for step 6, and
`docs/architecture/` for step 3.

**Done when:** the spec text is reviewed.

### Step 2: jena-array on Jena 6.3.0 (P0, L, blocking)

**Why:** Turtle-array, SPARQL-array, `Node_Seq`, `Node_Undef` and the results formats all live in the fork
(§3.1).

**Work:**

- **Done, 2026-11-24.** The port is on Jena's `rdf-array` branch, `7ee350db0b..335ff4d986`. Its notes are
  in that branch's `doc/rdf-array.md`.
  - **The merge** (§3.1): every conflict resolved, the moved files' changes re-applied, and 6.x names
    used throughout.
  - **The grammar**: `main.jj` merged with 6.x's `<<(`, reifiers and `LANG_DIR`; the parsers regenerated
    with JavaCC 7.0.13 (SHA-1 checked), with no choice conflicts.
  - **The solver**: jena-array's `SolverRX3`/`SolverRX4`, with 6.x's `makeAbortable`. TDB1 and TDB2 take
    the matcher's several results.
  - **6.x adaptations**: `CONSTRUCT` through 6.x's `TemplateLib` (blank nodes renewed, unbound
    variables written as slots); `NodeUtils.isValidAsRDF` and the Model API (`StatementImpl.createObject`)
    accept sequences; `G` and `GraphList` read a sequence as a list; the results comparison goes through
    6.x's `BNodeIso`.
  - **`NodeFactory.createSeqNodeView`**, the lazy constructor of §3.1.
  - **The commits**: layer by layer, as Erich Bremer, no co-author line.
- **Done: BeakGraph on the fork.**
  - BeakGraph compiles against the fork unchanged.
  - Its W3C and SPARQL-CDT runners read a manifest's `mf:entries ( … )` as a sequence term, through
    `ManifestLists`.
  - Two W3C RDF 1.2 documents with a collection as subject now check that BeakGraph refuses the sequence
    subject (`src/test/resources/w3c/rdf12/README.md`).
- **Left:**
  - the CSV spelling (optional);
  - the GraalVM native build against the fork;
  - jena-arq's ladder-dependent tests (`TestGList`, `TestList`), to flag with a sequence counterpart each;
  - writers with no sequence spelling (JSON-LD, RDF/XML, Thrift, Protobuf). BeakGraph refuses those
    outputs for sequences anyway (§3.6).

**Done when:**

- the fork's own suites pass on 6.3.0: `TestUndefSlots` (43), `TestSequenceQueries`,
  `TestArraySyntaxExamples`, `TestLangSequences`, `Scripts_RdfArrayContrib` (58) and
  `Scripts_RdfArrayContribSPARQL` (207);
  - **Met, 2026-11-24:** 43, 39, 14 and 14. The contrib suites ran 54 and 204, which are all the entries
    of the copied manifests.
  - jena-core: all 10,181 tests pass. jena-arq: 89 of 12,322 fail, and only in the two known kinds
    (ladder-dependent tests, writers without a sequence spelling); the fork's `doc/rdf-array.md` lists
    them.
- BeakGraph's whole existing test suite and the W3C RDF 1.2 / SPARQL 1.2 suites pass unchanged on the
  forked Jena. Nothing in a sequence-free store may change.
  - **Met, except two RDF 1.2 documents that put a collection in the subject position.** They are now
    sequence subjects, which BeakGraph refuses, and the runner checks that it does.

### Step 3: v7 storage on `-method 0` (P1, M)

**Why:** this proves the format with one engine before it is copied into five.

**Work:**

- Recursion: the compound-term walker (§3.2).
- Guards: in `ProcessQuad`, `registerSequence` beside `registerTripleTerm`.
- Order: `NodeComparator.getMacroType` and the recursion; `TermKey`.
- Writing:
  - `DictionaryNodeEncoder`'s sequence branch and hook;
  - `MultiTypeDictionaryWriter`: the `sequenceStarts` / `sequenceElements` writers, `numSlots`, and the
    `Stats.numSequences` gate;
  - `DictionaryIds`: element ids, with slot 0.
- Reading:
  - `MultiTypeDictionaryReader`: the extract case and the band and suffix checks;
  - `PositionalDictionaryReader`: the sequence band, `isSequenceObjectId`, and locate on ids;
  - cache weights.
- Tools: `-verify -deep` checks, `-export` and `relativeReference`.
- Bump `Params.FORMAT_VERSION` to 7.
- Tests mirror the triple-term ones (Appendix B, testing), and add the slot, empty, nested,
  blank-node-inside and depth-bound fixtures.

**Done when:**

- round trips are isomorphic under the fork's `IsoAlg` (blank nodes inside sequences included);
- `TermKeyTest`'s every-pair-of-kinds test includes sequences;
- shuffled inputs build byte-identical stores;
- HDF5 and Zarr stores are byte-identical;
- `-verify -deep` passes.

**Status (2026-11-24): done.**

- **What landed.**
  - `DataType.SEQUENCE`, `SeqTerms` (the facade) and `CompoundTerms` (one walker for triple terms
    and sequences, which `TripleTerms.map`, `RelativeIris`, `RelativeIRIResolver`, the blank-node
    alignment and the numeric canonicalization now use).
  - The ingest guards and `registerSequence`; the order (macro 5) in `NodeComparator` and `TermKey`.
  - The writers: the encoder's `SequenceHook`, `MultiTypeDictionaryWriter`'s two stores and
    `numSlots`, `DictionaryIds.encodeSequence`.
  - The reader: lazy rows, the band and suffix checks at open, locate on ids, cache weights, and the
    triple-term scans clamped to the band's end.
  - The tools: `-verify -deep` (stores, slot count, depth bound, every row in its band), `-export`
    (the line formats and Turtle/TriG; JSON-LD refuses, and so does the endpoint), and a recursive
    `relativeReference`.
  - `Params.FORMAT_VERSION` = 7, for every engine.
- **Beyond the step.** `-method 2` shares method 0's parse, so it took sequences for a few lines and
  builds byte-identical stores. Step 5 starts from methods 3, 1, 4 and 5, which refuse sequences on a
  guard meanwhile.
- **Tests.**
  - Round trip under the fork's `IsoMatcher`: slots, empty, nested, blank nodes and triple terms
    inside.
  - The stores and `numSlots`; the suffix and the band; locate on ids, hit and miss.
  - Shuffled inputs give the same bytes, and so do methods 0 and 2.
  - HDF5 and Zarr hold the same store (`ContainerParityTest`); `-verify -deep` passes and names a
    wrong `numSlots`.
  - `TermKeyTest`'s every-pair-of-kinds includes sequences, and `NodeComparatorSequenceOrderTest`
    pins §6.1 rule 7.
  - The guards are tested directly (`SequenceIngestGuardTest`): the fork's parsers already refuse a
    sequence inside a triple term and nesting past 500 levels.
  - The W3C RDF 1.2 runner now stores the suites' collection objects as sequences on methods 0 and 2.
  - Nothing else moved: the stress fixture's stores from methods 0, 1 and 3 differ from master's in
    the version attribute and its header checksum alone. The suite: 2,591 tests pass, 92 skipped.
- **Left for step 4.** A pattern holding a sequence with a variable or a gap is a plain miss for now;
  delegating it to jena-array's solver is step 4's first item.

### Step 4: correct queries (P1, M), milestone A

**Why:** after this step BeakGraph answers rdf-array data correctly, if not yet fast.

**Work:** Layer A of §3.7: delegation, guards, slot-aware variable analysis keyed on `numSlots`, and
`SlotPolicy`.

**A conformance harness:**

- It builds a store from each contrib test's data and runs the query through `BGDatasetGraph`.
- It compares results with the fork's `ResultSetCompare`, which knows `seq` and `undef`.
- It runs on `method0`, both containers.
- Tests that need sequence subjects are flagged with the reason.

**Done when:**

- `Scripts_RdfArrayContrib` and `Scripts_RdfArrayContribSPARQL` (rdf-tests-contrib, `rdf-undef` branch),
  `TestUndefSlots` and the array-syntax §4.1 examples pass over BeakGraph stores;
- every existing parity test (`AlgebraShapeParityTest`, `RangePushdownParityTest`, …) is unchanged.

### Step 5: the other five engines (P2, L)

**Work:**

- **`-method 2`:** `ParallelPositionalDictionaryWriter` resolves entities and predicates first when
  sequences exist, as it does for triple terms, because elements may be entities.
- **`-method 3`:** `UltraIngest.buildSets` and `buildStats`, where `countLiteralStats` would fail on a
  sequence; `UltraDictionary` resolves elements by binary search, like components.
- **`-method 1`, `4` and `5`:**
  - `NodeCodec` gains `T_SEQUENCE` (a length, then the elements) and `T_UNDEF`.
  - `HugeRecords.estimateBytes` gets a sequence branch.
  - The interior sorters receive elements.
  - `mergeRange` routes sequences to the literal space.
  - A `HugeSequences` beside `HugeTripleTerms`:
    - lengths are known per term, so prefix sums over the sequence suffix give each element's global index
      once the dictionary is merged;
    - `(element term, global index)` records are spilled, merge-joined with the dictionary, and written
      into `sequenceElements` by index;
    - slots are written as 0 directly, without a spill.
  - `PlaidIngest` follows the disk chain.

**Done when:**

- `ParallelWriterParityTest`, `UltraWriterParityTest`, `HugeWriterParityTest`,
  `HugeUltraWriterParityTest` and `PlaidWriterParityTest` hold with a sequence fixture: byte-identical for
  0, 2 and 3, isomorphic for 1, 4 and 5;
- `TermSpillBudgetTest` covers long sequences;
- the CI engine matrix (`ci.yml`:49-56) also runs the step 4 harness on `method1` and `method3`.

### Step 6: native arrays (P2, L), milestone B

**Why:** the point of keeping these stores in HDF5 and Zarr is that array tools can open them.

**Work:**

- The store abstraction of §3.5, in both containers, and its tests.
- The classifier, with its determinism and losslessness tests: canonical `xsd:integer` only,
  `-0.0`/`NaN` bits kept, slots at leaves only, tensor or ragged decided per key and length.
- Record classes (§3.5a): signatures, the string, IRI and language columns, and the `dateTime` and `date`
  rules, with tests that every packed value rebuilds its exact lexical form.
- The writer pass. Keys and signatures are found while the sequence suffix is encoded, in id order. Rows
  are spilled per key and length (per signature for records), then written with known shapes once the
  classes are decided. `sequenceArrays` is written last, from the decided classes.
- The HDF5 encoding: typed and contiguous; `-arrays-compress` for shuffle + deflate; variable-length UTF-8
  string datasets.
- The Zarr encoding: typed and raw; `-arrays-compress` for sharding + zstd; `dimension_names`; `string`
  arrays with `vlen-utf8`.
- The lazy list of §3.6 reads packed values from the arrays.
- `-verify -deep` compares every packed row with its canonical elements.

**Done when:**

- value-level container parity holds;
- a smoke test reads the roster, matrix and embedding fixtures with h5py and zarr-python and the FHIR
  component fixture into a pandas table, and xarray opens the Zarr classes with their dimension names and
  decodes the date columns to `datetime64`. The test runs only where Python is installed, as an opt-in
  test;
- the engines agree on `/.BG/arrays` (byte-identical for 0, 2 and 3 in one container);
- `-arrays` off leaves stores byte-identical to step 5's.

### Step 7: id-native matching (P3, L)

**Work:** Layer B of §3.7:

- `SequenceMatcher` with prefix ranges;
- the node-valued binding slot;
- no substitution into sequence patterns;
- `UNNEST`, `isSEQ` and `SEQLEN` on ids;
- union dedup keys.

Step 4's delegation stays, for whatever the matcher declines.

**Done when:**

- a parity test runs every step 4 query once delegated and once id-native and gets identical answers;
- benchmarks on synthetic data show the gain. The data: FHIR-style component tuples as in `examples/fhir`,
  rosters with slots, matrices, polygons as point rows, embeddings (N × 768 doubles). Measure **in process,
  on synthetic stores only, never real data through the endpoint**; judge remote reads by requests and
  bytes.

### Step 8: statistics and the element index (P3, M)

**Work:**

- **VoID statistics.** Sequence counts, lengths and slots per predicate feed `BGReorderTransform`'s
  costs; `DistinctNodeCounter.hash` hashes sequences structurally.
- **An optional element index, `-seqindex`.**
  - It answers "which sequences hold element *e*, and where": `( ... :x ... )`, `?x IN ?seq` with `?seq`
    free, and gap patterns with a constant inside.
  - It is a two-level structure in the style of the quad indexes: element id → sequence ordinals →
    positions, as bitmap + block-structured levels with their rank/select directories.
  - It costs one more sort of `(element, sequence, position)` records at build time.

**Done when:** containment queries no longer scan, and reorder picks sequence patterns by cost.

### Step 9: v8, packed numeric sequences stored only as arrays (P4, XL, gated)

**Why:** with millions of distinct doubles (embeddings, for example), the dictionary route stores each
double as a literal row plus an element id, and sorts them all at build time. A native float64 array holds
the value in 8 bytes.

**Work:**

- Packed top-level sequences keep their dictionary row, so their id and order are unchanged, but write no
  canonical elements.
- Their leaves and inner sequences enter the dictionary only if they are used elsewhere.
- `locate` compares packed candidates by value.
- Element bindings are node-valued (step 7's slot).
- Prefix ranges over packed sequences binary-search values.
- Record classes keep their canonical elements, because BeakGraph reads strings from its dictionary.

**The gate:** go only if step 7's benchmarks show the dictionary and elements dominate the store size or
build time for array-heavy data, and keep v7 with `-arrays` otherwise.

### Step 10: later and optional

Each of these waits on the spec or on a need:

- **Occurrence semantics** (rdf-array's 2026-11-03 model), once the spec and jena-array settle it: a
  multiplicity column beside the last index level (absent means all 1), then occurrence identity in joins.
- **Sequences inside triple terms:** lift the guard (rdf-array #5).
- **Ladder folding at ingest (`-foldLists`):** folding cannot be done by one update, and rdf-array wants a
  converter. Start with the in-memory engines.
- **`cdt:List` ↔ sequence conversion.**
- **`xsd:decimal` packing** as scaled int64 with CF `scale_factor`, so that xarray decodes it.
- **More of §3.5a.**
  - String columns as codes into a table of distinct values, a pandas Categorical: smaller where a few
    values repeat, as units and codes do.
  - A fallback for sequences no class takes: a datatype-code column and a lexical-form column, lossless
    but untyped.
  - `xsd:time`, `gYear` and durations.
  - `rdf:dirLangString`.
  - Variable-length lists below the outermost level, as multi-level starts.
- **HDF5 dimension scales:** a Falcon addition (vlen of object references) under the next -SNAPSHOT.
- **Vector extension functions** over tensor rows (dot product, cosine), as BeakGraph functions, not
  rdf-array.
- **A `COLLECT` aggregate**, once array-syntax.md adds one. Today it is an open gap.
- **JSON-LD `@list` → sequence**, if jena-array maps it.

---

## 5. Tests and conformance

| Layer | Tests |
|---|---|
| Comparator | the every-pair-of-kinds and sort-on-keys tests of `TermKeyTest`; shuffle stability over nested sequences and slots; element-id-array order equals term order |
| Dictionary | `DictionaryNodeEncoderTest`, `DictionarySectionTest`, `DataTypeOrdinalTest`, `StatsTest`, `ReaderProfileAndInvariantsTest` (suffix and band), `IngestContractTest` (the new guards) |
| Round trip | `TermFidelityTest`, `RDF12TripleTermTest`'s analogues for sequences (lookup hits and misses, pattern routes GSP / GP / scan, nested patterns, repeated variables, unmatchable components fast, a pattern outside the object position answering empty, named graphs and union dedup) |
| Engines | the five writer-parity tests with a sequence fixture; `TripleTermOnlyObjectsTest`'s analogue (a store whose only literals are sequences); `TermSpillBudgetTest` |
| Containers | byte parity of `.BG` except `/.BG/arrays`; value parity of `/.BG/arrays`, strings and dates included; the opt-in Python smoke test |
| Native arrays | the same classes from shuffled inputs and from every engine; every packed value rebuilds its exact term (canonical integers, `dateTime` zones and fractions, dates); tensor or ragged decided by `-arrays-min-rows`; signatures for lists, tuples and numeric blocks |
| Conformance | rdf-tests-contrib `rdf-undef`: `Scripts_RdfArrayContrib` (58) and `Scripts_RdfArrayContribSPARQL` (207); `TestUndefSlots` (43); array-syntax.md §4.1; the CI engine matrix |
| CLI | `ExportTest` (N-Quads-array both paths; JSON-LD refuses), `VerifyCommandTest` (sequence stores pass `-deep`, corrupt starts and elements fail) |
| Engine | delegated versus id-native parity (step 7); every existing query-parity test unchanged on sequence-free stores |

---

## 6. Risks and open questions

- **A Jena upgrade can move the term order.** It did: Jena 6.3.0 compares strings by code point, and
  `NodeComparator` took parts of its order from Jena.
  - The fix, on `rdfarray` (2026-11-24): `FrozenNodeCmp` keeps 6.2.0's order inside BeakGraph.
    `FilterBounds` gives no range hint where ARQ's new order and the dictionary's disagree.
  - The proof: a stress fixture builds byte-identical stores under 6.2.0 and 6.3.0 (CHANGELOG).
  - The rule: the sequence order of §3.3 is built the same way, owning every comparison it makes.
  - The check for every future bump: rebuild a stress fixture with the old and the new Jena and
    compare the bytes.
- **The fork is a moving target.**
  - `rdf-undef` is a strawman over `rdf-sequences`, and its §6 lists five open decisions.
  - The data model of 2026-11-03 (occurrences, no subject sequences) is not implemented anywhere yet.
  - Mitigations: pin fork commits; keep `SeqTerms` and `SlotPolicy` as the only places that know.
  - The porting cost is front-loaded in step 2, and step 3 can start in parallel.
- **"=" or `sameTerm` for constant elements.** The redline and the reference implementation may differ
  (§3.7). The contrib suites decide, and value clusters make either cheap.
- **Value semantics against the adopted model.**
  - Under occurrences, `SELECT DISTINCT ?l` and the joins in rdf-array.md's A/B/C table answer
    differently.
  - BeakGraph will answer as jena-array does, and says so in its compliance notes, as
    `RDF_1.2-compliance.md` does for RDF 1.2.
- **Slots and BeakGraph's join machinery.**
  - The danger is wrong rows: a `MINUS` rewrite or a placed filter that assumes a variable is bound.
  - It is contained by keying on `numSlots` and by the delegated-versus-native parity test.
- **Dictionary growth for numeric data until step 9.** It is measured in step 7; until then, `-arrays`
  gives array users what they need.
- **Hostile nesting.** The depth bound guards ingest. Extract and compare can be iterative where it
  matters.
  - The fork's parsers stop at 500 levels (`Recursion limit exceeded for nested structures`), below
    the store's 1,000, so only programmatic input reaches BeakGraph's bound.
- **A lazy sequence outlives its store badly.** A stored sequence reads its elements from the store when
  asked (§3.6), so one kept after its store is closed fails on access. Results are written while the
  store is open; a caller that keeps sequences copies them (`SeqTerms.create(new ArrayList<>(…))`).
- **Jena's `NodeValue` initialization deadlock.** Any new code that builds `NodeValue`s on worker threads
  (a `SEQLEN` fast path, array-backed elements) goes through `NodeValues`, which initializes `NodeValue`
  first. Only a fresh-JVM CLI build shows the hang.
- **Remote reads.** Each class adds arrays: in Zarr, chunk files and requests; in HDF5, ranges of the one
  file. Raw mode keeps today's request pattern, and sharding keeps compressed Zarr to a shard index plus
  inner chunks.

---

## Appendix A: worked examples

### A.1 Sequences, slots and a tensor

```turtle
@prefix : <http://ex/> .
:x  :p      ( 1 undef 3 ) .
:y  :p      ( 1 2 3 ) .
:m1 :matrix ( ( 1 2 3 ) ( 4 5 6 ) ) .
```

**Ids.**

- **Entities:** E1 `urn:x-arq:DefaultGraph`, E2 `:m1`, E3 `:x`, E4 `:y`, so `maxEntityId = 4`.
- **Predicates:** P1 `:matrix`, P2 `:p`.
- **Literals section:** the integers come first, then the sequences (§3.3).

| Row | Term | Object id | `datatypes` | `offsets` | Why it sorts here |
|---|---|---|---|---|---|
| L1–L6 | `1` … `6` (`xsd:integer`) | 5–10 | 2 (STRING) | 0–5 | numeric space, by value |
| L7 | `( 1 undef 3 )` | 11 | 14 | 0 | ties `( 1 2 3 )` at 1; the slot is below 2 |
| L8 | `( 1 2 3 )` | 12 | 14 | 1 | |
| L9 | `( 4 5 6 )` | 13 | 14 | 2 | first element 4 > 1; inner only |
| L10 | `( ( 1 2 3 ) ( 4 5 6 ) )` | 14 | 14 | 3 | first element a sequence > the literal 1 |

**The element store.** The element-id arrays ascend exactly as the terms do: `(5 0 7) < (5 6 7) <
(8 9 10) < (12 13)`.

```
sequenceStarts   : 0 3 6 9 11
sequenceElements : 5 0 7 | 5 6 7 | 8 9 10 | 12 13        @numSlots = 1
objects          : … 11 12 14                            (13 is inner only)
```

**Queries.**

- `?s :p ( 1 2 3 )`: locate the elements (5, 6, 7), binary-search the suffix on id arrays to object 12,
  probe `GPOS (E1, P2, 12)`. The answer is `:y`.
- `?s :p ( ?a ?b ?c )`: clamp P2's object run to [11, 14], giving 11 and 12, both of length 3.
  - At 11: `?a` is 1, `?b` is unbound (the slot), `?c` is 3.
  - At 12: all three are bound.
  - This is rdf-undef §3's first row.
- `?s :p ( 1 ... )`: the prefix range is the sequences whose first element is 5, ids [11, 12].

**With `-arrays -arrays-min-rows 1`.**

- **Classes:** the top-level sequences are 11, 12 and 14. In name order, class 1 is `i64_2x3` and class 2
  is `i64_3`.
- **`sequenceArrays`:** `2 2 0 1` (13 is inner only).

```
.BG/arrays/i64_3/values   int64 [2, 3]      [[1, 0, 3], [1, 2, 3]]
.BG/arrays/i64_3/mask     bool  [2, 3]      [[F, T, F], [F, F, F]]
.BG/arrays/i64_3/seq      int64 [2]         [11, 12]
.BG/arrays/i64_2x3/values int64 [1, 2, 3]   [[[1, 2, 3], [4, 5, 6]]]
.BG/arrays/i64_2x3/seq    int64 [1]         [14]
```

**One array in each container.**

- **HDF5:** `/.BG/arrays/i64_3/values` is `H5T_STD_I64LE`, dataspace `[2, 3]`, contiguous, with the string
  attribute `datatype = http://www.w3.org/2001/XMLSchema#integer` on its group.
- **Zarr:**

  ```json
  { "zarr_format": 3, "node_type": "array", "shape": [2, 3], "data_type": "int64",
    "chunk_grid": {"name": "regular", "configuration": {"chunk_shape": [2, 3]}},
    "chunk_key_encoding": {"name": "default", "configuration": {"separator": "/"}},
    "codecs": [{"name": "bytes", "configuration": {"endian": "little"}}],
    "fill_value": 0, "dimension_names": ["seq", "d1"] }
  ```

### A.2 A record class

These are the components of array-syntax.md §4, built with `-arrays -arrays-min-rows 1`:

```turtle
:obs1 :component ( ( "8480-6" "systolic" ( 120 "mm[Hg]" ) ) ( "8462-4" "diastolic" ( 80 "mm[Hg]" ) ) ) .
:obs2 :component ( ( "8480-6" "systolic" ( 150 "mm[Hg]" ) ) ( "8462-4" "diastolic" ( 95 "mm[Hg]" ) ) ) .
:obs3 :component ( ( "8480-6" "systolic" ( "high" "mm[Hg]" ) ) ) .
```

- **Signatures.** `:obs1`'s and `:obs2`'s lists are `list((str,str,(i64,str)))`, and `:obs3`'s is
  `list((str,str,(str,str)))`. `i64` spells before `str`, so the first is `rec_1`.
- **Row order.** Rows are in id order: `:obs1`'s list sorts before `:obs2`'s, since 120 < 150 at the
  first difference.

```
.BG/arrays/rec_1/     signature "list((str,str,(i64,str)))"
  starts  int64  [3]   0, 2, 4
  f1      string [4]   "8480-6", "8462-4", "8480-6", "8462-4"
  f2      string [4]   "systolic", "diastolic", "systolic", "diastolic"
  f3_1    int64  [4]   120, 80, 150, 95
  f3_2    string [4]   "mm[Hg]", "mm[Hg]", "mm[Hg]", "mm[Hg]"
  seq     int64  [2]   the object ids of :obs1's and :obs2's lists
.BG/arrays/rec_2/     signature "list((str,str,(str,str)))"      :obs3's list, one row
```

**The columns in each container.**

- `f1`, `f2` and `f3_2` are variable-length UTF-8 string datasets in HDF5, and `string` arrays with
  `vlen-utf8` in Zarr.
- `f3_1` is int64 in both, with `datatype = http://www.w3.org/2001/XMLSchema#integer`.
- Without `-arrays-min-rows 1`, `rec_2` has too few rows to be written, so `:obs3` stays in the canonical
  store only.

---

## Appendix B: where the code changes

Line numbers are as of `6d58aa2`. The touch list for a new compound kind, by area:

| Area | Places |
|---|---|
| Ordinal and counters | `core/lib/DataType`; `core/lib/Stats` (`numSequences`); `hdf5/writers/ultra/UltraIngest.mergeLiteralStats` |
| Recursion helpers | `core/lib/TripleTerms` (`walk` :46-57, `map` :74-86) → a compound-term walker; `core/lib/RelativeIris.relativizeNode` :54-57; `core/RelativeIRIResolver.tripleTermRecursive` :164-168 |
| Order | `core/lib/NodeComparator.getMacroType` :483-489 and the recursion :147-159; `core/lib/TermKey` (:59, `placeOf` :155-157, `compare` :185-188) |
| Normalization | `PositionalDictionaryWriterBuilder.canonicalizeNumericObject` :281-291, `alignBnodes` :222-240 (and its fast exit :226-228); `huge/HugeBuildPipeline.canonicalizeNumericObject` :906-913, `scopeNode` :714-734 |
| Guards and registration | `ProcessQuad` :411-568 (`registerTripleTerm` :520-568); `UltraIngest.buildSets` :296-336, `registerTripleTermSet` :358-399; `HugeBuildPipeline.acceptRow` :746-773, `collectTripleTerm` :805-860 |
| Section routing | `hdf5/DictionarySection.LITERALS` :23-24; `hdf5/writers/DictionaryIds.objectId` :33-40; `HugeBuildPipeline.mergeRange` :1198, :1212; `huge/StreamingDictionaryWriter` :109-112 |
| Encoder and store | `hdf5/writers/DictionaryNodeEncoder.encode` :226-246; `hdf5/writers/MultiTypeDictionaryWriter` (:101, :109-121, :228-238, :276-292) |
| Engines | `PositionalDictionaryWriter` :63-79, :161-163; `parallel/ParallelPositionalDictionaryWriter` :86-107; `ultra/UltraDictionary` :108-160; `huge/HugeTripleTerms` → `HugeSequences`; `HugeBuildPipeline` :457-501; `StreamingDictionaryWriter` :59-65, :218-287 |
| Disk serialization | `huge/NodeCodec` (tags :30-35, write :73-85, read :114-120); `huge/HugeRecords.estimateBytes` :93-97 |
| Reading | `hdf5/readers/MultiTypeDictionaryReader` (extract :293-301, suffix :153-167, ordinal check :250, search :368-413); `PositionalDictionaryReader` :63-102, :167-186; `hdf5/jena/SimpleNodeTable` :24-29, :87-99 |
| Store abstraction (step 6) | `store/WritableStore`, `hdf5/WritableGroup`, `hdf5/WritableDataset`, `hdf5/WritableHdfFile`, `store/ZarrStoreWriter`; `store/OpenStore`, `StoreGroup`, `StoreArray`, `Hdf5Store`, `ZarrStore` (:304-315) |
| Query routing | `hdf5/jena/BGIteratorMaster` :43-53; `BGIteratorPOS` :105-114, :242-247; `BGIteratorSO` :116-127, :206-255; `BGIteratorSPO_All` :155-174; `hdf5/jena/StageGeneratorDirectorBG` :28-34; `OpExecutorBG` :158-210 |
| Compound matching | `hdf5/jena/TripleTermMatcher` → `SequenceMatcher`; `BindingNodeId` (node-valued slot) |
| Variable analyses | `core/BGReorderTransform.addObjectVars` :407-419; `core/MinusJoin.addVars` :174-184 and `boundVars` :120-172; `core/PathMerge.addVars` :259-270; `GraphVarBG.isPattern` :253-255 and `walkable` :199-244; `OptionalHashJoin` :168-179; `HDF5Reader.collectTripleTermVars` :584-594 |
| Guards on the query side | `FilterBounds.orderAgreesWithArq` :112-124; `core/fuseki/ResultCache.walk` :137-168; `BGSparqlService.containsTripleTerms` :268-276 |
| Output | `hdf5/jena/IndexExport` :136, :256; `cmdline/BeakGraphCLI` :545-559, :612-618; `core/fuseki/DistinctNodeCounter.hash` :99-107 |
| Version | `Params.FORMAT_VERSION` :66; `HDF5Reader` :127 |
