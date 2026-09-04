# BeakGraph File Format Specification

**Format version 5 — derived from and verified against BeakGraph 0.18.0+ (2026-07-18; v5 adds RDF 1.2 triple terms).**

This document specifies the on-disk format of a BeakGraph store (`.h5`) precisely enough that an
independent implementation — in Rust, C, Python, or any other language, with no access to the Java
source — can **create** files that the reference reader accepts and answers queries from correctly,
and can read files the reference writer produces.

What this document does **not** cover, because it is specified elsewhere:

* The HDF5 container format itself ([HDF Group specification](https://www.hdfgroup.org/solutions/hdf5/)).
  §3 defines the narrow *profile* of HDF5 that BeakGraph uses; everything below that profile is any
  conformant HDF5 library's job.
* RDF concepts, RDF 1.1/1.2 syntax, SPARQL semantics (W3C specifications).
* Zstandard compression ([RFC 8878](https://www.rfc-editor.org/rfc/rfc8878)).
* The SPARQL-CDT composite-datatype proposal ([spec](https://awslabs.github.io/SPARQL-CDTs/spec/latest.html)).

All facts below were taken from the 0.18.0 reference implementation and cross-checked against a real
file built and byte-dumped for the purpose; Appendix A reproduces that worked example. The reference
implementation ships six build engines (`-method 0…5`); they are alternative *pipelines*, not
alternative *formats* — methods 0/2/3 produce byte-identical stores and methods 1/4/5 produce
isomorphic stores (blank-node treatment aside, §7.6). This document specifies the format they all
target.

---

## 1. Scope and mental model

A BeakGraph is a **read-only, indexed RDF Dataset** (default graph + named graphs), heavily inspired
by [RDF HDT](https://www.rdfhdt.org/): terms are replaced by integer ids from sorted dictionaries,
and the quads are stored as two sorted orderings (GSPO and GPOS) encoded as succinct
bitmap-plus-sequence level structures. The whole store is a tree of HDF5 groups and small,
individually meaningful datasets.

The single most important design fact, from which most of the format follows:

> **A term's id *is* its 1-based rank in a total order over terms.** There is no persisted term→id
> table anywhere. Readers find a term's id by binary-searching the dictionary, re-materializing
> candidate terms and comparing them with the *same* total order the writer sorted with (§6). A
> writer that sorts with a subtly different order produces a file whose terms silently cannot be
> found.

The second key fact: **the format evolves by presence-sniffing.** Readers probe for optional
datasets by name and treat absence as "feature not used" (e.g. no language-tagged literals → no
`langs`/`langTags` datasets). The `formatVersion` attribute exists only to stop *old* readers from
misreading *newer* files; it is not consulted to locate data.

---

## 2. Terminology

* **Term / node** — an RDF term: IRI, blank node, or literal.
* **Section** — one sorted dictionary (`entities`, `predicates`, or `literals`).
* **Id** — a 1-based rank within a section (or within the composite object space, §7.1).
* **Bit-packed sequence** — the primitive of §5.1.
* **FCD** — front-coded dictionary of strings, §5.5.
* **Run** — a maximal group of consecutive index rows sharing the same parent (§8.1).
* `MinBits(x)` — the number of bits needed to represent `x`: `MinBits(0) = 1`, otherwise
  `64 − countLeadingZeros(x)` (so `MinBits(1)=1`, `MinBits(2)=2`, `MinBits(255)=8`,
  `MinBits(256)=9`).
* **Byte-rounded width** — `ceil(w / 8) * 8`, with a floor of 8 (a computed width of 0 becomes 8).

Integer byte order is **big-endian everywhere**. Text is **UTF-8** on disk; "string length" in
prefix coding means **UTF-16 code units** (§5.5.3 — this is a deliberate Java-ism that must be
reproduced).

---

## 3. HDF5 container profile

BeakGraph uses a deliberately tiny slice of HDF5. A conformant writer MUST stay inside it, because
the reference reader bypasses most of the HDF5 data model and reads dataset bytes raw:

1. **Datasets are 1-dimensional arrays of 8-bit signed integers** (Java `byte[]`; HDF5 type class
   integer, size 1). The reader treats every dataset as an opaque byte region; the HDF5 element type
   is never used to interpret values.
2. **Storage layout MUST be contiguous.** The reader casts every dataset to a contiguous dataset and
   memory-maps (or range-reads) its byte extent directly from the file at `dataAddress`. Chunked,
   compact, or filtered (compressed/shuffled) layouts are **not readable**.
3. **No HDF5-level compression or filters.** Compression happens *inside* BeakGraph's own encodings
   (§5.4/§5.5), where it survives raw byte access.
4. **Attributes are scalars** with exact Java-visible types, and the reader casts them un-defensively:

   | Attribute | HDF5 type | Appears on |
   |---|---|---|
   | `formatVersion` | 32-bit signed int | the `.BG` group |
   | `numQuads` | 64-bit signed int | the `.BG` group |
   | `numEntries` | 64-bit signed int | every bit-packed / raw-value dataset, FCD groups |
   | `width` | 32-bit signed int | every bit-packed dataset |
   | `blockSize` | 32-bit signed int | FCD groups |
   | `numBlocks` | 64-bit signed int | FCD groups |
   | `compression_threshold` | 32-bit signed int | FCD groups |

   Writing `numEntries` as a 32-bit integer (or `width` as 64-bit) will crash the reference reader
   with a cast error. Match the widths exactly.
5. Group and dataset **names are case-sensitive** and exactly as given in this document.
6. Anything else in the HDF5 file outside the `.BG` group is ignored by readers.

The reference writer uses jHDF 0.12 for the in-memory engines; files from the "huge" engines are
written with the native HDF5 library. Both stay inside this profile.

---

## 4. Top-level layout and versioning

```
/                          HDF5 root
└── .BG                    group ("the store")
    ├── @numQuads          int64   — count of SOURCE quads only (§9.6)
    ├── @formatVersion     int32   — 4
    ├── dictionary/        group   (§7)
    │   ├── entities/      group   — multi-type section (may be absent, §7.9)
    │   ├── predicates/    group   — multi-type section
    │   ├── literals/      group   — multi-type section
    │   ├── graphs         bit-packed id list (§7.8)
    │   ├── subjects       bit-packed id list
    │   └── objects        bit-packed id list
    ├── GSPO/              group   — quad index, Graph-Subject-Predicate-Object order (§8)
    └── GPOS/              group   — quad index, Graph-Predicate-Object-Subject order
```

### 4.1 Version semantics

* A file with no `formatVersion` attribute is version 1.
* A reader supporting version *N* MUST reject files with `formatVersion > N`
  ("newer than this build supports"). It MUST accept `≤ N`.
* Version history (all changes are additive; readers locate features by dataset presence):

  | v | Change |
  |---|---|
  | 1–2 | Historic. The rank/select directory (§8.4) in these files is **incorrectly seeded**; readers ignore it and fall back to linear bitmap scans. |
  | 3 | Rank/select directory corrected: `BB` seeded so `BB[k]` = ones-before-block-*k*, `SB` entries widened to cover the bitmap length. Directory is trusted from v3 (`RANK_DIRECTORY_MIN_VERSION = 3`). |
  | 4 | Optional `langDirs` dataset (base direction for `rdf:dirLangString`, §7.5.3). Absence = "no directional literals", which is why v3 files read unchanged. |
  | 5 | RDF 1.2 triple terms: `DataType.TRIPLE_TERM` (ordinal 13) and the optional `tripleTerms` component store in the literals section (§7.6a). Triple terms occupy that section's contiguous suffix of the object id space. Absence = "no triple terms"; v3/v4 files read unchanged, and a triple-term-free v5 store is shape-identical to v4 output. |

* **A new writer MUST stamp `formatVersion = 5`** and produce a correct directory (§8.4).

---

## 5. Primitive encodings

Everything in the store is built from five primitives.

### 5.1 Bit-packed unsigned integer sequence

A sequence of `numEntries` fixed-width unsigned integers packed **MSB-first** into a byte stream.

* Attributes: `width` (int32, bits per value), `numEntries` (int64).
* Legal widths: **1…57, or exactly 64**. Widths 58–63 are illegal (the reference reader's
  unaligned-64-bit fast path cannot serve them).
* Encoding: conceptually concatenate each value's `width`-bit big-endian representation into one
  bit string; bit 0 of the string is the **most significant bit of byte 0**. The final byte is
  zero-padded in its **low** bits.
* Decoding entry `i`: the bits at positions `[i*width, (i+1)*width)` of that bit string, read as an
  unsigned big-endian integer. Equivalently: read the (unaligned) 64-bit big-endian word at byte
  `(i*width) >> 3`, then `(word >>> (64 − (i*width & 7) − width)) & ((1 << width) − 1)`.
* **Signedness exception**: widths 32 and 64 may carry negative values as raw two's-complement bit
  patterns (used for `xsd:int` / `xsd:long` value stores, §7.3). At any other width values MUST be
  non-negative and `< 2^width`.
* A sequence with zero bytes is **not written** — the dataset is simply absent.
* 1-bit sequences are the store's **bitmaps**. `select1(r)` (1-based rank *r* → 0-based position of
  the *r*-th set bit) is the operation the indexes are designed around; §8.4 defines its
  acceleration directory.

### 5.2 VByte (variable-length unsigned integer)

Used only inside FCD string buffers (§5.5). **Note the convention — it is HDT's, not LEB128's:**

* The value is emitted in little-endian 7-bit groups (least-significant group first).
* Bytes with the high bit **clear** (`0x00–0x7F`) are continuation bytes; the **final** byte has the
  high bit **set** (`0x80 | group`).
* Value 0 encodes as the single byte `0x80`. Value 300 (`0b100101100`) encodes as `0x2C 0x82`.

Decoding: accumulate `byte & 0x7F` groups, shifting each into place 7 bits higher than the last,
until a byte with the high bit set is consumed (that byte's low 7 bits are the final, highest
group).

### 5.3 Raw big-endian value sequence

A plain concatenation of fixed-size big-endian values with a `numEntries` (int64) attribute and no
`width`. Three element types occur:

| Dataset | Element |
|---|---|
| FCD `offsets` (§5.5) | 8-byte signed big-endian integer |
| dictionary `floats` | 4-byte IEEE-754 binary32, big-endian |
| dictionary `doubles` | 8-byte IEEE-754 binary64, big-endian |

### 5.4 Compressed fragment

Wherever a string fragment is stored compressed (flagged per-entry, §5.5), the payload is:

```
[ 4-byte big-endian int: uncompressed UTF-8 byte length ][ standard Zstandard frame ]
```

Any compliant zstd encoder output is acceptable (the reference uses aircompressor; readers use a
standard zstd decoder and rely on the explicit length header for output sizing, not on the frame's
optional content-size field).

### 5.5 Front-coded string dictionary (FCD)

An FCD stores an ordered sequence of strings with shared-prefix elimination, in blocks of
`blockSize` entries (**always 16** in practice; must be ≥ 2). It appears as a **group**:

```
<name>/                          group
  @blockSize              int32  = 16
  @numBlocks              int64  = ceil(numEntries / blockSize)   (0 when empty)
  @numEntries             int64
  @compression_threshold  int32  = 64
  stringbuffer            byte dataset — the encoded stream (no attributes)
  offsets                 raw int64 sequence — one entry per block: byte offset of the
                          block's first fragment within stringbuffer
  compressed              1-bit bit-packed sequence — one bit per ENTRY: 1 iff that
                          entry's fragment payload is compressed (§5.4)
```

#### 5.5.1 Stream layout

Entries `16k … 16k+15` form block `k`. Within `stringbuffer`:

* **Block head** (first entry of a block): `VByte(fragLen)` + fragment payload, where the fragment
  is the **entire** string.
* **Every subsequent entry**: `VByte(prefixLen)` + `VByte(fragLen)` + fragment payload, where the
  fragment is the **suffix** after the shared prefix with the *immediately preceding entry's full
  string*.

`fragLen` is always the **stored payload's byte length** (compressed length when the compressed bit
is set, raw UTF-8 length otherwise).

#### 5.5.2 Compression rule

A fragment (head string or suffix) is compressed iff its **raw UTF-8 byte length ≥ 64**
(`compression_threshold`). The corresponding entry's bit in `compressed` is set. Compression is
unconditional at the threshold, even if it inflates.

#### 5.5.3 Prefix length semantics — UTF-16 code units

`prefixLen` counts **UTF-16 code units** (Java `char`s) of the shared prefix — *not* bytes, *not*
code points. The writer additionally guarantees the prefix never ends between a UTF-16 surrogate
pair (it backs off one unit if it would), so the boundary is always a code-point boundary. A
non-Java implementation must therefore:

* *Writing*: find the longest common prefix by code points; `prefixLen` = its UTF-16 length (code
  points ≥ U+10000 count as 2, all others as 1).
* *Reading*: new string = first `prefixLen` UTF-16 units of the previous string + decoded suffix.

#### 5.5.4 Reading entry *n*

`block = n / blockSize`; seek to `offsets[block]`; decode the head; then apply `n mod blockSize`
suffix steps. (The `compressed` bit for the *j*-th fragment decoded this way is looked up at global
entry index `16·block + j`.)

#### 5.5.5 Entry order

**FCD entries are NOT independently sorted.** Each dictionary FCD holds the strings of its section's
nodes *in section order* (i.e. the total order of §6, filtered to the rows that use that FCD). This
is naturally near-sorted — which is what makes front-coding effective — but the FCD itself carries
no search structure; all term lookup happens at the section level (§7).

#### 5.5.6 Worked micro-example

From Appendix A, the literals `strings` FCD begins (hex):

```
8e 69 6e 2d 62 6e 6f 64 65 2d 67 72 61 70 68   VByte(14), "in-bnode-graph"   (block head)
80 85 70 6c 61 69 6e                            prefix 0, len 5, "plain"
80 85 74 79 70 65 64                            prefix 0, len 5, "typed"
80 85 68 65 6c 6c 6f                            prefix 0, len 5, "hello"
85 80                                           prefix 5, len 0   ("hello" again — a
85 80                                           prefix 5, len 0    lang-variant's identical
85 80                                           prefix 5, len 0    lexical form, §7.5)
80 84 33 2e 31 34                               prefix 0, len 4, "3.14"
...
```

---

## 6. The total order on RDF terms

This section is the heart of the format. Ids are ranks in this order; the reference reader
binary-searches with it; the ValueCluster range optimization (§10.4) additionally requires
value-equal literals to be **adjacent** in it. An implementation must reproduce it exactly.

### 6.1 Top-level algorithm

To compare nodes `a`, `b`:

1. **Default-graph sentinels first.** If a node is `<urn:x-arq:DefaultGraph>` or
   `<urn:x-arq:DefaultGraphNode>` it ranks before every other node. If both are sentinels, compare
   them as plain terms (§6.4) — `…DefaultGraph` < `…DefaultGraphNode`.
2. **Macro kind:** blank node (1) < IRI (2) < literal (3) < triple term (4). Different kinds order
   by kind.
3. **Two blank nodes:** by label, as Unicode code-point string comparison (`"b10" < "b2"`).
4. **Two IRIs:** by IRI string, code-point comparison. *Relative* IRIs (§9.2) compare by their
   stored relative form (so `""` sorts before any `http://…`, and `"sib.png"` after).
5. **Two triple terms:** STRUCTURALLY, by recursing this whole comparator through the components —
   subject, then predicate, then object (nested terms recurse; RDF 1.2 forbids cyclic terms, so
   this terminates). The recursion is mandatory: delegating triple-term pairs to a plain
   term-comparator would collapse terms differing only in an embedded base-direction literal
   (§6.3's repair never fires inside a delegated comparison), corrupting id assignment.
6. **Two literals:** §6.2.

### 6.2 Literal ordering

Apply the first matching rule:

1. **Both composite (`cdt:List`/`cdt:Map`)** — datatype IRI compared exactly as
   `http://w3id.org/awslabs/neptune/SPARQL-CDTs/List` <
   `http://w3id.org/awslabs/neptune/SPARQL-CDTs/Map`, then **lexical form** (code-point string
   compare). Composite values are *never* parsed for ordering — CDT has no canonical form, so term
   identity is lexical identity.
2. **Both language-tagged** (`rdf:langString` or `rdf:dirLangString`) — by
   `(language tag, lexical form, direction)` where direction ranks absent (0) < `ltr` (1) <
   `rtl` (2). Language tags compare as code-point strings; store the parser's case-canonicalized
   form (e.g. `en`, `en-US` — language subtag lowercase, region uppercase).
3. **Both in the same timezone-sensitive temporal value space** — the spaces are dateTime
   (incl. dateTimeStamp), date, time, gYear, gYearMonth, gMonth, gMonthDay, gDay, duration; a
   literal is in a space only if it is a *well-formed* instance:
   * All spaces except duration: compare as instants after **pinning a missing timezone to UTC**
     (XSD's ±14 h indeterminacy is resolved to UTC; pairs already determinate under XSD order are
     unaffected). Equal instants (e.g. `…Z` vs `…+00:00`) fall through to §6.4.
   * duration: compare by total months (years·12+months, signed), then total seconds
     (days·86400+hours·3600+minutes·60+seconds, signed, exact decimal), then §6.4. (So
     `"P1D"` < `"PT24H"` only via the §6.4 tie-break — they are value-equal — and both < `"P1M"`.)
4. **Everything else — SPARQL value order with fixed cross-space ranks** (this is Jena
   `NodeValue.compareAlways` behavior, reproduced here normatively):
   * Classify each literal into a value space. Two literals in the **same** space compare by
     **value** (numbers numerically across *all* numeric datatypes — int, integer, decimal, long,
     float, double interleave by magnitude; booleans false < true; strings by code point).
     Ill-formed literals (unparseable value for their datatype) and literals of unknown datatypes
     compare within their cluster by **lexical form, then datatype IRI**.
   * Two literals in **different** spaces order by this fixed rank (empirically verified against the
     reference stack; normative for compatibility):

     ```
     xsd:string (and simple literals)
     < language-tagged (rdf:langString / rdf:dirLangString)
     < numeric (all numeric XSD datatypes, one space)
     < xsd:boolean
     < xsd:gDay < xsd:gMonth < xsd:gMonthDay < xsd:gYear < xsd:gYearMonth
     < xsd:dateTime (and dateTimeStamp) < xsd:date < xsd:time
     < xsd:duration (incl. yearMonth/dayTime)
     < cdt:List < cdt:Map
     < unknown datatypes and ill-formed literals
     ```
   * If the value comparison answers "equal" (value-equal but possibly term-distinct, e.g.
     `"1"^^xsd:int` vs `"1"^^xsd:integer` vs `"1.0"^^xsd:double`), fall through to §6.4. This is
     what makes value-equal terms **adjacent but distinct** — the property ValueCluster (§10.4)
     depends on.

### 6.3 Exact-term tie-break

The final tie-break on two literals (used wherever a rule above answers "equal" but the terms may
differ) is:

1. lexical form (code-point compare);
2. datatype IRI (code-point compare);
3. language tag (empty for none);
4. base direction: absent < `ltr` < `rtl`.

(Steps 1–3 are Jena's `NodeCmp.compareRDFTerms`; step 4 repairs that comparator's blindness to
direction. The composite branch (§6.2.1) deliberately checks datatype *before* lexical form —
that exception is confined to composite–composite pairs.)

### 6.4 Invariants the order must satisfy

* It is a **strict total order on distinct terms**: `compare(a,b) = 0 ⇔ a` and `b` are the same RDF
  term. Any comparator that answers 0 for distinct terms collapses them onto one id and corrupts
  lookup; any cyclic comparator makes the sort input-order-dependent.
* **Value-equal literals are adjacent** (§6.2.4 + §6.3): everything between the first and last term
  with a given value has that value.
* It never throws: unparseable literals fall into the lexical cluster of §6.2.4.

A practical verification recipe for a new implementation is in §11.

---

## 7. The dictionary

### 7.1 Sections and id spaces

Terms are partitioned into three sorted sections under `/.BG/dictionary/`:

| Section | Contains | Id space |
|---|---|---|
| `entities` | Every term that occurs as a **graph name, subject, or non-literal object** — including IRIs and blank nodes appearing only **inside** triple terms. One shared pool — a node used as both graph and subject appears once. | 1 … `numEntities` |
| `predicates` | Every predicate IRI — including predicates appearing only inside triple terms. **Isolated** from entities — the same IRI used as predicate *and* subject gets one id in each section (isolation keeps predicate id widths small). | 1 … `numPredicates` |
| `literals` | Every literal **object** (top-level or inside a triple term), plus — as the section's **contiguous suffix** — every RDF 1.2 **triple term** at any nesting depth (v5). The §6 order macro-ranks triple terms after all literals, which is what makes the suffix contiguous. | 1 … `numLiterals` |

Each section is independently sorted by §6 and ids are 1-based ranks within the section.

**The object id space** is the concatenation `entities ⊕ literals`: an object's id is its entity id
if it is an IRI/bnode, or `maxEntityId + sectionId` if it is a literal **or triple term**. Because
the §6 order puts every bnode and IRI before every literal, and every literal before every triple
term, the *combined* object space is itself sorted — the property the object-level binary
searches, range filters, and the triple-term suffix range test rely on.

Graph names and subjects use raw entity ids. There is no separate graph dictionary.

### 7.2 Multi-type section layout

Each section is a group holding parallel per-node columns plus type-specific value stores. **Every
column that exists has exactly one entry per node, in id order** — this parallel-array invariant is
absolute; even rows that don't use a column write a 0 placeholder into it.

Per-node columns (bit-packed, §5.1):

| Dataset | Width | Meaning |
|---|---|---|
| `datatypes` | `1 + MinBits(14)` = **5** | The row's `DataType` ordinal (§7.4) — selects which value store the row's payload lives in. |
| `offsets` | `1 + MinBits(numNodes)` | Index of the row's payload **within its type-specific store** (0-based entry index — *not* a byte offset). Bnodes write 0. |
| `typedLiterals` | `1 + MinBits(numDatatypeIRIs)` | 1-based id into `typedLiteralsDictionary`; 0 for non-literal rows. **Literals section only.** |
| `langTags` | `1 + MinBits(numLangs)` | 1-based id into `langs`; 0 = no language tag. Present only if any language-tagged literal exists. |
| `langDirs` | **3** (`1 + MinBits(2)`) | 0 = none, 1 = `ltr`, 2 = `rtl`. Present only if any base-direction literal exists (format v4). |

Type-specific value stores (present only when non-empty; `offsets` points into these):

| Dataset | Kind | Holds |
|---|---|---|
| `iri` | FCD | IRI strings (absolute and relative share this store, distinguished by `datatypes`). |
| `strings` | FCD | Lexical forms of every string-routed literal (§7.3). |
| `tripleTerms` | bit-packed | RDF 1.2 triple-term component ids, fixed stride 3 (§7.6a). Width: `1 + MinBits(numEntities + numLiteralsSection)`, rounded to 64 if > 57. |
| `typedLiteralsDictionary` | FCD | The distinct literal datatype IRIs (§7.5.1). |
| `langs` | FCD | The distinct language tags (§7.5.2). |
| `integers` | bit-packed | `xsd:int` values. Width: **32** if any value is negative (two's complement), else `1 + MinBits(maxValue)`. |
| `longs` | bit-packed | `xsd:long` values. Width: **64** if any negative or `1 + MinBits(maxValue) > 57`, else `1 + MinBits(maxValue)`. |
| `floats` | raw binary32 BE | `xsd:float` values. Offset is the element index (byte offset = 4·offset). |
| `doubles` | raw binary64 BE | `xsd:double` values. Offset is the element index (byte offset = 8·offset). |

Sections omit columns that cannot apply: `entities` and `predicates` have **no**
`typedLiterals`/`langTags`/`langDirs` and no literal value stores — just `offsets`, `datatypes`,
and `iri`.

### 7.3 Node encoding rules (the routing table)

Walk the section's nodes in sorted order; for each node append one entry to every existing per-node
column:

| Node | `datatypes` | `offsets` | Payload |
|---|---|---|---|
| Blank node | `BNODE` (0) | 0 | **Nothing.** Labels are not stored (§7.6). |
| IRI with a scheme | `IRI` (1) | next `iri` index | IRI string → `iri` FCD |
| IRI without a scheme (relative reference, §9.2) | `RELATIVE_IRI` (12) | next `iri` index | relative string (possibly `""`) → `iri` FCD |
| Literal, datatype `xsd:int`, value parses | `INTEGER` (6) | next `integers` index | 32-bit value → `integers` |
| Literal, `xsd:long`, value parses | `LONG` (7) | next `longs` index | 64-bit value → `longs` |
| Literal, `xsd:float`, value parses | `FLOAT` (8) | next `floats` index | binary32 → `floats` |
| Literal, `xsd:double`, value parses | `DOUBLE` (9) | next `doubles` index | binary64 → `doubles` |
| **Every other literal** | `STRING` (2) | next `strings` index | lexical form → `strings` FCD |
| RDF 1.2 triple term (literals section only) | `TRIPLE_TERM` (13) | its 0-based triple-term ordinal *k* | component ids `(s, p, o)` → `tripleTerms` entries `[3k, 3k+2]` (§7.6a) |

Notes:

* "Every other literal" includes `xsd:string`, plain and language-tagged strings, `xsd:boolean`,
  `xsd:integer` (unbounded — deliberately *not* packed), `xsd:decimal`, all temporal types, WKT,
  CDT composites, custom datatypes, and **ill-typed literals** (`"abc"^^xsd:int` — value fails to
  parse → strings route, preserving the term exactly).
* Literal rows *additionally* write `typedLiterals` = the 1-based id of the literal's datatype IRI
  (for language-tagged literals that is `rdf:langString`/`rdf:dirLangString`), `langTags`, and
  `langDirs` as applicable. Non-literal rows write 0 to whichever of those columns exist.
* The four value-typed routes store **values, not lexical forms** — see canonicalization, §9.3.

### 7.4 `DataType` ordinals

The `datatypes` column stores ordinals of this enumeration. **Ordinals are on-disk values: append
only, never reorder, never recycle** — the marked dead entries were never written by any version but
their slots are burned.

| Ordinal | Name | Status |
|---|---|---|
| 0 | `BNODE` | live |
| 1 | `IRI` | live |
| 2 | `STRING` | live |
| 3 | `BYTE` | dead — reserved |
| 4 | `BOOLEAN` | dead — reserved |
| 5 | `SHORT` | dead — reserved |
| 6 | `INTEGER` | live (`xsd:int` only) |
| 7 | `LONG` | live |
| 8 | `FLOAT` | live |
| 9 | `DOUBLE` | live |
| 10 | `BIG_INTEGER` | dead — reserved |
| 11 | `BIG_DECIMAL` | dead — reserved |
| 12 | `RELATIVE_IRI` | live |
| 13 | `TRIPLE_TERM` | live since v5 (slot 14 of 15 — one slot remains before the column widens to 6 bits) |

The column width is `1 + MinBits(count-of-ordinals)` — 5 bits while the enum has ≤ 15 values.
Readers take the width from the attribute, so growth is read-compatible.

### 7.5 The small auxiliary dictionaries

#### 7.5.1 `typedLiteralsDictionary`

The distinct **datatype IRIs of every literal in the section** (including the value-typed ones and
`rdf:langString`/`rdf:dirLangString`), sorted by **plain string order** (UTF-16 code-unit order —
for ASCII IRIs this equals byte order; note this is *not* the §6 node order). Ids are 1-based
positions. Every literal row's `typedLiterals` entry references it; rows whose datatype route is
value-typed still record their datatype id here even though readers don't consult it for them.

#### 7.5.2 `langs` / `langTags`

`langs` is the sorted (same plain string order) set of distinct language tags, as produced by the
parser. `langTags[row]` = 1-based tag id, 0 = untagged. Both absent when no tagged literal exists.

#### 7.5.3 `langDirs`

Per-row base direction (0/1/2 = none/ltr/rtl), present only when some literal has one. A direction
implies a language tag. Absence of the dataset means "no directions anywhere" — this is what keeps
pre-v4 files readable.

### 7.6a Triple terms (format v5)

RDF 1.2 triple terms (`<<( s p o )>>`) are stored **structurally**, never as text. A triple-term
row in the literals section carries `datatypes = TRIPLE_TERM` and `offsets = k`, its 0-based
ordinal among the section's triple terms in id order; entries `[3k, 3k+1, 3k+2]` of the
`tripleTerms` store hold the component ids:

* `s` — an **entity id** (triple-term subjects are IRIs or blank nodes);
* `p` — a **predicate id**;
* `o` — an **object-space id**: an entity id, or `maxEntityId + sectionId` for a literal or a
  **nested triple term**, which therefore resolves recursively through the same machinery.

Consequences and invariants:

* Every component term MUST have its own dictionary entry — ingest recurses into triple terms so
  that an IRI, blank node, literal, or nested term appearing *only* inside one still gets an id.
  Interior terms do **not** join the `graphs`/`subjects`/`objects` role lists (§7.8): those record
  top-level positions only.
* Blank nodes inside triple terms are ordinary entity ids, so the rank-derived label regeneration
  (§7.6) preserves co-reference between a blank node inside a term and the same node in the graph
  — the property that forces rejection of blank nodes inside `cdt:` composite literals is free
  here by construction.
* Triple terms occupy the literals section's **contiguous suffix** (macro rank 4, §6.1), giving
  readers an O(1) "is this object id a triple term" range test and letting scans over triple-term
  patterns clamp to the suffix.
* Rows write 0 into `typedLiterals`/`langTags`/`langDirs` (the parallel-array invariant) and
  contribute nothing to `typedLiteralsDictionary` — a section whose only rows are triple terms
  omits that FCD entirely.
* Numeric canonicalization (§9.3) applies **inside** triple terms (the object component), and the
  component-kind guards (§9.5) apply at every depth. RDF 1.2 permits triple terms in the object
  position only; writers MUST reject them anywhere else.

### 7.6 Blank nodes

**Blank node labels are not stored.** A bnode's row carries only its ordinal (`BNODE`, offset 0).
Readers materialize the label **from the id**: `b%020d` formatted with the section id (e.g. entity
id 2 → `_:b00000000000000000002`). Consequences a writer must honor:

* Distinct blank nodes (per RDF's document-scoped label semantics — labels in different source
  documents never co-refer) MUST be distinct dictionary entries; co-referring uses of one bnode MUST
  collapse to one entry. Labels themselves are immaterial: the output dataset is isomorphic, not
  label-identical, to the source.
* For the *sort*, bnodes compare by label (§6.1.3), so the writer's choice of internal labels
  determines bnode id order. Any distinct labels work. (The reference in-memory engines relabel
  bnodes `b%020d` by first-encounter order, giving encounter-ordered ids; the disk engines use a
  different scheme. Both are conformant.)
* Because labels inside `cdt:` composite literals *are* semantically load-bearing, literals
  containing them are **rejected at ingest** (§9.5).

### 7.7 Reader reconstruction (`extract`) and canonical lexical forms

For validation, this is how a row becomes a term again (given row index `idx` = id − 1,
`off = offsets[idx]`, `dt = datatypes[idx]`):

* `BNODE` → blank node labeled `b%020d`(id).
* `IRI` / `RELATIVE_IRI` → IRI from `iri[off]`, string verbatim (relative refs stay relative;
  resolution against the serving URL is a query-time concern, not a storage one).
* `INTEGER`/`LONG` → integer literal of value `integers[off]` (sign-restored from two's complement
  at width 32/64) with datatype `xsd:int`/`xsd:long` and canonical lexical form = decimal
  `toString`.
* `FLOAT`/`DOUBLE` → literal of the IEEE value with lexical form per **Java
  `Float.toString`/`Double.toString`** (shortest decimal that round-trips, always with a decimal
  point, switching to exponent notation outside ~[10⁻³, 10⁷); e.g. `1.5`, `2.5`, `1.0E8`). A
  non-Java *reader* must reproduce this mapping to be term-compatible; a *writer* is unaffected
  (it stores bits, §9.3).
* `STRING` → let `lex = strings[off]`. If `langTags[idx] > 0`: a language-tagged literal with tag
  `langs[langTags[idx]−1]`, and, if `langDirs[idx] > 0`, base direction ltr/rtl
  (datatype `rdf:dirLangString`). Otherwise a typed literal with datatype IRI
  `typedLiteralsDictionary[typedLiterals[idx]−1]` (`typedLiterals[idx]` MUST be ≥ 1 here; a
  non-language `STRING` row with 0 is corrupt).
* `TRIPLE_TERM` → read `(s, p, o) = tripleTerms[3·off … 3·off+2]`; the term is
  `<<( extract_entities(s) extract_predicates(p) extract_objectSpace(o) )>>`, where the object
  extraction recurses back through this rule for nested terms.

### 7.8 Columnar role lists: `graphs`, `subjects`, `objects`

Three bit-packed id lists directly under `dictionary/`, recording **which ids actually occur in each
role** (the sections themselves pool roles):

| Dataset | Contents | Width |
|---|---|---|
| `graphs` | entity ids used as a graph name | byte-rounded `MinBits(numEntities + 1)` |
| `subjects` | entity ids used as a subject | byte-rounded `MinBits(numEntities + 1)` |
| `objects` | object-space ids (entity or `maxEntityId+literal`) used as an object | byte-rounded `MinBits(numEntities + numLiterals + 1)` |

Each list holds the distinct nodes of that role **sorted by §6** — equivalently, ids in ascending
order. Readers use them for `GRAPH ?g` enumeration, graph-membership tests, and
`SELECT DISTINCT ?s/?o` streaming; graph enumeration order = list order.

These lists are written whenever the store holds any quad (they are what makes a graph *a graph*;
the default graph's sentinel IRI appears here like any other graph name).

### 7.9 Empty stores

A store built from an empty source has **no section groups and no columnar lists at all** — readers
detect "no `entities`, no `predicates`, no `literals`" and answer every pattern with zero rows.
(An empty *section* — e.g. no literals anywhere — is likewise simply absent.) `numQuads` counts
source quads only, so it can be 0 even in a non-empty store that holds injected metadata (§10.1);
never key emptiness on it.

---

## 8. The quad indexes: `GSPO` and `GPOS`

Two orderings of the same deduplicated quads. `GSPO` serves patterns with graph+subject bound;
`GPOS` serves graph+predicate(+object) bound; a graph-variable pattern loops over the `graphs` list
(§7.8) running one of them per graph. Exactly these two exist — a conformant file MUST contain both.

### 8.1 Conceptual model

Take all quads (source + injected, §10), map each position to its id — for index `XYZW`, position
letters map G→graph id, S→subject entity id, P→predicate id, O→object-space id — sort by the §6
comparators position-by-position (equivalently: by id tuple, since ids are ranks — but note the sort
is defined over *terms*), and **drop exact duplicates**.

The result is a 4-level trie flattened into per-level arrays. Level 0 (the first letter — G for both
indexes) is **implicit**: it is materialized as *runs* at level 1. Levels 1–3 each store, per index
group, two parallel bit-packed datasets:

* `S<c>` — the sequence of ids at that level (`c` = the position's lowercase letter);
* `B<c>` — a bitmap of the same length: **bit set = this row starts a new run** (a new parent
  above it).

plus the rank/select directory `SB<c>`, `BB<c>` (§8.4). For `GSPO` the datasets are
`Bs Ss Bp Sp Bo So` (+ `SBs BBs SBp BBp SBo BBo`); for `GPOS`: `Bp Sp Bo So Bs Ss` (+ directories).

Semantics, stated for GSPO (GPOS is identical with its own position roles):

* `Ss` lists, for each graph in ascending L0 id order, that graph's distinct subjects in ascending
  order. `Bs` has a 1 on the **first** subject of each graph's run.
* `Sp` lists, for each `Ss` row (in `Ss` order), that (graph, subject)'s distinct predicates,
  ascending. `Bp` marks each (graph, subject)'s first predicate.
* `So` lists, for each `Sp` row, that (graph, subject, predicate)'s distinct objects, ascending.
  `Bo` marks run starts likewise.

Ascending ids within every run is what makes the per-run binary searches and object-range filters
sound.

**Addressing.** Because every L0 id from 1 to the *full size of its id space* gets exactly one run
(see padding, next), run lookup is pure `select1`:

* Graph `g`'s subject run: starts at slot `select1_Bs(g)`, ends one before the next set bit (or at
  the end of the bitmap).
* The predicate run of the subject at `Ss` slot `i` (0-based): starts at `select1_Bp(i + 1)`.
* The object run of the `Sp` slot `j`: starts at `select1_Bo(j + 1)`.

### 8.2 Padding — every L0 id owns a slot

L0 for both indexes is the graph position, whose id space is the **entire entities section**
(`maxL0 = numEntities`) — most entities are not graphs. To keep `select1(g)` addressing valid, the
writer emits, for **every L0 id that has no quads** (skipped between real runs, and trailing up to
`maxL0`), one **dummy row at every level**: `S = 0`, `B = 1` in `S1/B1`, `S2/B2`, `S3/B3`. Real ids
are ≥ 1, so a search inside a dummy run can never match. All three levels stay in lockstep
(`len(B1) = len(S1)` = number of L1 rows + dummies, etc.).

### 8.3 Construction algorithm

```
sort all quads by the index's 4-position comparator      (ties = duplicates)
lastUnique = none;  currentL0 = 1
for quad in sorted:
    if quad == lastUnique (all four positions term-equal): continue      # dedup
    changeL0 = first quad, or L0 term differs from lastUnique
    changeL1 = changeL0 or L1 term differs
    changeL2 = changeL1 or L2 term differs
    if changeL0:
        pad (thisL0Id − currentL0 − 1) skipped ids           # dummy rows, §8.2
        currentL0 = thisL0Id
    append to L3:  S3 += id(L3 position);  B3 += (changeL2 ? 1 : 0)
    if changeL2:   S2 += id(L2 position);  B2 += (changeL1 ? 1 : 0)
    if changeL1:   S1 += id(L1 position);  B1 += (changeL0 ? 1 : 0)
    lastUnique = quad
pad (maxL0 − currentL0) trailing ids
```

Widths: `B*` are 1-bit. `S*` widths are **byte-rounded** `MinBits(spaceSize + 1)` with a floor of 8,
where `spaceSize` is the position's id-space size (`numEntities` for G/S, `numPredicates` for P,
`numEntities + numLiterals` for O).

### 8.4 The rank/select directory (`SB*`, `BB*`)

Per level, two bit-packed datasets accelerate `select1` over `B*`, using superblocks of **512** bits
and blocks of **64** bits:

* `SB[j]` = number of set bits in `B` **before** superblock `j` (i.e. in bits `[0, j·512)`),
  cumulative. Seeded `SB[0] = 0`; one more entry appended each time the bit count crosses a
  multiple of 512. Entry count = `1 + floor(totalBits / 512)`.
* `BB[k]` = number of set bits **within block k's superblock** before block `k` (i.e. in bits
  `[floor(k·64/512)·512, k·64)`). Seeded `BB[0] = 0`; appended at every 64-bit crossing (so a
  superblock boundary writes `SB` first, resets the within-superblock count, then writes
  `BB = 0`). Entry count = `1 + floor(totalBits / 64)`.

Widths (byte-rounded): `SB` = `MinBits(maxCumulativeOnes)` where the reference sizes
`maxCumulativeOnes = totalQuadRows + maxL0 + 128` (any bound ≥ the true final count is readable —
the attribute carries the width); `BB` = `MinBits(512)` → **16**.

`select1(rank)` then proceeds: binary-search `SB` for the greatest `j` with `SB[j] < rank`; within
that superblock's blocks binary-search `BB` for the greatest `k` with `SB[j] + BB[k] < rank`; scan
words from bit `k·64` for the `(rank − SB[j] − BB[k])`-th set bit. Readers only trust the directory
when `formatVersion ≥ 3` (§4.1); the datasets are optional for correctness (absent → linear scan)
but a new writer SHOULD always emit them.

### 8.5 Traversal contract (what queries assume)

Illustrated for `GSPO` with G, S, P bound and O free — the other iterators are analogous:

```
gi = locate(graph term in entities)                    # §7 binary search
sRun   = [ select1_Bs(gi), nextSet_Bs − 1 ]
sSlot  = binarySearch(Ss, sRun, subjectId)             # unsigned compare
pRun   = [ select1_Bp(sSlot + 1), nextSet_Bp − 1 ]
pSlot  = binarySearch(Sp, pRun, predicateId)
oRun   = [ select1_Bo(pSlot + 1), nextSet_Bo − 1 ]
answer = So[oRun]                                      # ascending object ids
```

Numeric `FILTER` range constraints are pushed into `oRun` via unsigned `lowerBound`/`upperBound` on
`So`, with bounds widened to whole **value-equal clusters** of the object dictionary (§6.4's
adjacency invariant). The union graph (`urn:x-arq:unionGraph`) is answered by chaining per-graph
scans and deduplicating rows.

---

## 9. Ingest normalization — what a writer MUST do before encoding

The format stores *normalized* quads. A conformant writer applies these steps (order matters for
the bnode step only in that it must see final relative-IRI forms; the reference order is:
default-graph normalization → relativization → bnode handling → numeric canonicalization).

### 9.1 Default graph naming

Quads in the default graph are stored under the graph name **`urn:x-arq:DefaultGraph`** (Jena's
sentinel IRI), which enters the entities dictionary as an ordinary IRI — and, ranking first in §6,
almost always takes entity id 1. Readers translate default-graph requests to this node.

### 9.2 Relative IRIs

BeakGraph stores document-relative references **relatively** and resolves them against the serving
URL at query time. At parse, relative references are resolved against the fixed sentinel base
`http://beakgraph.invalid/d/d/.../d/%00` (32 `d/` directory levels, leaf `%00`); after parsing, any
IRI beginning `http://beakgraph.invalid/` is relativized textually against that base
(`RelativeIris.relativize`) back to its reference form: `<>` → `""`, `<#f>` → `"#f"`, `<?q>` →
`"?q"`, `<sib.png>` → `"sib.png"`, `<sub/x>` → `"sub/x"`, `<../t.png>` → `"../t.png"`,
`<../../x>` → `"../../x"`, `</LICENSE>` → `"/LICENSE"`. The base is deep so that parent and
path-absolute references survive RFC 3986 resolution (which discards `..` above the root); an IRI
that resolves directly under the sentinel root - a source's `</x>`, or a reference more than 32 levels
up, which collapses there - is stored path-absolute (`"/x"`).
The leaf `%00` cannot be authored as a sibling name, so no sibling collapses onto `""`. Stored rows
get `DataType.RELATIVE_IRI`; an IRI is classified relative iff it has no RFC 3986 scheme (empty
string included). A non-Java writer can implement this as: parse with the sentinel base, then
relativize against it with the canonical `../` form for any number of levels (not a single-level
relativizer). Stores written before this base was adopted (BeakGraph 0.18.0 pre-release and earlier)
hold `<../x>` and `</x>` collapsed onto the child form `"x"`; the layout is unchanged, only the
stored reference differs, so such stores open but serve those references below the document's
directory.

**Merged stores.** When several documents are merged into one store, each document is parsed
against the sentinel directory plus its path relative to the merge root (the CLI's `-src`):
`a/x.ttl` parses against `http://beakgraph.invalid/d/.../d/a/x.ttl`, and relativizing against the
common sentinel base then stores its `<>` as `"a/x.ttl"`, its `<img.png>` as `"a/img.png"` and its
`<../shared.png>` as `"shared.png"`. Documents' identical relative references therefore remain the
distinct resources RFC 3986 makes them, and the merged store serves them below its own URL with the
source tree's layout. A store merged from a single document parses like a single-source build.

### 9.3 Numeric canonicalization (documented RDF 1.1 deviation)

Objects typed `xsd:int`, `xsd:long`, `xsd:float`, `xsd:double` whose lexical form parses are
**replaced by their canonical value term** before dictionary construction: `"042"^^xsd:int` becomes
the same term as `"42"^^xsd:int` and they collapse to **one** dictionary entry (value stores hold
values; §7.7 defines the regenerated canonical lexical forms). Ill-typed instances
(`"abc"^^xsd:int`) are left untouched and stored term-exactly via the strings route. Without this
collapse the dictionary would hold two entries the reader's §6 comparator calls equal — breaking
binary search. This is a deliberate, documented deviation from RDF 1.1 term identity; it applies
only to those four datatypes and only in the object position (subjects/predicates/graphs cannot be
literals).

### 9.4 Blank node scoping

Blank node labels are document-scoped: when merging multiple sources into one store, equal labels
from different documents MUST map to distinct stored bnodes; within one document, one label = one
bnode. Labels themselves are discarded (§7.6).

### 9.5 Term guards (what MUST be rejected loudly)

* Graph names and subjects that are neither IRI nor blank node.
* **RDF 1.2 triple terms outside the object position** — the data model permits them nowhere else.
  In the object position they are stored (v5, §7.6a); their components carry the same guards at
  every nesting depth (term subject: IRI/blank; term predicate: IRI).
* **`cdt:List`/`cdt:Map` literals whose (well-formed) value contains a blank node** at any nesting
  depth — rank-derived labels would silently sever the spec-required co-reference between the label
  inside the literal and the graph's bnode. Ill-formed composites are unreachable from parsed
  documents (the CDT-aware parser rejects them) and, if fed programmatically, are stored as opaque
  term-exact strings. (Blank nodes inside TRIPLE TERMS are fine — they are structural entity ids,
  §7.6a.)
* Ill-typed literals otherwise (bad lexical form for a known datatype) are **accepted** and stored
  term-exactly via the strings route.

### 9.6 `numQuads`

The `.BG` attribute counts **raw source quads** — before deduplication, and excluding every injected
quad (§10). It is informational; readers never use it to gate behavior.

### 9.7 Deduplication

Duplicate quads (all four positions term-equal, after normalization) are eliminated **by the index
construction** (§8.3); the dictionary sets are naturally duplicate-free. A quad appearing in two
named graphs is two distinct quads.

---

## 10. Injected metadata (optional features)

All optional features below materialize as **ordinary quads in reserved graphs** — they flow through
the same dictionaries and indexes as source data and need no special storage. Reserved namespace:
`urn:x-beakgraph:*`.

### 10.1 VoID statistics graph

When enabled (`-void` / `-voidsketch`; default off), the writer accumulates dataset statistics and
appends them as triples in named graph **`urn:x-beakgraph:void`** (VoID vocabulary + SPARQL
service-description terms; content is ordinary RDF and not otherwise constrained by this spec).
These quads are indexed but not counted in `numQuads`.

### 10.2 Spatial index (GeoSPARQL `geof:sfIntersects` acceleration)

Enabled per build (`setSpatial(true)`). For every source quad whose **object is a literal of
datatype `http://www.opengis.net/ont/geosparql#wktLiteral`** (any predicate; call its subject `S`),
the writer emits derived quads. A CRS prefix (`<uri> WKT…`) is stripped before parsing.

**(A) Hilbert cell entries — the query contract.** In graph `urn:x-beakgraph:Spatial`:

```
S  <https://halcyon.is/ns/hilbertCell{s}>  "<cellId>"^^xsd:long .
```

computed per polygonal part of the geometry (each polygon of a multi/collection separately;
non-areal members via their envelope expanded by 0.5 on every side):

1. Hilbert curve: **2 dimensions × 31 bits**, the Skilling/Hamilton algorithm exactly as
   implemented by `com.github.davidmoten:hilbert-curve:0.2.3` — indices must be bit-identical.
   Domain per axis `[0, 2^31)`; coordinates are clamped into it (`max(0, min(2^31−1, v))`).
2. Bbox: `minX/maxX/minY/maxY = clamp(floor(envelope bound))`.
3. Scale: the smallest `s ∈ [0, 30]` such that the bbox is covered by at most **16** cells of side
   `2^s` (cell counts shrink as `s` grows, so scales are tried from `s = 0` upward until the cover
   fits), where the cover cell count is
   `(floorDiv(maxX,2^s) − floorDiv(minX,2^s) + 1) × (same for Y)`.
4. Cells: for every `(x, y)` with `x ∈ [floorDiv(minX,2^s), floorDiv(maxX,2^s)]` (resp. Y),
   `cellId = hilbertIndex31(x, y)` — the **same 31-bit curve** applied to the floor-divided
   coordinates (not a lower-order curve). One quad per distinct cell id.
5. The object literal MUST be `xsd:long` **by value** (§9.3 applies), because the query side pushes
   numeric range filters (`≥ low`, `≤ high` per Hilbert range) into the GPOS object level.

Recall safety: writer and query snap with the identical floor+clamp, so any two overlapping boxes
share a whole cell at every scale; false positives are eliminated at query time by exact JTS
intersection against the **original** WKT literal — which is therefore load-bearing and must be
stored verbatim (it is, as ordinary data).

**(B/C) Multi-resolution pyramid (viewer support, not queried by the engine).** Per pyramid level
`s` (0…14): the geometry is repeatedly half-scaled (`×0.5` per level, integer-snapped, cleaned,
stopping at area < 4.0 or 20 levels), and each level's WKT is written as
`S <https://halcyon.is/ns/asWKT{s}> "<wkt>"^^geo:wktLiteral` into graph `urn:x-beakgraph:Spatial`
**and** into every grid-tile graph `urn:x-beakgraph:grid:{s}:{x}:{y}` whose 512×512 tile (in
level-`s` coordinates, `x = floor(coord/512)`) the level-`s` polygon intersects.

**(D) Shape features** (separately enabled): derived scalar features in the **default graph** —
pyradiomics-style predicates under `https://halcyon.is/ns/pyr/shape2d/` (`MeshSurface`,
`Perimeter`, `Sphericity`, …) as `xsd:double`, and `https://halcyon.is/ns/`
`centroid`/`majorAxis`/`minorAxis` as WKT literals (`POINT`/`LINESTRING`, 4-decimal formatting).
Some feature values depend on Java2D rasterization and are not bit-reproducible cross-platform;
treat them as informational.

**Legacy note:** files written before the spatial redesign contain corner-based entries
(`hal:hilbertCorner`-era vocabulary) that current readers ignore; do not emit them.

### 10.3 Reserved vocabulary summary

| IRI | Role |
|---|---|
| `urn:x-arq:DefaultGraph` | default graph name (§9.1) |
| `urn:x-beakgraph:void` | VoID metadata graph |
| `urn:x-beakgraph:Spatial` | spatial index graph |
| `urn:x-beakgraph:grid:{level}:{x}:{y}` | spatial tile graphs |
| `https://halcyon.is/ns/hilbertCell{s}` | spatial cell predicates |
| `https://halcyon.is/ns/asWKT{s}` | pyramid predicates |

### 10.4 Reader behaviors a file must support (informative)

These reader features rely on format invariants already stated; they are listed so a validator knows
what to exercise: term `locate` via §6 binary search (symmetric with `extract`); `select1`-addressed
run traversal (§8.5); numeric range pushdown over value-equal clusters (§6.4 adjacency); graph
enumeration and membership via the `graphs` list; union-graph scans; HTTP range reading (a
consequence of the contiguous-dataset profile, §3 — no format work needed).

---

## 11. Conformance checklist for a new implementation

A writer is conformant when the reference stack (0.18.0+) can open its files and every query answers
as over a reference-built store. Practical gates, in order:

1. **Primitives round-trip**: bit-packed sequences (incl. widths 32/64 signed cases and the
   final-byte padding), VByte, FCD blocks (incl. UTF-16 prefix lengths, surrogate backoff, the
   ≥ 64-byte zstd rule, and the per-entry `compressed` bits).
2. **Comparator conformance**: sort a stress set of terms (mixed kinds; the §6.2.3 temporal traps —
   timezone-less vs timezoned, `P1D` vs `PT24H`; value-equal cross-datatype numerics; lang/dir
   variants; CDT lexical variants like `[1, 2]` vs `[1,2]`; ill-typed literals) and compare the
   permutation against the reference (Appendix A.4 provides an anchor). Shuffled inputs must sort
   identically — any instability means the order is not total.
3. **Dictionary parity**: for a fixture covering every row of the §7.3 routing table, the reference
   reader must extract, for every id, exactly the expected term (term set equality, not just
   count — value canonicalization (§9.3) included).
4. **Index parity**: patterns with every combination of bound/unbound G/S/P/O (through both indexes)
   over graphs including an empty-padded region, plus a numeric range `FILTER`, answer identically.
5. **Round-trip isomorphism**: source dataset ≅ exported dataset (blank-node isomorphism, §7.6).
6. The reference CLI's `-verify` / `-verify -deep` command validates structural invariants of a
   finished file and is the cheapest end-to-end oracle.

---

## Appendix A — worked example

Source (TriG; 24 quads — 21 default graph, 2 in `ex:g1`, 1 in a bnode-named graph):

```trig
@prefix ex:  <http://ex/> .   @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix cdt: <http://w3id.org/awslabs/neptune/SPARQL-CDTs/> .
ex:s1 ex:p ex:o1 .            ex:s1 ex:p _:shared .
_:shared ex:q "plain" .       <> ex:rel <sib.png> .
ex:s1 ex:num "42"^^xsd:int .  ex:s1 ex:num "042"^^xsd:int .      # collapse to one term
ex:s1 ex:num "-7"^^xsd:int .  ex:s1 ex:num "9000000000"^^xsd:long .
ex:s1 ex:num "2.5"^^xsd:float .   ex:s1 ex:num "1.5E0"^^xsd:double .
ex:s1 ex:num "12345678901234567890123"^^xsd:integer .
ex:s1 ex:num "3.14"^^xsd:decimal .   ex:s1 ex:str "typed"^^xsd:string .
ex:s1 ex:lang "hello"@en .    ex:s1 ex:lang "hello"@en-US .
ex:s1 ex:lang "hello"@en--ltr .    ex:s1 ex:lang "hello"@en--rtl .
ex:s1 ex:b "true"^^xsd:boolean .   ex:s1 ex:dt "2024-01-02T03:04:05Z"^^xsd:dateTime .
ex:s1 ex:list '[1, 2, 3]'^^cdt:List .   ex:s1 ex:map '{"k": 5}'^^cdt:Map .
ex:g1 { ex:s2 ex:p _:shared .  ex:s2 ex:p ex:o1 . }
_:bg  { ex:s3 ex:p "in-bnode-graph" . }
```

### A.1 File inventory (as built by the reference writer)

`.BG` attrs: `numQuads=24`, `formatVersion=4` — the dump was captured at v4; current builds stamp
`5` and are otherwise byte-identical for this triple-term-free fixture. Dataset shapes/attrs
abridged to the noteworthy:

```
dictionary/entities:    offsets(w=5,n=10) datatypes(w=5,n=10) iri(FCD n=8)
dictionary/predicates:  offsets(w=5,n=10) datatypes(w=5,n=10) iri(FCD n=10)
dictionary/literals:    offsets(w=6,n=18) datatypes(w=5,n=18) typedLiterals(w=5,n=18)
                        langTags(w=3,n=18) langDirs(w=3,n=18)
                        integers(w=32,n=2) longs(w=35,n=1) floats(n=1) doubles(n=1)
                        strings(FCD n=13) typedLiteralsDictionary(FCD n=13) langs(FCD n=2)
dictionary/graphs(w=8,n=3)=[1,3,5]  subjects(w=8,n=5)=[2,4,7,8,9]
dictionary/objects(w=8,n=21)=[2,6,10,11,…,28]
GSPO: Bs(n=12) Ss(w=8,n=12) Bp(n=19) Sp(w=8,n=19) Bo(n=30) So(w=8,n=30) + SB*/BB*(seed 0)
GPOS: Bp(n=19) Sp(w=8,n=19) Bo(n=30) So(w=8,n=30) Bs(n=30) Ss(w=8,n=30) + directories
```

### A.2 Id assignments (the §6 order in action)

```
entities:  E1  urn:x-arq:DefaultGraph      (sentinel first)
           E2  _:…002   E3  _:…003         (bnodes; labels regenerated from id)
           E4  ""                          (relative <>; empty string sorts before "http…")
           E5  http://ex/g1  E6 …/o1  E7 …/s1  E8 …/s2  E9 …/s3
           E10 sib.png                     (relative, "s" > "h")
predicates: P1 …/b  P2 …/dt  P3 …/lang  P4 …/list  P5 …/map  P6 …/num  P7 …/p
            P8 …/q  P9 …/rel  P10 …/str
literals (object ids 11–28):
  L1–L3   "in-bnode-graph","plain","typed"      xsd:string        (string space first)
  L4–L7   "hello"@en, @en--ltr, @en--rtl, @en-US       ((lang,lex,dir); "en" < "en-US")
  L8–L14  -7^^int < 1.5^^double < 2.5^^float < 3.14^^decimal < 42^^int
          < 9000000000^^long < 1234…123^^integer        (one numeric space, by value)
  L15     true^^boolean      L16  2024-01-02T03:04:05Z^^dateTime
  L17     [1, 2, 3]^^cdt:List     L18  {"k": 5}^^cdt:Map
```

Note `"042"^^xsd:int` produced no entry — canonicalization (§9.3) collapsed it into L12, leaving 23
unique quads for the indexes.

### A.3 Column and store bytes (verified)

```
literals/datatypes    : 2 2 2 2 2 2 2 6 9 8 2 6 7 2 2 2 2 2      (STRING…, INTEGER, DOUBLE, FLOAT, …)
literals/offsets      : 0 1 2 3 4 5 6 0 0 0 7 1 0 8 9 10 11 12   (per-store running indexes)
literals/typedLiterals: 13 13 13 4 3 3 4 10 8 9 7 10 12 11 5 6 1 2
   (typedLiteralsDictionary, string-sorted: 1=cdt List, 2=cdt Map, 3=rdf dirLangString,
    4=rdf langString, 5=xsd boolean, 6=dateTime, 7=decimal, 8=double, 9=float, 10=int,
    11=integer, 12=long, 13=string)
literals/langTags     : 0 0 0 1 1 1 2 0 …     (langs = ["en","en-US"])
literals/langDirs     : 0 0 0 0 1 2 0 0 …     (ltr=1 on L5, rtl=2 on L6)
literals/integers     : ff ff ff f9 | 00 00 00 2a        (-7, 42; width 32, two's complement)
literals/floats       : 40 20 00 00                      (2.5f, IEEE BE)
literals/doubles      : 3f f8 00 00 00 00 00 00          (1.5, IEEE BE)
literals/longs        : 43 0e 23 40 00                   (9000000000 in 35 bits, MSB-first, padded)
entities/iri stringbuffer: 96 "urn:x-arq:DefaultGraph" · 80 80 ("") · 80 8c "http://ex/g1"
    · 8a 82 "o1" · 8a 82 "s1" · 8b 81 "2" · 8b 81 "3" · 80 87 "sib.png"     (front-coding)
```

### A.4 GSPO decode (padding and runs visible)

10 entities ⇒ L0 padded to 10 graph slots (real graphs: 1, 3, 5):

```
Bs: 1 0 0 1 1 1 1 1 1 1 1 1
Ss: 2 4 7 0 9 0 8 0 0 0 0 0
     └G1: subjects {2,4,7}┘ G2:dummy G3:{9} G4:dummy G5:{8} G6–G10: dummies
Bp: 1 1 1 0 0 0 0 0 0 0 1 1 1 1 1 1 1 1 1
Sp: 8 9 1 2 3 4 5 6 7 10 0 7 0 7 0 0 0 0 0
     (G1,S2):{8} (G1,S4):{9} (G1,S7):{1,2,3,4,5,6,7,10} dummy (G3,S9):{7} dummy (G5,S8):{7} dummies
So run for (G1, S7=ex:s1, P6=ex:num): 18 19 20 21 22 23 24   ← ascending literal object ids
```

Total L3 rows = 23 unique quads + 7 dummy rows = 30. The cross-space literal rank order of §6.2.4
was verified by sorting a 29-literal stress set with the reference comparator; the observed
sequence is the one given there.

---

## Appendix B — constants

| Constant | Value |
|---|---|
| Root group name | `.BG` (leading dot) |
| Dictionary group name | `dictionary` |
| `FORMAT_VERSION` | 5 |
| `RANK_DIRECTORY_MIN_VERSION` | 3 |
| Index superblock / block size | 512 / 64 bits |
| FCD `blockSize` | 16 |
| FCD `compression_threshold` | 64 bytes |
| Bit-packed legal widths | 1–57, 64 |
| Blank-node label pattern | `b%020d` (id, zero-padded to 20 digits) |
| Relative-IRI sentinel base | `http://beakgraph.invalid/` + `d/`×32 + `%00` (`RelativeIris.SENTINEL_BASE`) |
| Spatial: Hilbert curve | 2 dims × 31 bits (davidmoten hilbert-curve 0.2.3 semantics) |
| Spatial: max cells / max scale | 16 / 30 |
| Spatial: grid tile size | 512 |
| Spatial: pyramid levels | ≤ 15 (`asWKT0…14`), stop at area < 4.0 or 20 halvings |

## Appendix C — what is deliberately NOT stored

* Blank-node labels (§7.6) — including inside triple terms, where components are structural ids.
* Non-canonical lexical forms of `xsd:int`/`long`/`float`/`double` object literals (§9.3) —
  including inside triple terms.
* Any textual serialization of triple terms — they are stored structurally by component id
  (§7.6a), never as `<<( … )>>` text.
* Prefixes/namespaces, base IRIs, and any parser-level syntax (comments, `@version`, …).
* Term→id hash tables (ids are ranks; lookup is comparison-based search).
* Any per-quad provenance or ordering beyond the two index sort orders; a quad is either present
  once or absent.
