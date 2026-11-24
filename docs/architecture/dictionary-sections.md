---
title: Inside a dictionary section
parent: Architecture
nav_order: 5
permalink: /architecture/dictionary-sections/
---

# Inside a dictionary section

Columnar node encoding (entities / predicates / literals)
{: .fs-6 .fw-300 }

Per node, in sorted order — every column that exists has one entry per node:

| Node | `datatypes[i]` | `offsets[i]` | Value storage |
|---|---|---|---|
| IRI | `IRI` / `RELATIVE_IRI` | → `iri` | front-coded (FCD) string blocks |
| blank node | `BNODE` | `0` | nothing! label regenerated from rank |
| `xsd:int` / `long` | `INTEGER` / `LONG` | → `integers` / `longs` | bit-packed values (two's complement at width 32 / 64) |
| `xsd:float` / `double` | `FLOAT` / `DOUBLE` | → `floats` / `doubles` | raw IEEE buffers |
| any other literal | `STRING` | → `strings` | FCD blocks + `typedLiterals` (datatype id) |
| language-tagged | `STRING` | → `strings` | + `langTags` (id into `langs`); + `langDirs` for `"x"@en--ltr` (v4) |
| RDF 1.2 triple term | `TRIPLE_TERM` | its ordinal *k* | `tripleTerms[3k..3k+2]`: the (s, p, o) component ids (v5) |
| RDF-array sequence | `SEQUENCE` | its ordinal *k* | `sequenceElements[starts[k]..starts[k+1])`: element ids, 0 for a slot (v7) |

- Front-coded dictionaries (FCD): sorted strings share prefixes; every 16th string is stored whole,
  the rest as (shared-prefix length in UTF-16 units, suffix); a fragment of 64 bytes or more is
  zstd-compressed, with one bit per entry saying which
- Numeric literals are stored by VALUE and re-canonicalized at ingest, so `"01"^^xsd:int` and
  `"1"^^xsd:int` collapse to one entry; `xsd:integer` and `xsd:decimal` are unbounded and stay strings
- Blank nodes cost zero bytes of text: the format stores only their rank; readers mint labels from
  ids (output is isomorphic, labels are not data)
- Triple terms are stored structurally, never as text: a nested one is an object id into the same
  band and resolves recursively, and a blank node inside one is an ordinary entity id, so
  co-reference survives
- Sequences likewise (v7): the elements are object ids in two block-structured arrays,
  `sequenceStarts` and `sequenceElements`, with 0 for an empty slot (`undef`). A nested sequence is
  a row of its own, so one that recurs is stored once. The sequences are the section's last rows, so
  two compare as their element-id arrays do, and a reader looks one up on ids. A stored sequence
  is read lazily: its length comes from the starts, each element when it is asked for
- VoID + SPARQL Service Description statistics can be embedded as ordinary quads in graph
  `urn:x-beakgraph:void` (`-void` exact / `-voidsketch` HyperLogLog; off by default - readers then
  use a fixed join-reorder heuristic)

---

[← Previous: Dictionary design](../dictionary-design/){: .btn } [Next: GSPO / GPOS indexes →](../indexes/){: .btn .btn-primary }
