---
title: Inside a dictionary section
parent: Architecture
nav_order: 4
permalink: /architecture/dictionary-sections/
---

# Inside a dictionary section

Columnar node encoding (entities / predicates / literals)
{: .fs-6 .fw-300 }

Per node, in sorted order:

| Node | `datatypes[i]` | `offsets[i]` | Value storage |
|---|---|---|---|
| URI | `IRI` / `REL_IRI` | → `iri` | front-coded (FCD) string blocks |
| blank node | `BNODE` | `0` | nothing! label regenerated from rank |
| `xsd:int` / `long` | `INTEGER` / `LONG` | → `integers` / `longs` | bit-packed values |
| `xsd:float` / `double` | `FLOAT` / `DOUBLE` | → `floats` / `doubles` | raw IEEE buffers |
| any other literal | `STRING` | → `strings` | FCD blocks + `typedLiterals` (datatype id) |
| language-tagged | `STRING` | → `strings` | + `langTags` (id into `langs` dictionary) |

- Front-coded dictionaries (FCD): sorted strings share prefixes; every 16th string is stored whole,
  the rest as (shared-prefix length, suffix)
- Numeric literals are stored by VALUE and re-canonicalized at ingest, so `"01"^^xsd:int` and
  `"1"^^xsd:int` collapse to one entry
- Blank nodes cost zero bytes of text: the format stores only their rank; readers mint labels from
  ids (output is isomorphic, labels are not data)
- VoID + SPARQL Service Description statistics can be embedded as ordinary quads in graph
  `urn:x-beakgraph:void` (`-void` exact / `-voidsketch` HyperLogLog; off by default - readers then
  use a fixed join-reorder heuristic)

---

[← Previous: Dictionary design](../dictionary-design/){: .btn } [Next: GSPO / GPOS indexes →](../indexes/){: .btn .btn-primary }
