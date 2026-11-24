---
title: Dictionary design
parent: Architecture
nav_order: 4
permalink: /architecture/dictionary-design/
---

# Dictionary design

One strict total order does all the work
{: .fs-6 .fw-300 }

- **`NodeComparator`: a strict total order over RDF terms**
  - Default-graph sentinel first, then blank node < IRI < literal < triple term < sequence; literals ordered by
    VALUE (2 < 10, across the numeric datatypes), with exact-term tie-breaks so distinct terms get
    distinct positions
  - Strings compare in UTF-16 code-unit order (Java's `String.compareTo`) — an implementation in
    another language must reproduce it to assign the same ids
- **An id IS a rank**
  - Sort the distinct terms of a section; a term's 1-based position is its id. `locate()` = binary
    search, re-materializing candidates and comparing them in the same order; `extract()` = indexed
    read. No term → id table in the store
- **Three sections, two id spaces**
  - entities (IRIs + blank nodes usable as G/S/O) → ids 1..E; predicates → ids 1..P, kept apart so
    predicate ids stay narrow
  - objects: entities keep 1..E, literals follow as E+1..E+L, with RDF 1.2 triple terms and then
    RDF-array sequences (v7) as the literals' last rows — matching the macro order, so sorting quads
    by id tuple equals sorting them by term
- **Value order is what range filters ride on**: value-equal literals (`"5"^^xsd:int`,
  `"5.0"^^xsd:double`) sit side by side, so `FILTER(?x > 5)` becomes a contiguous id range
  (`ValueCluster`)
- **That id/rank equivalence is what lets the fast writers sort packed integer keys instead of
  comparing nodes — provably the same order**

---

[← Previous: Containers](../containers/){: .btn } [Next: Inside a dictionary section →](../dictionary-sections/){: .btn .btn-primary }
