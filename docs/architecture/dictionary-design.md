---
title: Dictionary design
parent: Architecture
nav_order: 3
permalink: /architecture/dictionary-design/
---

# Dictionary design

One strict total order does all the work
{: .fs-6 .fw-300 }

- **`NodeComparator`: a strict total order over RDF terms**
  - Default-graph sentinel first, then BNode < URI < Literal; literals ordered by VALUE (2 < 10),
    with exact-term tie-breaks so distinct terms get distinct positions
- **An id IS a rank**
  - Sort the distinct terms of a section; a term's 1-based position is its id. `locate()` = binary
    search; `extract()` = indexed read. No hash tables in the file
- **Three sections, two id spaces**
  - entities (URIs + bnodes usable as G/S/O) → ids 1..E; predicates → ids 1..P
  - objects: entities keep 1..E, literals follow as E+1..E+L — matching the macro order, so sorting
    quads by id tuple equals sorting them by term
- **That id/rank equivalence is what lets the fast writers sort packed integer keys instead of
  comparing nodes — provably the same order**

---

[← Previous: File layout](../file-layout/){: .btn } [Next: Inside a dictionary section →](../dictionary-sections/){: .btn .btn-primary }
