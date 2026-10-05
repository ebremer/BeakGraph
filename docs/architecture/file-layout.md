---
title: File layout
parent: Architecture
nav_order: 2
permalink: /architecture/file-layout/
---

# File layout

Groups and datasets inside the `.h5` container
{: .fs-6 .fw-300 }

```text
/.BG                          attrs: numQuads, formatVersion (=3)
 |- dictionary/
 |   |- entities/             URIs + blank nodes from G, S, and entity O
 |   |- predicates/           predicate URIs
 |   |- literals/             every literal object
 |   |- graphs                (columnar id lists: the DISTINCT sorted ids
 |   |- subjects               used in each quad position - drives fast
 |   |- objects                enumeration and cardinality answers)
 |- GSPO/                     index ordered graph > subject > predicate > object
 |   |- Ss  Bs   SBs  BBs     level 1 (subject layer)
 |   |- Sp  Bp   SBp  BBp     level 2 (predicate layer)
 |   |- So  Bo   SBo  BBo     level 3 (object layer)
 |- GPOS/                     index ordered graph > predicate > object > subject
     |- Sp  Bp   SBp  BBp
     |- So  Bo   SBo  BBo
     |- Ss  Bs   SBs  BBs
```

Every dataset is a bit-packed buffer with two attributes: `width` (bits per entry) and `numEntries`.

---

[← Previous: What a BeakGraph file is](../what-a-beakgraph-file-is/){: .btn } [Next: Dictionary design →](../dictionary-design/){: .btn .btn-primary }
