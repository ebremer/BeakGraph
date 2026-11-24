---
title: File layout
parent: Architecture
nav_order: 2
permalink: /architecture/file-layout/
---

# File layout

Groups and arrays inside a store — format version 7
{: .fs-6 .fw-300 }

```text
/.BG                          attrs: numQuads, formatVersion (=7)
 |- dictionary/
 |   |- entities/             IRIs + blank nodes used as G, S or non-literal O
 |   |- predicates/           predicate IRIs
 |   |- literals/             every literal object, then every RDF 1.2 triple term, then every sequence
 |   |- graphs    graphs_blocks       the DISTINCT sorted ids used in each
 |   |- subjects  subjects_blocks     position, block-structured, with their
 |   |- objects   objects_blocks      directories (graph enumeration, membership)
 |- GSPO/                     index ordered graph > subject > predicate > object
 |   |- Bs  Ss Ss_blocks  SBs BBs     level 1 (subject layer)
 |   |- Bp  Sp Sp_blocks  SBp BBp     level 2 (predicate layer)
 |   |- Bo  So So_blocks  SBo BBo     level 3 (object layer)
 |- GPOS/                     index ordered graph > predicate > object > subject
     |- Bp  Sp Sp_blocks  SBp BBp
     |- Bo  So So_blocks  SBo BBo
     |- Bs  Ss Ss_blocks  SBs BBs
```

- Every packed integer array carries `numEntries` and `width` (on a block-structured one, its
  widest block's); the raw value arrays (`floats`, `doubles`, an FCD's `offsets`) carry `numEntries`
  alone. A block-structured array also carries `blockEntries` — its presence is what marks the
  form — and has its `_blocks` directory beside it ([Integer sequences](../bit-packed-buffers/))
- A section holds per-node columns (`datatypes`, `offsets`; in `literals` also `typedLiterals`,
  `langTags`, `langDirs`) and value stores (`iri`, `strings`, `integers`, `longs`, `floats`,
  `doubles`, `tripleTerms`, `sequenceStarts`, `sequenceElements`, `typedLiteralsDictionary`,
  `langs`), each present only when used
  ([Inside a dictionary section](../dictionary-sections/))
- Readers find features by presence: no language tags → no `langs` / `langTags`; no triple terms → no
  `tripleTerms`; no sequences → no `sequenceStarts` / `sequenceElements`. `formatVersion` only stops
  older readers from misreading newer stores
- In Zarr the same tree is a hierarchy of groups and `uint8` arrays: `.BG/GSPO/So` is
  `.BG/GSPO/So/zarr.json` plus its chunk `.BG/GSPO/So/c/0` — about 60 arrays, some 130 files, with the
  metadata of all of them consolidated into the root `zarr.json`

---

[← Previous: What a BeakGraph store is](../what-a-beakgraph-file-is/){: .btn } [Next: Containers →](../containers/){: .btn .btn-primary }
