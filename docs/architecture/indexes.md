---
title: GSPO / GPOS indexes
parent: Architecture
nav_order: 6
permalink: /architecture/indexes/
---

# GSPO / GPOS indexes

A three-level id trie in flat arrays (HDT-style)
{: .fs-6 .fw-300 }

Quads sorted by `(g,s,p,o)`:

```text
g=1: (s=2,p=1,o=7) (s=2,p=3,o=4) (s=5,p=1,o=7)     g=3: (s=2,p=1,o=9)

level 1  Ss = 2 5 | 2         one entry per distinct (g,s);  Bs bit=1 marks a new g
level 2  Sp = 1 3 1 | 1       one entry per distinct (g,s,p); Bp bit=1 marks a new (g,s)
level 3  So = 7 4 7 | 9       one entry per quad;             Bo bit=1 marks a new (g,s,p)
```

Every entity id with no quads as a graph is padded with one dummy row (S=0, B=1) per level, so graph
g's region is always addressable as `select1(Bs, g)`.

- S sequences hold ids (block-structured since format v6, each with its `_blocks` directory); B
  bitmaps delimit parent boundaries — together they encode the trie with zero pointers
- Navigation: the children of prefix #k live between `select1(B, k)` and the next set bit; each
  level's range is then binary-searched for the wanted id (ids are sorted within a parent)
- Duplicates were removed during the build scan; there is no object-first index

| Bound | Index | Cost |
|---|---|---|
| S, P (O free or bound) | GSPO | one probe, then the O run |
| P, O | GPOS | one probe, then the S run |
| P only | GPOS | the predicate's O/S sub-tree |
| S only | GSPO | the subject's P/O sub-tree |
| O only | GPOS | one (g, p, o) probe per predicate of the graph |
| none | GSPO | a scan of the graph |

The graph is always bound inside an index; `GRAPH ?g` walks the `graphs` list.

---

[← Previous: Inside a dictionary section](../dictionary-sections/){: .btn } [Next: Rank / select directories →](../rank-select/){: .btn .btn-primary }
