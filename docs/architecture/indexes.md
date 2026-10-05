---
title: GSPO / GPOS indexes
parent: Architecture
nav_order: 5
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

Empty graph ids are padded with one dummy row (S=0, B=1) per level, so graph gi's region is always
addressable as `select1(Bs-level, gi)`.

- S buffers hold ids; B bitmaps delimit parent boundaries — together they encode the trie with zero
  pointers
- Navigation: the children of prefix #k live between `select1(B, k)`+adjacent boundaries; each
  level's range is then binary-searched for the wanted id (ids are sorted within a parent)
- GSPO answers `(g,s,?,?)`-shaped patterns; GPOS answers `(g,?,p,o)`-shaped ones; duplicates were
  removed during the build scan

---

[← Previous: Inside a dictionary section](../dictionary-sections/){: .btn } [Next: Rank / select directories →](../rank-select/){: .btn .btn-primary }
