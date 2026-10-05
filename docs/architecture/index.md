---
title: Architecture
nav_order: 4
has_children: true
permalink: /architecture/
description: The architecture of the BeakGraph on-disk format, from the original design slides.
---

# BeakGraph HDF5 Files
{: .fs-9 }

Architecture of the on-disk format
{: .fs-6 .fw-300 }

Dictionary-encoded, columnar RDF quad stores — SPARQL-queryable at rest.

[Start: What a BeakGraph file is →](what-a-beakgraph-file-is/){: .btn .btn-primary }

{: .note }
These pages are the original architecture slides as web pages, one page per slide (the deck,
`BeakGraph-HDF5-Architecture.pptx`, remains in the repository's git history). They describe format version 3 and the five writers of that time. Since then,
format v4 added the `langDirs` dataset for RDF 1.2 base-direction literals, v5 the `tripleTerms`
component store for RDF 1.2 triple terms, and `-method 5` (plaid) joined the writers. The
[file format specification](../specification/) is normative; the [changelog](../changelog/) has
the version history.
