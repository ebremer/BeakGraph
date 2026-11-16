---
title: Architecture
nav_order: 4
has_children: true
permalink: /architecture/
description: How a BeakGraph store is laid out, read and written - format v6, the HDF5 and Zarr containers, and the six writers.
---

# BeakGraph stores
{: .fs-9 }

Architecture of the on-disk format, the read path and the writers
{: .fs-6 .fw-300 }

Dictionary-encoded, columnar RDF quad stores — SPARQL-queryable at rest, in an HDF5 file or a Zarr
store.

[Start: What a BeakGraph store is →](what-a-beakgraph-file-is/){: .btn .btn-primary }

{: .note }
These pages describe BeakGraph 0.20.0: format version 6, two containers (HDF5 and Zarr) and six
writers, one topic per page. They began as the architecture slides of format version 3 (the deck,
`BeakGraph-HDF5-Architecture.pptx`, remains in the repository's git history). They are an overview:
the [file format specification](../specification/) is normative, and the
[changelog](../changelog/) has the version history.
