# Recorded benchmark results

Committed JMH runs are reference points, not a shared scratch area: every file
here is named `<date>-<commit>-<what>.json` and lists the machine below, so a
number can always be traced to the code and hardware that produced it. Local
before/after runs go to `results/local/` (git-ignored) - see the parent README,
"Baseline discipline".

| File | Commit | Built against | Machine | Notes |
|---|---|---|---|---|
| `2026-07-16-df27406-rdf12-baseline.json` | `df27406` (master before the RDF 1.2 / lexical-CDT dictionary work) | BeakGraph 0.17.0 | maintainer laptop (see `vmVersion` inside the file) | Read-path baseline the RDF 1.2 work was measured against. Predates the exact numeric order and the sentinel-base relative-IRI change, so compare it only with runs of that era. |
