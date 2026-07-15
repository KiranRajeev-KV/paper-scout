# Paper Scout output example

This directory is a compact, self-contained example of a completed Paper Scout
run. It is based on the multimodal RAG hallucination run from July 2026, but is
deliberately abbreviated so the repository does not store a large generated
snapshot.

- `report.md` is the human-readable report.
- `result.json` is the corresponding structured result.
- `references.bib` contains the BibTeX entries cited by both files.

The UUIDs in the report are BibTeX entry keys. Validate the bibliography with:

```sh
biber --tool --validate-datamodel examples/references.bib
```
