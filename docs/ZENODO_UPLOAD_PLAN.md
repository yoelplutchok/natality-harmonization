# Zenodo Upload Plan (archival)

**Status**: completed 2026-04-08 with the minting of DOI **[10.5281/zenodo.19363075](https://doi.org/10.5281/zenodo.19363075)**.

This document previously contained a step-by-step plan for preparing the first Zenodo upload. The plan has been executed; the DOI is live. Everything below is kept for provenance.

## Current shipped schema (as of 2026-04-22)

- **V2 natality**: 71 harmonized columns + 13 derived = **84 columns**; 138,819,655 rows (1990–2024).
- **V3 linked**: 78 harmonized columns + 16 derived = **94 columns**; 74,943,824 rows (2005–2023).
- Pipeline validates: 38/38 invariants pass; 183/183 V2 external targets pass; 35/35 V3 linked external targets pass.

## Canonical current references

- **README.md** — headline numbers and Zenodo DOI badge.
- **docs/ABOUT_THIS_RELEASE.md** — release summary, what V2.5 added.
- **docs/CODEBOOK.md** — column-level schema (source of truth).
- **metadata/harmonized_schema.csv** — machine-readable schema (source of truth).
- **docs/COMPARABILITY.md** — cross-year comparability rules and known pitfalls.
- **docs/VALIDATION.md** — validation targets and outputs.
- **FIX_LOG.md** + `FRESH_AUDIT_REPORT.md` + `AUDIT_REPORT_NEW.md` + `AUDIT_SYNTHESIS.md` — 2026-04-22 audit cycle.

## For future uploads (V2.6+ / bug-fix releases)

If you need to upload a revised version:

1. Update the schema in code (`scripts/03_harmonize/harmonize_v1_core.py` `out_schema` and `scripts/03_harmonize/harmonize_linked_v3.py` `OUT_SCHEMA`).
2. Re-run the full pipeline per the commands in `README.md`'s "Quick reproduce" section.
3. Run all validators and confirm invariants/targets still pass.
4. Update `docs/ABOUT_THIS_RELEASE.md` with the new release notes and bump the column/row counts in README if they changed.
5. Update `.zenodo.json` `version` if present (currently not tracked).
6. Tag and push; Zenodo's GitHub integration will create a new version of the record.
7. After the DOI is minted, update README and `docs/ABOUT_THIS_RELEASE.md` with the new DOI.
