# U.S. Natality Harmonization Project

A modern, researcher-ready release of harmonized U.S. natality microdata for cross-year analysis.

## Scope

Harmonized U.S. natality microdata for **1990–2024** with a stable 61-column harmonized schema (71 with derived indicators) covering maternal and paternal demographics, prenatal care, medical risk factors, congenital anomalies, infections, fertility treatment, and birth outcomes, plus documentation and validation.

## Raw data

**NCHS public-use period natality** files: `Nat{year}.zip` (1990–1993) and `Nat{year}us.zip` (1994–2023), with the annual documentation PDF as the layout authority. Full rationale, URLs, and caveats: **[docs/DATA_SOURCE_V1.md](docs/DATA_SOURCE_V1.md)**.

## Current status

- **Downloads:** `raw_data/` contains all 35 natality zips (1990–2024); `raw_docs/` contains documentation PDFs; `metadata/file_inventory.csv` tracks URLs, filenames, and import status.
- **Import:** `scripts/01_import/parse_all_v1_years.py` produces `output/yearly_clean/natality_{year}_core.parquet` (raw substrings). Handles five record layouts (350/1350/1500/775/1345 bytes). `7z` on PATH for deflate64/PPMd zips (2009–2013, 2015).
- **Harmonized (1990–2024):** complete — `output/harmonized/natality_v2_harmonized.parquet` + `natality_v2_harmonized_derived.parquet`.
- **Validation:** all invariant checks pass (0 violations); 183/183 external targets pass (1990–2024); key rates plausible across all 35 years. See `output/validation/`.
- **V3 Linked (2005–2023):** `output/harmonized/natality_v3_linked_harmonized_derived.parquet` — 74.9M birth records with linked infant death data (IMR, cause of death, neonatal/postneonatal). Validated against NCHS linked file user guides for 2005, 2010, 2015, 2020, 2021, 2022, and 2023 (35/35 external targets pass; IMR trend 6.74→5.35).

## Headline metrics

- **Years covered**: 1990–2024 (35 years natality); 2005–2023 (19 years linked birth-infant death)
- **Birth records**: 138,819,655 total (natality V2); 74,943,824 (linked V3)
- **Harmonized columns**: 67 natality + 10 derived = 77 (V2); 74 + 13 derived = 87 including death-side (V3 linked)
- **Convenience outputs**: `output/convenience/` — residents-only subsets (134.95M V2, 63.86M V3 linked)
- **External validation**: `output/validation/external_validation_v1_comparison.csv` (183 targets, 1990–2024, all pass); linked V3: 35/35 targets pass (2005/2010/2015/2020-2023 user guides — births, deaths, IMR, neonatal/postneonatal deaths and IMR)

## Repository structure

```
natality-harmonization/
├── README.md
├── raw_data/              # source files (not committed if large; use inventory)
├── raw_docs/              # PDFs, guides from NCHS / state sources
├── metadata/              # CSV tracking sheets (source of truth for harmonization)
├── scripts/
│   ├── 01_import/
│   ├── 02_clean_yearly/
│   ├── 03_harmonize/
│   ├── 04_derive/
│   └── 05_validate/
├── output/
│   ├── yearly_clean/
│   ├── harmonized/
│   └── validation/
├── notebooks/
└── docs/                  # human-facing markdown (codebook, FAQ, etc.)
```

## Progress checklist

See [docs/MILESTONES.md](docs/MILESTONES.md) for the full Version 1 milestone ladder.

## Quick links (docs to be filled as you build)

| Document | Purpose |
|----------|---------|
| [docs/ABOUT_SOURCE_DATA.md](docs/ABOUT_SOURCE_DATA.md) | What natality files are and why they matter |
| [docs/ABOUT_THIS_RELEASE.md](docs/ABOUT_THIS_RELEASE.md) | What this project adds vs. raw NCHS files |
| [docs/CODEBOOK.md](docs/CODEBOOK.md) | Variable-level definitions |
| [docs/COMPARABILITY.md](docs/COMPARABILITY.md) | Cross-year comparability |
| [docs/VALIDATION.md](docs/VALIDATION.md) | Checks vs. official tabulations |
| [docs/DOWNLOADS.md](docs/DOWNLOADS.md) | How to obtain the V1 outputs |
| [docs/GETTING_STARTED.md](docs/GETTING_STARTED.md) | Load data, caveats, examples |
| [docs/FAQ.md](docs/FAQ.md) | Common questions |
| [docs/DATA_SOURCE_V1.md](docs/DATA_SOURCE_V1.md) | Locked V1 source: NCHS files, URLs, format |
| [docs/DOWNLOAD_INSTRUCTIONS.md](docs/DOWNLOAD_INSTRUCTIONS.md) | Where to click / `curl` / update `file_inventory.csv` |

## Principles

Reproducibility, transparency, limited claims about comparability, usability for researchers who did not build the pipeline, and **incremental releases** (ship V1 before expanding years or adding linked mortality).

## Citation

*TBD* — add a preprint, data note, or DOI when you publish Version 1.
