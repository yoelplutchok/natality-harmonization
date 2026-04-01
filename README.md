# U.S. Natality Harmonization Project

A modern, researcher-ready release of harmonized U.S. natality microdata for cross-year analysis.

## Scope

Harmonized U.S. natality microdata for **1990–2024** with a stable 69-column harmonized schema (82 with derived indicators) covering maternal and paternal demographics, prenatal care, medical risk factors, congenital anomalies, infections, fertility treatment, clinical detail, and birth outcomes, plus documentation and validation.

## Raw data

**NCHS public-use period natality** files: `Nat{year}.zip` (1990–1993) and `Nat{year}us.zip` (1994–2024), with the annual documentation PDF as the layout authority. Full rationale, URLs, and caveats: **[docs/DATA_SOURCE_V1.md](docs/DATA_SOURCE_V1.md)**.

## Current status

- **Downloads:** `raw_data/` contains all 35 natality zips (1990–2024); `raw_docs/` contains documentation PDFs; `metadata/file_inventory.csv` tracks URLs, filenames, and import status.
- **Import:** `scripts/01_import/parse_all_v1_years.py` produces `output/yearly_clean/natality_{year}_core.parquet` (raw substrings). Handles five record layouts (350/1350/1500/775/1345 bytes). `7z` on PATH for deflate64/PPMd zips (2009–2013, 2015).
- **Harmonized (1990–2024):** complete — `output/harmonized/natality_v2_harmonized.parquet` + `natality_v2_harmonized_derived.parquet`.
- **Validation:** all invariant checks pass (0 violations); 183/183 external targets pass (1990–2024); key rates plausible across all 35 years. See `output/validation/`.
- **V3 Linked (2005–2023):** `output/harmonized/natality_v3_linked_harmonized_derived.parquet` — 74.9M birth records with linked infant death data (IMR, cause of death, neonatal/postneonatal). Validated against NCHS linked file user guides for 2005, 2010, 2015, 2020-2023 (35/35 external targets pass; IMR trend 6.74→5.49).

## Headline metrics

- **Years covered**: 1990–2024 (35 years natality); 2005–2023 (19 years linked birth-infant death)
- **Birth records**: 138,819,655 total (natality V2); 74,943,824 (linked V3)
- **Harmonized columns**: 69 natality + 13 derived = 82 (V2); 74 + 13 derived = 87 including death-side (V3 linked)
- **Convenience outputs**: `output/convenience/` — residents-only subsets (134.95M V2, 63.86M V3 linked)
- **External validation**: `output/validation/external_validation_v1_comparison.csv` (183 targets, 1990–2024, all pass); linked V3: 35/35 targets pass (2005/2010/2015/2020-2023 user guides — births, deaths, IMR, neonatal/postneonatal deaths and IMR)

## Repository structure

```
natality-harmonization/
├── README.md
├── requirements.txt
├── raw_data/              # NCHS natality + linked zips (not committed; see inventory)
│   └── linked/            # linked birth-infant death zips
├── raw_docs/              # NCHS User Guide PDFs
│   └── linked/            # linked file user guides
├── metadata/              # CSV tracking sheets (source of truth for harmonization)
├── scripts/
│   ├── 01_import/         # parse fixed-width → per-year Parquet
│   ├── 03_harmonize/      # map era-specific fields → common schema
│   ├── 04_derive/         # compute LBW, preterm, age categories, etc.
│   ├── 05_validate/       # invariants, external targets, missingness diagnostics
│   ├── 06_convenience/    # residents-only subsets + provenance
│   └── 07_figures/        # paper figures
├── output/
│   ├── yearly_clean/      # raw per-year Parquet extracts
│   ├── linked/            # raw per-year linked Parquet extracts
│   ├── harmonized/        # stacked harmonized + derived Parquet files
│   ├── convenience/       # residents-only subsets + PROVENANCE.md
│   └── validation/        # external targets, invariants, missingness
├── notebooks/             # quickstart examples
├── figures/               # publication-ready figures (PDF + PNG)
└── docs/                  # codebook, comparability, FAQ, validation, etc.
```

## Progress checklist

See [docs/MILESTONES.md](docs/MILESTONES.md) for the full Version 1 milestone ladder.

## Documentation

| Document | Purpose |
|----------|---------|
| [docs/GETTING_STARTED.md](docs/GETTING_STARTED.md) | Load data, caveats, example analyses |
| [docs/CODEBOOK.md](docs/CODEBOOK.md) | Variable-level definitions (69 harmonized + 13 derived) |
| [docs/COMPARABILITY.md](docs/COMPARABILITY.md) | Cross-year comparability rules and known pitfalls |
| [docs/VALIDATION.md](docs/VALIDATION.md) | Validation against official NCHS tabulations |
| [docs/FAQ.md](docs/FAQ.md) | Common questions (20 entries) |
| [docs/ABOUT_THIS_RELEASE.md](docs/ABOUT_THIS_RELEASE.md) | What this project adds vs. raw NCHS files |
| [docs/ABOUT_SOURCE_DATA.md](docs/ABOUT_SOURCE_DATA.md) | What natality files are and why they matter |
| [docs/DATA_SOURCE_V1.md](docs/DATA_SOURCE_V1.md) | NCHS source files, URLs, compression formats |
| [docs/DOWNLOADS.md](docs/DOWNLOADS.md) | Build instructions for all outputs |
| [docs/DOWNLOAD_INSTRUCTIONS.md](docs/DOWNLOAD_INSTRUCTIONS.md) | How to download raw NCHS files (`curl` commands) |

## Principles

Reproducibility, transparency, explicit comparability documentation, and usability for researchers who did not build the pipeline.

## Citation

- Cite **NCHS** as the source of the underlying public-use microdata.
- Cite this repository/release (DOI pending).
