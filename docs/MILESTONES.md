# Version 1 milestone ladder

Use this checklist; check boxes as you complete work.

- [x] **M1** — Skeleton: folders, metadata CSVs, scope in README *(V1 raw source locked — see [DATA_SOURCE_V1.md](DATA_SOURCE_V1.md))*
- [x] **M2** — Benchmark years (2005, 2010, 2015) documented; benchmark crosswalk finalized in `metadata/variable_crosswalk_working.csv` (raw-field mappings, comparability classes, recode/coverage notes).
- [x] **M3** — Full **core** parse for 2005–2015 → `output/yearly_clean/natality_{year}_core.parquet`; row counts and QA tracked in `metadata/validation_tracking.csv`. External row-count validation vs NCHS resident births + raw zip size: `output/validation/row_count_validation_nchs_2005_2015.md`. Missingness/frequency QA outputs: `output/validation/qa_core_2005_2015.md`.
- [x] **M4** — Harmonized schema drafted with comparability classes (`metadata/harmonized_schema.csv` + `docs/COMPARABILITY.md`)
- [x] **M5** — Stacked harmonized V1 file created (`output/harmonized/natality_v1_harmonized.parquet`) with expanded V1 schema
- [x] **M6** — Derived variables generated for harmonized V1 stack (`scripts/04_derive/derive_v1_core.py` → `output/harmonized/natality_v1_harmonized_derived.parquet`)
- [x] **M7** — Validation vs. official tabulations documented (`output/validation/external_validation_v1_comparison.csv` + `docs/VALIDATION.md`)
- [x] **M8** — Public-facing materials: home (README), codebook, downloads, getting started, FAQ

Later versions (2–5) start after M8 is done.

# Version 2 milestone ladder (in progress)

- [x] **V2.1** — Extend imports/parsing to 2016–2020 (`output/yearly_clean/natality_{year}_core.parquet` for 2016–2020) and update `metadata/file_inventory.csv`.
- [x] **V2.2** — Make validation scripts year-range-safe (row counts, QA, invariants, key rates) so V2 outputs don’t overwrite V1 artifacts.
- [x] **V2.3** — Build a working 2005–2020 stack (`output/harmonized/natality_v2_harmonized.parquet` + `_derived.parquet`) and validate against extended external targets (`metadata/external_validation_targets_v2.csv`; results in `output/validation/v2/`).
- [x] **V2.4** — Final V2 scope: 1990–2024 (35 years), 61 harmonized + 10 derived columns (71 total). Includes paternal demographics, congenital anomalies, infections, fertility treatment, prior cesarean count, payment source. 164/164 external validation targets pass. V3 linked extended to 2005–2023 (19 years, 74.9M rows). Convenience residents-only Parquet subsets generated.
- [x] **V2.5** — Post-LBW-IMR-divergence improvements: 69 harmonized + 13 derived columns (82 total), 183/183 external targets pass. New columns: `marital_reporting_flag` (2014+, from F_MAR_P), `race_bridge_method` (derivation era indicator), `diabetes_any_bool`/`hypertension_chronic_bool`/`hypertension_gestational_bool` (sentinel 9→null), 6 clinical detail columns (`pre_pregnancy_diabetes`, `gestational_diabetes`, `nicu_admission`, `weight_gain_pounds`, `induction_of_labor`, `breastfed_at_discharge`). Reconstructed `maternal_race_ethnicity_5` for 2020–2024 from MRACE6 detail codes. Added unified missingness diagnostics script, null-rate discontinuity detection in invariants validator, parquet versioning with SHA-256 provenance. Added "Known pitfalls for multi-decade trend analyses" to COMPARABILITY.md. Full docs update across all markdown files.
