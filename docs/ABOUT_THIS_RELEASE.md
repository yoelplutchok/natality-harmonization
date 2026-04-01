# About this release

This repository produces **harmonized, researcher-ready** releases of **U.S. natality public-use microdata** and **linked birth-infant death data**.

## What's included

### V2 Natality (1990-2024): 138.8 million births

A single stacked Parquet file covering all 35 years of U.S. natality data, with a stable 69-column harmonized schema (plus 13 derived columns). The pipeline resolves five different NCHS record layouts, three certificate revisions, and dozens of field-name/coding changes into one consistent format. Variables span maternal and paternal demographics, prenatal care, medical risk factors, congenital anomalies, infections, fertility treatment, clinical detail, delivery details, and birth outcomes.

**Output files:**

| File | Rows | Columns | Description |
|------|------|---------|-------------|
| `natality_v2_harmonized.parquet` | 138,819,655 | 69 | Harmonized birth records |
| `natality_v2_harmonized_derived.parquet` | 138,819,655 | 82 | + derived indicators (LBW, preterm, singleton, diabetes/HTN booleans, etc.) |
| `natality_v2_residents_only.parquet` | 138,582,904 | 80 | **Convenience** — residents only, `restatus`/`is_foreign_resident` dropped |

### V3 Linked birth-infant death (2005-2023): 74.9 million births

The same birth records for 2005-2023, now linked to infant death certificates. Adds cause of death, age at death, neonatal/postneonatal classification, and record weights — everything needed for infant mortality research.

**Output files:**

| File | Rows | Columns | Description |
|------|------|---------|-------------|
| `natality_v3_linked_harmonized.parquet` | 74,943,824 | 76 | Birth-side + death-side harmonized |
| `natality_v3_linked_harmonized_derived.parquet` | 74,943,824 | 92 | + derived indicators including neonatal/postneonatal death |
| `natality_v3_linked_residents_only.parquet` | 74,785,708 | 90 | **Convenience** — residents only, `restatus`/`is_foreign_resident` dropped |

## What the pipeline adds over raw NCHS files

- **Named-column Parquet outputs** — no fixed-width parsing needed
- A **stable harmonized schema** (`metadata/harmonized_schema.csv`) with provenance and derivation rules
- Explicit **comparability policy** (`docs/COMPARABILITY.md`) with trend-safe subset guidance and a "known pitfalls" checklist for multi-decade analyses
- Common **derived analytic variables** (LBW, preterm, singleton, age categories, nullable boolean medical risk factors, neonatal/postneonatal death)
- **Race/ethnicity reconstruction for 2020+**: `maternal_race_ethnicity_5` is reconstructed from MRACE6 detail codes after NCHS dropped bridged race; `race_bridge_method` indicates the derivation era
- **Marital reporting flag**: `marital_reporting_flag` (2014+) distinguishes California non-reporting-state nulls from genuine unknowns
- **Missingness diagnostics**: per-variable per-year null rates with structural break detection (`output/validation/harmonized_missingness_by_year.csv`)
- **Validation artifacts** against official NCHS tabulations (`docs/VALIDATION.md`) plus null-rate discontinuity detection in the invariant checker
- Automatic handling of **era-specific field positions, names, and coding** across 5 record layouts
- **Parquet versioning**: convenience files embed git hash and build timestamp in metadata; `PROVENANCE.md` provides SHA-256 checksums

## Key harmonization policies

Full details in `docs/COMPARABILITY.md`. Highlights:

- **Resident births are the default analysis universe**: filter to `is_foreign_resident == false` to match NCHS residence-based tabulations.
- **Certificate revision is explicit**: `certificate_revision` enables revision-consistent subsets, critical for 2009-2013 when key fields are revised-only.
- **Gestation has three measurement eras**: LMP (1990-2002), combined (2003-2013), obstetric estimate (2014-2024). Use `gestational_age_weeks_source` to identify the era.
- **Race/ethnicity uses three bridge methods**: approximate crosswalk (1990-2002), NCHS bridged race (2003-2019), reconstructed from MRACE6 detail (2020-2024). `race_bridge_method` indicates which was used; multiracial births (~3% for 2020+) map to null.
- **Marital status has a 2017 break**: California stopped reporting; use `marital_reporting_flag` (2014+) to filter to reporting states.
- **Medical risk factor booleans**: `diabetes_any_bool`, `hypertension_chronic_bool`, `hypertension_gestational_bool` convert the 1/2/9 integer coding to nullable booleans (9→null), preventing sentinel unknowns from passing `IS NOT NULL` filters.
- **2009-2013 has structural missingness**: education, prenatal care, and smoking are effectively revised-only in public-use files for those years.
- **Linked file death-side**: `infant_death` boolean normalizes different FLGND coding across eras (1/2 for 2005-2013, 1/blank for 2014-2023).

## What this release does NOT claim

- Not every variable is fully trend-safe across all 35 years (see comparability classes in the codebook and "Known pitfalls" in `COMPARABILITY.md`).
- No restricted-use geography or restricted-use variables are included.
- 2009-2013 missing unrevised values are documented, not imputed.
- 1990-2002 race bridging is approximate (official NCHS bridged race starts at 2003); 2020-2024 race/ethnicity is reconstructed from detail codes with ~3% multiracial exclusion.

## Validation summary

| Dataset | External targets | Result |
|---------|-----------------|--------|
| V2 Natality (1990-2024) | 183 targets across 1990-2024 (births, LBW%, preterm%, plurality, singleton%, male%, cesarean%, smoking%, Medicaid%) | 183/183 pass |
| V2 Natality invariants | Deterministic consistency checks | 0 violations |
| V3 Linked (2005-2023) | 35 active targets across 2005, 2010, 2015, 2020-2023 (births, infant deaths, IMR, neonatal/postneonatal deaths, neonatal/postneonatal IMR) | 35/35 active pass |

Note: 4 additional 2021 neonatal/postneonatal split targets are intentionally excluded pending investigation of an unresolved 131-death discrepancy between our file and the user guide.

See `docs/VALIDATION.md` for details and `output/validation/` for machine-readable results.

## How to reproduce

From repo root (after downloading raw inputs per `docs/DOWNLOAD_INSTRUCTIONS.md`):

```bash
# V2 Natality (1990-2024)
python scripts/01_import/parse_all_v1_years.py --years 1990-2024
python scripts/03_harmonize/harmonize_v1_core.py --years 1990-2024
python scripts/04_derive/derive_v1_core.py
python scripts/05_validate/compare_external_targets_v1.py
python scripts/05_validate/validate_v1_invariants.py --years 1990-2024
python scripts/05_validate/harmonized_missingness.py

# V3 Linked (2005-2015, denominator-plus format)
python scripts/01_import/parse_all_linked_years.py --years 2005-2015

# V3 Linked (2016-2023, period-cohort format)
python scripts/01_import/parse_linked_cohort_year.py --zip raw_data/linked/2017PE2016CO.zip --year 2016 --out output/linked/linked_2016_denomplus.parquet
python scripts/01_import/parse_linked_cohort_year.py --zip raw_data/linked/2018PE2017CO.zip --year 2017 --out output/linked/linked_2017_denomplus.parquet
python scripts/01_import/parse_linked_cohort_year.py --zip raw_data/linked/2019PE2018CO.zip --year 2018 --out output/linked/linked_2018_denomplus.parquet
python scripts/01_import/parse_linked_cohort_year.py --zip raw_data/linked/2020PE2019CO.zip --year 2019 --out output/linked/linked_2019_denomplus.parquet
python scripts/01_import/parse_linked_cohort_year.py --zip raw_data/linked/2021PE2020CO.zip --year 2020 --out output/linked/linked_2020_denomplus.parquet
python scripts/01_import/parse_linked_cohort_year.py --zip raw_data/linked/2022PE2021CO.zip --year 2021 --out output/linked/linked_2021_denomplus.parquet
python scripts/01_import/parse_linked_cohort_year.py --zip raw_data/linked/2023PE2022CO.zip --year 2022 --out output/linked/linked_2022_denomplus.parquet
python scripts/01_import/parse_linked_cohort_year.py --zip raw_data/linked/2024PE2023CO.zip --year 2023 --out output/linked/linked_2023_denomplus.parquet

# V3 harmonize + derive + validate
python scripts/03_harmonize/harmonize_linked_v3.py --years 2005-2023
python scripts/04_derive/derive_linked_v3.py
python scripts/05_validate/validate_linked_parquets.py --years 2005-2023
python scripts/05_validate/compare_external_targets_v3_linked.py

# Convenience: residents-only subsets (optional)
python scripts/06_convenience/write_residents_only.py
```

## Citation

- Cite NCHS as the source of the underlying natality and linked birth-infant death public-use files.
- Cite this repository/release (DOI pending; when available, it will be linked from `README.md`).
