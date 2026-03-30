# FAQ

## What years are covered?

- **V2 Natality**: 1990-2024 (35 years, 138.8 million birth records)
- **V3 Linked birth-infant death**: 2005-2023 (19 years, 74.9 million birth records with linked infant death data)

## What is the difference between V2 and V3?

**V2 Natality** contains birth records only — maternal demographics, prenatal care, birth outcomes. It covers the longest time span (1990-2024).

**V3 Linked** contains birth records linked to infant death certificates. It includes all the V2 birth-side columns plus death-side columns (cause of death, age at death, neonatal/postneonatal classification). Use V3 for infant mortality research. V3 starts at 2005 because that is when NCHS began releasing public-use cohort linked files in the current format.

## Which file should I use?

- For **birth outcome research** (LBW, preterm, demographic trends): use `output/convenience/natality_v2_residents_only.parquet` (pre-filtered to residents, 69 columns)
- For **infant mortality research** (IMR, cause-specific mortality, neonatal vs postneonatal): use `output/convenience/natality_v3_linked_residents_only.parquet` (pre-filtered to residents, 79 columns)
- For analyses that need **foreign residents** or the `restatus` column: use the full `natality_v2_harmonized_derived.parquet` (71 columns) or `natality_v3_linked_harmonized_derived.parquet` (81 columns)
- For **auditing or debugging**: use the yearly `output/yearly_clean/` or `output/linked/` files

## Are these data nationally representative?

Yes. These are the **NCHS national public-use natality files** — essentially a census of all registered births in the United States for each year. However:

- Some variables are not fully comparable across years and certificate revisions (see `docs/COMPARABILITY.md`)
- Public-use files do **not** include sub-state geography (county/city)
- 1990-1993 files use `Nat{year}.zip` (without "us" suffix) but contain only US records

## What is the recommended analysis universe?

Use **resident births** by default:

```python
df_res = df[df["is_foreign_resident"] == False]
```

This matches the residence-based tabulations used in all NCHS publications and in our validation.

## Which variables are fully comparable across all years?

See `docs/COMPARABILITY.md` for the full list. Variables with **full** comparability (trend-safe 1990-2024):

- `year`, `restatus`, `is_foreign_resident`
- `marital_status`, `live_birth_order_recode`, `total_birth_order_recode`
- `plurality_recode`, `infant_sex`, `birthweight_grams`, `apgar5`
- Derived: `low_birthweight`, `very_low_birthweight`, `singleton`, `maternal_age_cat`

## Which variables have known breaks?

Key breaks:

- **Gestation/preterm**: three eras with different measurement methods (LMP → combined → obstetric estimate). Series breaks at 2003 and 2014.
- **Education/prenatal care/smoking**: effectively revised-only in 2009-2013 public-use files (use `certificate_revision == 'revised_2003'` for consistent analysis).
- **Race bridging**: approximate for 1990-2002; official NCHS bridged race from 2003.
- **Maternal age**: 2003 uses an approximate recode from `MAGER41`.
- **Father education**: null for 1995-2008 (field dropped from public-use files); partial coverage 2009-2010 (2003-revision states only).
- **Father race/ethnicity**: `NH_other` category only exists for 1990-2002 (code 8 semantic shift).
- **Payment source**: available 2009+ (partial 2009-2010); null for 1990-2008.

## Why are some fields missing in some years?

Common reasons:

- **Not on that certificate revision** (e.g., pre-pregnancy smoking, congenital anomalies, infections, and fertility treatment fields are only available 2014+)
- **Structural coverage differences** between revised and unrevised certificates
- **Public-use blanking** of unrevised-only fields in 2009-2013; father education dropped from public-use 1995-2008
- **Sentinel codes** (`99` for gestation, `9999` for birthweight) treated as missing in derived `_clean` fields
- **"Not applicable" codes** (e.g., `RF_FEDRG` X code for births without fertility treatment → null)

## Should I apply the record weight in the linked file?

**No, for cohort analyses.** The NCHS user guides explicitly state: "For cohort file use: do not apply the weight." The weight is designed for period (cross-sectional) analyses only. All validation in this project uses unweighted cohort data.

## What is the difference between the 2005-2015 and 2016-2020 linked files?

The underlying data format changed:

- **2005-2015**: "denominator-plus" format — birth and death fields are appended in a single record
- **2016-2020**: "period-cohort" format — birth records (denominator) and death records (numerator) are in separate files, merged by sequence number (CO_SEQNUM)

This project handles both formats transparently. The harmonized output has the same schema regardless of source format.

## How was the age-at-death variable changed in 2019?

Starting with 2019, NCHS calculates age at death from the birth certificate's date and time of birth (instead of the death certificate). This improves accuracy for deaths in the first 24 hours but means the `<1 hour` and `1 day` age categories are not perfectly comparable with 2005-2018. The impact on total neonatal/postneonatal splits is minimal.

## Is geography included?

No. The public-use natality files do not include sub-state geography. State-level identifiers are also suppressed in the public-use linked files from 2005 onward.

## How should the data be cited?

- Cite NCHS as the source of the underlying public-use microdata
- Cite this repository/release (DOI pending)

Example:

> Birth and linked birth-infant death microdata from the National Center for Health Statistics (NCHS), U.S. Centers for Disease Control and Prevention. Harmonized using the U.S. Natality Harmonization Project [URL/DOI].

## Where do I see variable definitions and provenance?

- Machine-readable schema: `metadata/harmonized_schema.csv`
- Human-readable overview: `docs/CODEBOOK.md`
- Comparability details: `docs/COMPARABILITY.md`

## How do I validate that the pipeline matches official totals?

See `docs/VALIDATION.md`. Key artifacts:

- `output/validation/external_validation_v1_comparison.csv` (V2 natality: 164 targets)
- `output/validation/external_validation_v3_linked_comparison.csv` (V3 linked: 14 targets)
- `output/validation/invariants_report_1990_2023.md` (deterministic consistency checks)
