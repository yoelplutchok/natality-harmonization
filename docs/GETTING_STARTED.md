# Getting started

## Prerequisites

```bash
pip install -r requirements.txt
brew install p7zip   # needed for 2009-2013 (deflate64) and 2015 (PPMd) zips
```

## Quick start: load harmonized data

If the pipeline has already been run, you can immediately load the outputs:

```python
import pandas as pd

# V2 Natality: 138.8M births, 1990-2024 (71 columns)
df = pd.read_parquet("output/harmonized/natality_v2_harmonized_derived.parquet")

# Filter to residents (standard NCHS analysis universe)
df_res = df[df["is_foreign_resident"] == False]

# Or use the pre-filtered convenience file (recommended for most analyses):
df_res = pd.read_parquet("output/convenience/natality_v2_residents_only.parquet")

# V3 Linked: 74.9M births with infant death data, 2005-2023 (81 columns)
linked = pd.read_parquet("output/harmonized/natality_v3_linked_harmonized_derived.parquet")
linked_res = linked[linked["is_foreign_resident"] == False]

# Or use the pre-filtered convenience file:
linked_res = pd.read_parquet("output/convenience/natality_v3_linked_residents_only.parquet")
```

## Available output files

### V2 Natality (1990-2024)

| File | Rows | Columns | Use case |
|------|------|---------|----------|
| `output/convenience/natality_v2_residents_only.parquet` | 138,582,904 | 69 | **Recommended** — residents only, derived indicators included |
| `output/harmonized/natality_v2_harmonized_derived.parquet` | 138,819,655 | 71 | Full file with foreign residents + restatus columns |
| `output/harmonized/natality_v2_harmonized.parquet` | 138,819,655 | 61 | Harmonized only (no derived indicators) |
| `output/yearly_clean/natality_{year}_core.parquet` | varies | varies | Raw NCHS substrings (audit/debug only) |

### V3 Linked birth-infant death (2005-2023)

| File | Rows | Columns | Use case |
|------|------|---------|----------|
| `output/convenience/natality_v3_linked_residents_only.parquet` | 74,785,708 | 79 | **Recommended** — residents only, derived indicators included |
| `output/harmonized/natality_v3_linked_harmonized_derived.parquet` | 74,943,824 | 81 | Full file with foreign residents + restatus columns |
| `output/harmonized/natality_v3_linked_harmonized.parquet` | 74,943,824 | 68 | Harmonized only (no derived indicators) |
| `output/linked/linked_{year}_denomplus.parquet` | varies | ~51 | Raw parsed linked records (audit/debug only) |

## Example analyses

### Low birthweight rate by year (residents)

```python
res = df[df["is_foreign_resident"] == False]
lbw = res.groupby("year").apply(
    lambda g: (g["low_birthweight"].sum() / g["low_birthweight"].notna().sum()) * 100
)
print(lbw)  # LBW% from ~7.0% (1990) to ~8.2% (2020)
```

### Infant mortality rate by year (residents)

```python
res = linked[linked["is_foreign_resident"] == False]
imr = res.groupby("year").apply(
    lambda g: g["infant_death"].sum() / len(g) * 1000
)
print(imr)  # IMR from 6.74 (2005) to 5.35 (2020)
```

### Cause-specific infant mortality

```python
deaths = linked_res[linked_res["infant_death"] == True]
# Top 5 underlying causes of death (ICD-10) in 2020
deaths_2020 = deaths[deaths["year"] == 2020]
deaths_2020["underlying_cause_icd10"].value_counts().head(10)
```

### Neonatal vs postneonatal mortality trend

```python
trend = linked_res.groupby("year").agg(
    births=("infant_death", "count"),
    neonatal=("neonatal_death", "sum"),
    postneonatal=("postneonatal_death", "sum"),
)
trend["neo_imr"] = trend["neonatal"] / trend["births"] * 1000
trend["post_imr"] = trend["postneonatal"] / trend["births"] * 1000
```

## Rebuilding from source

### V2 Natality

```bash
python scripts/01_import/parse_all_v1_years.py --years 1990-2024
python scripts/03_harmonize/harmonize_v1_core.py --years 1990-2024 \
  --out output/harmonized/natality_v2_harmonized.parquet
python scripts/04_derive/derive_v1_core.py \
  --in output/harmonized/natality_v2_harmonized.parquet \
  --out output/harmonized/natality_v2_harmonized_derived.parquet
```

### V3 Linked

```bash
# 2005-2015 (denominator-plus format)
python scripts/01_import/parse_all_linked_years.py --years 2005-2015

# 2016-2020 (period-cohort format)
python scripts/01_import/parse_linked_cohort_year.py \
  --zip raw_data/linked/2017PE2016CO.zip --year 2016 \
  --out output/linked/linked_2016_denomplus.parquet
python scripts/01_import/parse_linked_cohort_year.py \
  --zip raw_data/linked/2018PE2017CO.zip --year 2017 \
  --out output/linked/linked_2017_denomplus.parquet
python scripts/01_import/parse_linked_cohort_year.py \
  --zip raw_data/linked/2019PE2018CO.zip --year 2018 \
  --out output/linked/linked_2018_denomplus.parquet
python scripts/01_import/parse_linked_cohort_year.py \
  --zip raw_data/linked/2020PE2019CO.zip --year 2019 \
  --out output/linked/linked_2019_denomplus.parquet
python scripts/01_import/parse_linked_cohort_year.py \
  --zip raw_data/linked/2021PE2020CO.zip --year 2020 \
  --out output/linked/linked_2020_denomplus.parquet

# Harmonize + derive
python scripts/03_harmonize/harmonize_linked_v3.py --years 2005-2023
python scripts/04_derive/derive_linked_v3.py
```

### Convenience: residents-only subsets (optional)

```bash
python scripts/06_convenience/write_residents_only.py
```

## Critical caveats (read before trend work)

### For all analyses

1. **Residents-only is the default analytic universe** — use `is_foreign_resident == false` (or `restatus != 4`) to match NCHS residence-based tabulations.

2. **Three gestation measurement eras** — preterm rates shift at each boundary:
   - 1990-2002: LMP-based (`gestational_age_weeks_source == 'lmp'`)
   - 2003-2013: combined gestation (`'combined'`)
   - 2014-2023: obstetric estimate (`'obstetric_estimate'`)

3. **Certificate revision coverage matters (especially 2009-2013)** — education, prenatal care, and smoking are effectively revised-only in 2009-2013 public-use files. Use `certificate_revision == 'revised_2003'` for revision-consistent analysis.

4. **1990-2002 era differences**:
   - Race bridge is **approximate** (no official NCHS bridged race before 2003)
   - Education is mapped from years-of-schooling to 4-category
   - Smoking `smoke_any` and `smoke_intensity` come from independent source fields
   - 2003 maternal age is approximate (converted from 41-category recode)

### For linked (V3) analyses

5. **Cohort vs period** — the V3 file follows each birth cohort for a full year of mortality experience. This is preferred for multivariate analysis over period files.

6. **Record weight** — for cohort analyses, NCHS recommends **not** applying the weight variable. The unweighted data are used in all validation targets.

7. **Age-at-death comparability note** — starting 2019, NCHS revised how AGED/AGER5/AGER22 are calculated (using birth certificate time-of-birth instead of death certificate). This produces more accurate sub-24-hour age categorization but means the <1 day and 1-day age-at-death categories are not perfectly comparable with 2005-2018. Total neonatal/postneonatal splits are minimally affected.

8. **Bridged race not available for 2020** — the linked file user guide notes that bridged race is dropped starting 2020 data year. The `maternal_race_bridged4` field uses the available race variables but may have different coverage in 2020.

## Where to look next

- **Codebook**: `docs/CODEBOOK.md` — variable definitions and death-side columns
- **Comparability**: `docs/COMPARABILITY.md` — trend-safe subset guidance
- **Validation**: `docs/VALIDATION.md` — what was checked and how
- **Source data**: `docs/ABOUT_SOURCE_DATA.md` — what NCHS files are used
- **Schema**: `metadata/harmonized_schema.csv` — machine-readable provenance
