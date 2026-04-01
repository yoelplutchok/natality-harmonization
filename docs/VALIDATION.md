# Validation

## Row counts (external + internal)

- `output/validation/row_count_validation_nchs_1994_2024.md` — **unified 1994–2024** Parquet rows vs zip size-implied counts (all years match); resident rows match NCHS residence totals exactly for 1994–2018 (2019–2024 not yet in CDC Open Data API but validated via NVSR targets)
- `output/validation/row_count_validation_nchs_1994_2004.md` — 1994–2004 subset (legacy)
- `output/validation/row_count_validation_nchs_2005_2015.md` — 2005–2015 subset (original V1 baseline)
- The script (`scripts/05_validate/validate_row_counts_vs_nchs.py`) also fetches NCHS published annual births (**residence-based**) from `data.cdc.gov` (`e6fc-ccez`) and uses `RESTATUS` (code `4` = foreign resident) to reproduce the NCHS residence totals exactly.
- Note: 1990–1993 zips (`Nat{year}.zip`) contain only US records. The parser reads all 350-byte records; RESTATUS correctly distinguishes resident status.

## Missingness + frequency QA (raw core extracts)

Legacy per-era QA on the **raw** (pre-harmonization) yearly Parquet extracts. These check blank/whitespace rates on the NCHS field names, not the harmonized column names. For harmonized-level null rates, see the "Harmonized missingness diagnostics" section below.

- Summary pointer: `output/validation/qa_core_2005_2015.md` (V1 baseline)
- Missingness (blank/whitespace-only) by year/column: `output/validation/qa_missingness_core_2005_2015.csv`
- Frequencies (selected low-cardinality columns): `output/validation/qa_frequencies_core_2005_2015.csv`

## Harmonized missingness diagnostics

Per-variable per-year null rates across all harmonized variables, with structural break detection.

Run:

```bash
python scripts/05_validate/harmonized_missingness.py
```

Outputs:

- `output/validation/harmonized_missingness_by_year.csv` — null rate for every harmonized variable by year (columns: `year`, `variable`, `n_total`, `n_null`, `null_pct`)
- `output/validation/harmonized_missingness_breaks.csv` — any variable where the null rate changes by >5 percentage points between adjacent years
- `output/validation/harmonized_missingness_report.md` — summary with break table

This is the recommended first check before any multi-year analysis. Known structural breaks include:

| Variable | Break year | Mechanism |
|----------|-----------|-----------|
| `marital_status` | 2017 | California stopped reporting (~11–12% null) |
| `smoking_any_during_pregnancy` | 2009, 2014 | Revised-only in 2009–2013; all states revised by 2014 |
| `maternal_education_cat4` | 2009 | Same revised-only mechanism as smoking |
| `maternal_race_bridged4` | 2020 | NCHS dropped bridged race (100% null) |
| `maternal_race_ethnicity_5` | 2020 | Multiracial ~3% cannot be bridged (now reconstructed from MRACE6) |
| `father_education_cat4` | 1995, 2009 | Dropped from public-use 1995–2008; partially restored 2009+ |

## Key derived rates (resident-only)

- By-year resident-only LBW + preterm rates:
  - `output/validation/key_rates_core_1990_2024.csv` (full range)
  - `output/validation/key_rates_core_1990_2024.md`

Reproduce:

```bash
python scripts/03_harmonize/harmonize_v1_core.py --years 1990-2024 --out output/harmonized/natality_v2_harmonized.parquet
python scripts/04_derive/derive_v1_core.py --in output/harmonized/natality_v2_harmonized.parquet --out output/harmonized/natality_v2_harmonized_derived.parquet
python scripts/05_validate/key_rates_from_derived_core.py --in output/harmonized/natality_v2_harmonized_derived.parquet --years 1990-2024
```

Spot-checks: LBW 6.97% (1990), 7.32% (1995), 7.57% (2000), 8.07% (2015), 8.24% (2020) — all match NCHS published values. Preterm rates show expected series breaks at 2003 (LMP→combined) and 2014 (combined→obstetric estimate).

## External validation (V2 natality)

**Results: 183/183 targets pass.** Validated against NCHS “Births: Final Data” NVSR reports, CDC Data API, and NCHS Data Briefs covering resident birth counts, LBW%, preterm%, twin/triplet+ rates, cesarean%, singleton%, male%, smoking%, and Medicaid% across 1990-2024.

### Playbook

Goal: for a small set of **gold** outcomes/distributions, reproduce published NCHS residence-based values and record the comparison in a **machine-readable** way.

### Step 1: Define the validation universe(s)

- **resident**: exclude foreign residents (`is_foreign_resident == false`)
- **resident_revised**: resident births **and** `certificate_revision == 'revised_2003'`
  - Use this universe for domains that are effectively revised-only in 2009–2013 public-use files (education, prenatal care initiation, smoking).

### Step 2: Pick official targets (recommended minimum)

For benchmark years (e.g., **1995**, **2000**, **2005**, **2010**, **2015**, **2020**), pull values from NCHS “Births: Final Data” (or equivalent official natality report) for:

- resident births (count)
- low birthweight % (<2500g)
- preterm % (<37 weeks)
- (recommended) twin birth rate (per 1,000 births) and triplet+ birth rate (per 100,000 births)
- (optional) singleton % / plurality distribution
- (optional) male % (sex ratio)

For education / smoking / prenatal-care initiation, either:

- validate only **2014+** (near-national revised coverage), or
- validate within a **revised-only reporting area** if the official table defines one (then use `resident_revised`).

### Step 3: Record targets in a CSV (auditable)

Edit:

- `metadata/external_validation_targets_v1.csv`

Fill in `expected_value`, `tolerance_abs`, and a precise `value_source` (table name/number + publication).

### Step 4: Run the comparison script

```bash
python scripts/05_validate/compare_external_targets_v1.py
```

Outputs:

- `output/validation/external_validation_v1_comparison.csv`
- `output/validation/external_validation_v1_comparison.md`

### Step 5: Treat failures as actionable bugs or documented differences

If a comparison fails:

- confirm universe alignment (resident vs occurrence, resident-only vs all births)
- confirm definition alignment (gestation source break at 2014; sentinel codes; category mappings)
- if still failing, either fix the pipeline/metadata or document the reason as a V1 limitation (with a clear “do not use for trend” note in `docs/COMPARABILITY.md` and `metadata/harmonized_schema.csv`).

## Internal invariants (regression checks)

Deterministic internal invariants scan that should always pass unless a regression is introduced.

Run:

```bash
python scripts/05_validate/validate_v1_invariants.py \
  --in output/harmonized/natality_v2_harmonized_derived.parquet --years 1990-2024
```

Outputs:

- `output/validation/invariants_report_1990_2024.md`
- `output/validation/invariants_year_summary_1990_2024.csv`

This script checks:

- internal consistency of derived fields (LBW/preterm/singleton vs their defining cleaned variables)
- consistency of race/ethnicity and Hispanic-origin derivations
- smoking any/intensity consistency (scoped to 2003+ where smoke_any is derived from intensity; 1990–2002 exempt because TOBACCO and CIGAR6 are independent source fields)
- gestation source rules (no obstetric-estimate source prior to 2014)
- certificate revision validity (2014+ must be revised_2003; values must be one of the three allowed strings)
- preservation of known structural coverage constraints (e.g., 2009–2013 unrevised records remain missing for revised-only domains)
- era-coverage constraints for 2014+-only variables (congenital anomalies, infections, prior cesarean count, fertility treatment, ART must be null pre-2014)
- father demographics consistency (Hispanic/race-ethnicity agreement, education null 1995–2008, payment source null pre-2009)
- **null-rate discontinuity detection**: for 16 key harmonized variables, computes per-year null rates and flags any >5 percentage-point year-over-year change (informational — reflects known structural changes, not bugs)

## V3: Linked birth-infant death validation (2005–2023)

### Row counts and parsing validation

Run:

```bash
python scripts/05_validate/validate_linked_parquets.py --years 2005-2023
```

Outputs:

- Range-labeled CSV/MD summaries under `output/validation/` (filenames reflect the year range used)
- Current full-range outputs: `linked_validation_2005_2023.csv` and `linked_validation_2005_2023.md`

This script checks:

- **Row-count correctness**: Parquet rows match zip size-implied rows across the full 2005-2023 linked scope
- **Layout correctness**: DOB_YY matches expected year at 100%; byte-level field position spot checks
- **Missingness sanity**: death fields (AGED, UCOD) blank for survivors, filled for deaths — exact alignment with FLGND
- **Frequency/IMR trend**: IMR falls from 6.74 (2005) to 5.49 (2023) in the full linked files, remaining in the expected low-5 to mid-6 range
- **Linked vs natality row counts**: exact for 14/19 years; small positive differences occur only in 2005, 2006, 2008, 2011, and 2012 due to LATEREC

### External validation (linked)

Targets: `metadata/external_validation_targets_v3_linked.csv` — 35 active targets across 2005, 2010, 2015, and 2020-2023.

Run:

```bash
python scripts/05_validate/compare_external_targets_v3_linked.py
```

Outputs:

- `output/validation/external_validation_v3_linked_comparison.csv`
- `output/validation/external_validation_v3_linked_comparison.md`

**Results: 35/35 active targets pass.**

Cross-checked against linked file user guides (`LinkCO05Guide.pdf`, `LinkCO10Guide.pdf`, `LinkCO15Guide.pdf`, `21PE20CO_linkedUG.pdf`, `22PE21CO_linkedUG.pdf`, `23PE22CO_linkedUG.pdf`, `24PE23CO_linkedUG.pdf`):

- 2005 and 2010: resident births and weighted infant death counts match the user guides exactly.
- 2015: resident births, IMR, and neonatal deaths match exactly; unweighted infant deaths and postneonatal deaths differ by 1 record but pass within tolerance.
- 2020: all active births/deaths/IMR/neonatal/postneonatal targets match exactly.
- 2021: resident births, unweighted infant deaths, and IMR match exactly.
- 2022 and 2023: all active births/deaths/IMR/neonatal/postneonatal/neonatal-IMR/postneonatal-IMR targets match exactly.

Notes:
- 2005 and 2010 user guides report **weighted** infant death counts. 2015 and 2020-2023 user guides report **unweighted** (explicitly labeled).
- 2015 and 2020-2023 guides: "For cohort file use: do not apply the weight."
- The 1-record differences in 2015 are likely LATEREC edge cases and are not a concern.
- The 4 excluded 2021 split targets (`neonatal_deaths`, `postneonatal_deaths`, `neonatal_imr_per_1000`, `postneonatal_imr_per_1000`) remain unresolved: our data show 12,489 neonatal / 7,476 postneonatal deaths versus 12,620 / 7,345 in the user guide, while total deaths match exactly.
- 2016-2023 use period-cohort format (separate numerator/denominator files merged by CO_SEQNUM); 2005-2015 use denominator-plus format.

### IMR trend (residents, 2005–2023)

| Year | Births | Deaths | IMR | Neonatal IMR | Postneonatal IMR |
|------|--------|--------|-----|-------------|-----------------|
| 2005 | 4,138,577 | 27,893 | 6.74 | 4.47 | 2.27 |
| 2006 | 4,265,593 | 28,278 | 6.63 | 4.40 | 2.23 |
| 2007 | 4,316,233 | 28,725 | 6.66 | 4.33 | 2.32 |
| 2008 | 4,247,726 | 27,492 | 6.47 | 4.23 | 2.24 |
| 2009 | 4,130,665 | 25,793 | 6.24 | 4.11 | 2.13 |
| 2010 | 3,999,386 | 24,156 | 6.04 | 3.99 | 2.05 |
| 2011 | 3,953,591 | 23,547 | 5.96 | 4.00 | 1.95 |
| 2012 | 3,952,842 | 23,378 | 5.91 | 3.98 | 1.94 |
| 2013 | 3,932,181 | 23,142 | 5.89 | 3.99 | 1.89 |
| 2014 | 3,988,076 | 23,256 | 5.83 | 3.91 | 1.92 |
| 2015 | 3,978,497 | 23,327 | 5.86 | 3.91 | 1.95 |
| 2016 | 3,945,875 | 22,893 | 5.80 | 3.85 | 1.95 |
| 2017 | 3,855,500 | 22,167 | 5.75 | 3.84 | 1.91 |
| 2018 | 3,791,712 | 21,346 | 5.63 | 3.75 | 1.88 |
| 2019 | 3,747,540 | 20,639 | 5.51 | 3.65 | 1.85 |
| 2020 | 3,613,647 | 19,346 | 5.35 | 3.53 | 1.82 |
| 2021 | 3,664,292 | 19,965 | 5.45 | 3.41 | 2.04 |
| 2022 | 3,667,758 | 20,268 | 5.53 | 3.53 | 2.00 |
| 2023 | 3,596,017 | 19,743 | 5.49 | 3.59 | 1.91 |

The 2021 neonatal and postneonatal rates above are computed from the linked file itself; they are shown for trend context but excluded from external target validation until the 131-death split discrepancy is resolved.

### Reproduce linked harmonized + derived outputs

```bash
# 2005-2015 (denominator-plus format)
python scripts/01_import/parse_all_linked_years.py --years 2005-2015

# 2016-2023 (period-cohort format)
for cohort_year in 2016 2017 2018 2019 2020 2021 2022 2023; do
  period_year=$((cohort_year + 1))
  python scripts/01_import/parse_linked_cohort_year.py \
    --zip "raw_data/linked/${period_year}PE${cohort_year}CO.zip" \
    --year "$cohort_year" \
    --out "output/linked/linked_${cohort_year}_denomplus.parquet"
done

# Harmonize and derive
python scripts/03_harmonize/harmonize_linked_v3.py --years 2005-2023
python scripts/04_derive/derive_linked_v3.py

# Validate
python scripts/05_validate/validate_linked_parquets.py --years 2005-2023
python scripts/05_validate/compare_external_targets_v3_linked.py
```
