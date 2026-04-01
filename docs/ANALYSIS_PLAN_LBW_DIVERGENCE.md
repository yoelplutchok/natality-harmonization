# Rising Low Birthweight, Falling Infant Mortality: Decomposing 35 Years of Diverging U.S. Birth Outcomes, 1990-2023

## Comprehensive Execution Plan

---

## CURSOR / LLM INITIALIZATION PROMPT

> **Copy and paste this entire block when starting a new session for this project:**

```text
You are helping me build a research project decomposing the 35-year divergence
between rising low birthweight (LBW) and falling infant mortality (IMR) in the
United States, using the natality-harmonization project's harmonized public-use
files (V2: 1990-2024, 138.8M births; V3 linked: 2005-2023, 74.9M births).

This execution plan document is your PRIMARY source of truth.

## YOUR WORKING RULES:

### 1. STEP-BY-STEP EXECUTION
- Work through ONE phase at a time. Do not rush ahead.
- After completing each phase, STOP and wait for my confirmation before proceeding.
- If a phase has multiple sub-tasks, complete them all before moving on.

### 2. DOCUMENT UPDATES (CRITICAL)
After completing each phase, you MUST update this markdown file with:
- Mark the phase as complete in the Progress Log below
- Add a timestamped entry in the "Execution Log" section describing:
  - What was done
  - What files were created/modified
  - Any issues encountered and how they were resolved
  - Output verification (e.g., "Singleton LBW 2024: 6.999%, matching prior query")

### 3. MANDATORY SELF-AUDIT PROTOCOL
Every substantive phase requires TWO passes:
1. Primary implementation pass
2. Independent audit pass

You may not mark a phase complete until the audit pass is done.

At minimum, every audit pass must:
- Recompute key counts independently from raw or prior-phase data
- Check exact summation identities (subgroups must sum to totals)
- Compare outputs to external validation targets where available
- Spot-check at least 3 strata manually
- Confirm that outputs are internally consistent across files
- State any residual risks explicitly

### 4. STOP CONDITIONS
If ANY of the following happen, STOP and investigate before proceeding:
- An exact count fails to match a required target outside stated tolerance
- Subgroup counts do not sum exactly to totals
- A partial-comparability variable is used outside the approved era/subset
- A phase rerun changes a prior result unexpectedly and you do not know why
- A Kitagawa composition + rate effect does not sum to the total observed change

### 5. NO SILENT DEVIATIONS
Before making ANY change that deviates from this plan:
1. STOP and explain what you want to change
2. Explain WHY you think it is better
3. Wait for my approval before proceeding

Flag deviations with: "PROPOSED CHANGE: [description]"

### 6. DATA ACQUISITION
Use the pre-built harmonized outputs from the `natality-harmonization` repo.
Do NOT download raw NCHS files — use the pre-built harmonized output.

The source data lives in the sibling repo at ~/Desktop/natality-harmonization/.
During Phase 0 setup, two Parquet files are copied into this repo's data/ folder:

- data/natality_v2_residents_only.parquet (~1.5 GB, 138,582,904 rows, 69 cols)
  Source: natality-harmonization/output/convenience/natality_v2_residents_only.parquet
- data/natality_v3_linked_residents_only.parquet (~919 MB, 74,785,708 rows, 79 cols)
  Source: natality-harmonization/output/convenience/natality_v3_linked_residents_only.parquet

See Phase 0 for exact copy commands and verification steps.

### 7. STATISTICAL RIGOR
- This is a descriptive decomposition study. NEVER make causal claims.
- Use language like "associated with," "compositional shift," NOT "caused by."
- Kitagawa decomposition is a purely arithmetic identity: total change =
  composition effect + rate effect. It does not identify causal mechanisms.
- The smoking counterfactual is a "what-if" calculation, not a causal estimate.
  State this explicitly.
- With N > 130 million, virtually everything will be statistically significant.
  Focus on effect sizes, absolute differences, and public health significance.
- Birthweight is fully comparable 1990-2024 with NO measurement breaks.
  Gestational age is NOT (three measurement eras). Do NOT use preterm as a
  primary outcome — use LBW.
- For 2009-2013, education and smoking are revised-certificate-only. Handle
  with explicit filtering (certificate_revision == 'revised_2003') or document
  the missingness.

### 8. TECHNICAL ENVIRONMENT
Primary: Python (pyarrow for streaming, DuckDB for aggregation, matplotlib/seaborn for figures)
The V2 file is ~1.5 GB. Use streaming/batch processing or DuckDB — do NOT
load into a pandas DataFrame whole.

### 9. OUTPUT HYGIENE
- If code changes, assume dependent outputs are stale until rerun.
- Never present stale outputs as current.
- Keep audit outputs in a separate output/audits/ folder.

### 10. SESSION CONTINUITY
At the start of each new session:
1. Read this entire document to understand current state
2. Check the Progress Log and Execution Log
3. Summarize: "Last session we completed X. Next step is Y."
```

---

## Table of Contents

0. [Project Setup & Reproducibility Standards](#0-project-setup--reproducibility-standards)
1. [Project Overview & Theoretical Framework](#1-project-overview--theoretical-framework)
2. [Technical Stack Decisions](#2-technical-stack-decisions)
3. [Phase 1: Data Extraction & Singleton LBW Trends](#3-phase-1-data-extraction--singleton-lbw-trends)
4. [Phase 2: Smoking Counterfactual](#4-phase-2-smoking-counterfactual)
5. [Phase 3: Kitagawa Decomposition of LBW Trends](#5-phase-3-kitagawa-decomposition-of-lbw-trends)
6. [Phase 4: Birthweight-Specific Infant Mortality](#6-phase-4-birthweight-specific-infant-mortality)
7. [Phase 5: The 2021 Surge Investigation](#7-phase-5-the-2021-surge-investigation)
8. [Phase 6: Sensitivity Analyses](#8-phase-6-sensitivity-analyses)
9. [Phase 7: Visualization & Tables](#9-phase-7-visualization--tables)
10. [Phase 8: Write-Up & Publication](#10-phase-8-write-up--publication)
11. [File Structure](#11-file-structure)
12. [Timeline](#12-timeline)
13. [Risk Mitigation, Audit Discipline & Stop Rules](#13-risk-mitigation-audit-discipline--stop-rules)
14. [Validation Targets](#14-validation-targets)
15. [Critical Files for Implementation](#15-critical-files-for-implementation)

---

## Progress Log

> **LLM updates this section after each completed phase**

### Phase 0: Project Setup
- [ ] Repository created at `~/Desktop/lbw-imr-divergence/`
- [ ] Directory structure created
- [ ] `requirements.txt` initialized
- [ ] `config.yml` created
- [ ] `.gitignore` created
- [ ] V2 Parquet copied from natality-harmonization and row count verified (138,582,904)
- [ ] V3 Parquet copied from natality-harmonization and row count verified (74,785,708)
- [ ] `data/PROVENANCE.md` created
- [ ] Reference docs copied (COMPARABILITY.md, CODEBOOK.md, validation targets)

### Phase 1: Data Extraction & Singleton LBW Trends
- [ ] Singleton/multiple LBW/VLBW rates extracted for all 35 years
- [ ] Aggregate LBW rates verified against NCHS external targets
- [ ] Race/ethnicity-stratified LBW trends extracted
- [ ] Phase 1 audit completed

### Phase 2: Smoking Counterfactual
- [ ] Smoking prevalence by year extracted (2003-2024)
- [ ] LBW by smoking status by year extracted
- [ ] Counterfactual LBW series computed
- [ ] Phase 2 audit completed

### Phase 3: Kitagawa Decomposition
- [ ] Primary decomposition (2003-2024, 8 factors) completed
- [ ] Factor-specific decomposition completed
- [ ] Time-series decomposition (5-year intervals) completed
- [ ] Sensitivity: 1990-2024 minimal model completed
- [ ] Sensitivity: 2014-2024 BMI-enriched model completed
- [ ] Phase 3 audit completed

### Phase 4: Birthweight-Specific Infant Mortality
- [ ] BW-specific IMR by year (2005-2023) computed
- [ ] IMR Kitagawa decomposition (composition vs. rate) completed
- [ ] 2005→2020 vs 2020→2023 comparison completed
- [ ] Phase 4 audit completed

### Phase 5: 2021 Surge Investigation
- [ ] 2019-2024 LBW by subgroup extracted
- [ ] Broad vs. concentrated pattern characterized
- [ ] Phase 5 audit completed

### Phase 6: Sensitivity Analyses
- [ ] S1: VLBW trends
- [ ] S2: Birthweight distribution analysis
- [ ] S3: Preterm-LBW vs term-LBW (2014-2024 only)
- [ ] S4: First-born singletons only
- [ ] S5: 2009-2013 education missingness sensitivity
- [ ] Phase 6 audit completed

### Phase 7: Visualization & Tables
- [ ] Figure 1: Singleton LBW trend 1990-2024
- [ ] Figure 2: Singleton LBW by race/ethnicity
- [ ] Figure 3: Observed vs. counterfactual (smoking) LBW
- [ ] Figure 4: Kitagawa composition vs. rate effects over time
- [ ] Figure 5: LBW-IMR divergence (dual-axis)
- [ ] Figure 6: BW-specific IMR trends
- [ ] All tables formatted for publication

### Phase 8: Write-Up
- [ ] Manuscript outline finalized
- [ ] Main draft written
- [ ] Supplement drafted
- [ ] References compiled
- [ ] Submission package assembled

---

## Execution Log

```text
(No entries yet — LLM adds timestamped entries as phases are completed)
```

---

## CRITICAL CONTEXT: VERIFIED DATA FINDINGS

> **READ THIS ENTIRE SECTION BEFORE STARTING ANY WORK.** These values were
> verified from hands-on data exploration and should be used as early
> stop-checks during implementation.

### A. Data Source Architecture

This project uses TWO Parquet files copied from the `natality-harmonization` repo into `data/`:

**V2 Natality (LBW trend analysis):**
```
data/natality_v2_residents_only.parquet
```
- 138,582,904 rows (resident births 1990-2024)
- 69 columns (61 harmonized + 8 derived + convenience filters)
- ~1.5 GB Parquet
- Validated against 183/183 external NCHS targets (0 failures)

**V3 Linked Birth-Infant Death (mortality analysis):**
```
data/natality_v3_linked_residents_only.parquet
```
- 74,785,708 rows (resident births 2005-2023, with linked death outcomes)
- 79 columns (68 harmonized + 11 derived)
- ~919 MB Parquet
- Validated against 35/35 external NCHS targets (0 failures)
- One row per birth (denominator-plus design): both survivors and deaths
- Birthweight on 99.97% of survivors and 99.4% of deaths

### B. Verified Singleton LBW Rates (1990-2024)

These were verified directly from the V2 residents-only parquet via DuckDB query. All aggregate LBW rates match NCHS-published NVSR figures within 0.04 ppt.

| Year | Singleton Births | Singleton LBW % | Aggregate LBW % | NCHS Published |
|------|-----------------|-----------------|-----------------|----------------|
| 1990 | 4,056,422 | 5.901 | 6.969 | 7.0 |
| 1995 | 3,794,202 | 5.964 | 7.320 | 7.3 |
| 2000 | 3,841,382 | 6.000 | 7.574 | 7.57 |
| 2005 | 3,995,267 | 6.382 | 8.189 | 8.2 |
| 2006 | 4,117,849 | 6.489 | 8.261 | 8.3 |
| 2007 | 4,128,818 | 6.440 | 8.218 | 8.2 |
| 2010 | 3,823,702 | 6.341 | 8.148 | 8.15 |
| 2012 | 3,812,252 | 6.257 | 7.995 | 7.99 |
| 2014 | 3,860,838 | 6.261 | 8.002 | 8.00 |
| 2016 | 3,837,541 | 6.396 | 8.166 | 8.17 |
| 2018 | 3,689,964 | 6.518 | 8.281 | 8.28 |
| 2020 | 3,523,210 | 6.679 | 8.242 | 8.24 |
| 2021 | 3,567,877 | 6.926 | 8.520 | 8.52 |
| 2022 | 3,536,742 | 7.012 | 8.603 | 8.60 |
| 2023 | 3,508,972 | 7.004 | 8.579 | 8.58 |
| 2024 | 3,501,866 | 6.999 | — | — |

**Key trajectory features:**
- Singleton LBW: 5.90% → 7.00% = **+1.10 ppt over 35 years** (18.6% relative increase)
- NOT monotonic: rose 1990-2006, dipped 2007-2014, then re-accelerated 2015-2024
- 2021 spike: +0.247 ppt in one year (largest single-year jump in the 35-year series)
- Multiple-birth share peaked at ~3.5% (2014), declined to 3.1% (2024) — multiples do NOT drive the aggregate trend

### C. Verified IMR Trends (2005-2023)

From the V3 linked validation (35/35 external targets pass):

| Year | Resident Births | Infant Deaths | IMR per 1,000 | Neonatal Deaths | Postneonatal Deaths |
|------|----------------|---------------|----------------|-----------------|---------------------|
| 2005 | 4,081,364 | 27,524 | 6.74 | 18,514 | 9,010 |
| 2010 | 3,953,590 | 23,864 | 6.04 | 15,738 | 8,126 |
| 2015 | 3,945,875 | 23,152 | 5.87 | 15,199 | 7,953 |
| 2020 | 3,605,201 | 19,286 | 5.35 | 12,587 | 6,699 |
| 2021 | 3,652,838 | 19,877 | 5.44 | 12,866 | 7,011 |
| 2022 | 3,623,468 | 20,020 | 5.53 | 12,938 | 7,082 |
| 2023 | 3,591,328 | 19,726 | 5.49 | 12,886 | 6,840 |

### D. Birthweight Comparability (the critical asset)

Birthweight (`birthweight_grams` / `birthweight_grams_clean`) is classified as **FULL comparability** across all 35 years. This is the single strongest variable in the dataset for trend analysis.

**Why this classification is well-supported:**
1. Raw-field missingness: 0% blank rate for `DBIRWT` (1990-2004) and `DBWT` (2003+) across every single year
2. Consistent sentinel: 9999 = unknown birthweight in both eras. Derive code converts to null: `_null_if_equal(bw, 9999)`
3. Simple direct mapping: integer grams, no transformation, no rounding, no recoding
4. All invariant checks pass: `bw9999_clean_not_null = 0`, `bw_out_of_range = 0`, `lbw_logic_mismatch = 0`
5. Computed LBW rates match NCHS-published NVSR rates to within 0.04 ppt for all 34 checked years

**There are NO known caveats, measurement changes, or reporting breaks for birthweight across 1990-2024.**

### E. Key Comparability Constraints for Risk Factors

| Variable | Full Comparability Window | Issue Outside Window |
|----------|--------------------------|----------------------|
| maternal_age_cat (6 groups) | 1990-2024 | 2003 uses MAGER41 recode; grouped categories absorb noise |
| maternal_race_ethnicity_5 | 2003-2024 (official bridge) | 1990-2002 uses approximate bridge from detail codes |
| marital_status | 1990-2024 | None |
| singleton | 1990-2024 | None |
| diabetes_any | 1990-2024 | U/R combined; fully comparable but ascertainment may differ across eras |
| hypertension_chronic | 1990-2024 | Same as diabetes |
| hypertension_gestational | 1990-2024 | Same as diabetes |
| maternal_education_cat4 | 2003-2024 (with 2009-2013 gap) | 1990-2002 mapped from years-of-schooling (different source); 2009-2013 revised-only (~25-30% missing) |
| smoking_any_during_pregnancy | 2003-2024 (clean) | 1990-2002 from independent TOBACCO field (different coding) |
| bmi_prepregnancy_recode6 | 2014-2024 | Not available before 2014 |
| payment_source_recode | 2014-2024 (near-full) | Partial 2011-2013 |

**Decision for Kitagawa tiers:**
- **Primary model (2003-2024)**: age, race/eth, marital, education, smoking, diabetes, chronic HTN, gestational HTN = 3,840 cells
- **Extended model (1990-2024)**: age, race/eth, marital, diabetes, chronic HTN, gestational HTN = 480 cells (no smoking or education)
- **BMI-enriched model (2014-2024)**: primary + BMI recode6 + payment source = ~69,120 cells (sparse; may need collapsing)

### F. Known Data Issues

1. **2009-2013 education missingness**: Public-use files have entirely blank education for unrevised-certificate areas. Use `certificate_revision == 'revised_2003'` filter or accept ~25-30% missing and use an "unknown" category. Document either approach.

2. **1990-2002 race bridge is approximate**: The natality-harmonization pipeline bridges 1990-2002 race from detail codes. This is NOT the official NCHS bridged race (which starts 2003). Expect ~0.5-1% category shifts at the 2003 boundary. Document in methods.

3. **Diabetes/hypertension ascertainment changes**: While the field is technically present for all 35 years, clinical ascertainment practices changed (e.g., GDM screening became universal ~2010). Rising diabetes rates partly reflect increased detection, not just increased prevalence. Kitagawa interpretation must account for this.

4. **2020 linked file race**: Bridged race is available for 2020 in the V3 linked file (unlike the V2 natality file where it was dropped for the 2020 natality-only release). Verified: `maternal_race_ethnicity_5` is populated for 2005-2023 in the linked convenience file.

5. **Stale key_rates CSV**: `/output/validation/key_rates_core_1990_2020.csv` contains incorrect values for 1990-1993 (pre-RECTYPE bug fix). Use the external validation comparison file or query the parquet directly for authoritative rates.

### G. Column Names in Source Parquets

**V2 columns needed for this analysis:**
```
# Universe
year, is_foreign_resident, singleton, certificate_revision

# Outcome
birthweight_grams_clean, low_birthweight, very_low_birthweight

# Risk factors (Tier 1: 1990-2024)
maternal_age_cat, maternal_race_ethnicity_5, marital_status,
diabetes_any, hypertension_chronic, hypertension_gestational

# Risk factors (Tier 2: 2003-2024)
maternal_education_cat4, smoking_any_during_pregnancy

# Risk factors (Tier 3: 2014-2024)
bmi_prepregnancy_recode6, payment_source_recode

# Supplementary
plurality_recode, live_birth_order_recode, infant_sex,
gestational_age_weeks_clean, gestational_age_weeks_source,
preterm_lt37, apgar5_clean
```

**V3 columns needed (adds death-side):**
```
# All V2 columns above, plus:
infant_death, neonatal_death, postneonatal_death,
age_at_death_days, age_at_death_recode5,
underlying_cause_icd10, cause_recode_130, cause_group,
manner_of_death
```

### H. What This Study Adds to the Literature

| Prior work | What they did | Gap we fill |
|-----------|---------------|-------------|
| NCHS annual NVSR reports (Martin, Osterman et al.) | Report LBW rates descriptively by year, race, age | No decomposition; no singleton isolation; no multi-decade synthesis |
| Callaghan et al. 2006, Pediatrics | Decomposed IMR trends 1990-2002 into preterm vs. birthweight-specific components | Only 12 years; focused on preterm, not LBW per se; predates post-2014 LBW acceleration |
| MacDorman & Mathews, NVSR reports | Published BW-specific IMR tables in government reports | Data in tables, but no cohesive trend analysis or decomposition as primary contribution |
| NICU survival literature (Philip et al., various) | Documented improving survival at low gestational ages | Mechanism known but never quantified at population level against worsening BW distribution |
| **This study** | 35-year singleton LBW trend + Kitagawa decomposition + smoking counterfactual + BW-specific IMR divergence + 2021 surge analysis | **First formal multi-decade Kitagawa decomposition of U.S. LBW trends; first unified LBW-IMR divergence narrative with quantitative decomposition; first smoking counterfactual for LBW** |

---

## 0. Project Setup & Reproducibility Standards

### 0.1 Repository Setup

This analysis lives in its OWN repo at `~/Desktop/lbw-imr-divergence/`, separate from the `natality-harmonization` data infrastructure repo. This follows the same pattern as `bmi-cause-specific-imr/` and `multiple-gestation-linked-imr/`.

```bash
cd ~/Desktop
mkdir lbw-imr-divergence
cd lbw-imr-divergence
git init

mkdir -p data
mkdir -p scripts
mkdir -p output/{tables,figures,models,validation,audits}
mkdir -p docs

touch README.md
touch .gitignore
touch config.yml
```

### 0.2 Data Acquisition (CRITICAL — do this first)

This project requires TWO Parquet files from the `natality-harmonization` repo. Copy them into `data/`:

```bash
# From inside lbw-imr-divergence/
NHARM=~/Desktop/natality-harmonization

# 1. V2 residents-only (LBW trends — Phases 1, 2, 3, 5, 6)
cp "$NHARM/output/convenience/natality_v2_residents_only.parquet" data/
#    Expected: ~1.5 GB, 138,597,636 rows, 69 columns

# 2. V3 linked residents-only (mortality analysis — Phase 4)
cp "$NHARM/output/convenience/natality_v3_linked_residents_only.parquet" data/
#    Expected: ~919 MB, 74,797,371 rows, 79 columns
```

**Verification after copy:**
```bash
# Check file sizes
ls -lh data/*.parquet
# Expected:
#   natality_v2_residents_only.parquet         ~1.5 GB
#   natality_v3_linked_residents_only.parquet   ~919 MB

# Quick row-count check (requires duckdb CLI or Python)
python3 -c "
import duckdb
con = duckdb.connect()
for f in ['data/natality_v2_residents_only.parquet',
          'data/natality_v3_linked_residents_only.parquet']:
    n = con.sql(f\"SELECT COUNT(*) FROM read_parquet('{f}')\").fetchone()[0]
    print(f'{f}: {n:,} rows')
"
# Expected:
#   natality_v2_residents_only.parquet: 138,582,904 rows
#   natality_v3_linked_residents_only.parquet: 74,785,708 rows
```

**What each file contains:**

| File | Rows | Cols | Years | Universe | Use in this project |
|------|------|------|-------|----------|---------------------|
| `natality_v2_residents_only.parquet` | 138.6M | 69 | 1990-2024 | US resident births (all) | Phases 1-3, 5-6: LBW trends, smoking counterfactual, Kitagawa, 2021 surge, sensitivity |
| `natality_v3_linked_residents_only.parquet` | 74.8M | 79 | 2005-2023 | US resident births with linked infant death | Phase 4: BW-specific IMR, IMR Kitagawa |

**You also need reference documentation (optional — copy or read in-place):**
```bash
# Copy key docs for reference (optional — can read from natality-harmonization directly)
cp "$NHARM/docs/COMPARABILITY.md" docs/comparability_reference.md
cp "$NHARM/docs/CODEBOOK.md" docs/codebook_reference.md
cp "$NHARM/metadata/external_validation_targets_v1.csv" data/
cp "$NHARM/metadata/external_validation_targets_v3_linked.csv" data/
```

### 0.3 Directory Structure

```
lbw-imr-divergence/
├── data/
│   ├── natality_v2_residents_only.parquet     # Source (~1.5 GB, NEVER modify)
│   ├── natality_v3_linked_residents_only.parquet  # Source (~919 MB, NEVER modify)
│   ├── external_validation_targets_v1.csv     # 183 NCHS targets (optional reference)
│   ├── external_validation_targets_v3_linked.csv  # 35 linked targets (optional reference)
│   └── PROVENANCE.md                          # Data provenance documentation
├── scripts/
│   ├── 01_extract_lbw_trends.py               # Phase 1: singleton/multiple/aggregate LBW
│   ├── 01a_audit_trends.py                    # Phase 1 audit
│   ├── 02_smoking_counterfactual.py           # Phase 2: smoking × LBW decomposition
│   ├── 02a_audit_smoking.py                   # Phase 2 audit
│   ├── 03_kitagawa_decomposition.py           # Phase 3: formal Kitagawa
│   ├── 03a_audit_kitagawa.py                  # Phase 3 audit
│   ├── 04_bw_specific_imr.py                  # Phase 4: BW-specific mortality from V3
│   ├── 04a_audit_mortality.py                 # Phase 4 audit
│   ├── 05_surge_2021.py                       # Phase 5: 2021 subgroup analysis
│   ├── 05a_audit_surge.py                     # Phase 5 audit
│   ├── 06_sensitivity.py                      # Phase 6: all sensitivity analyses
│   ├── 06a_audit_sensitivity.py               # Phase 6 audit
│   ├── 07_figures.py                          # All figures
│   └── 08_tables.py                           # Publication-formatted tables
├── output/
│   ├── tables/
│   │   ├── table1_singleton_lbw_by_year.csv
│   │   ├── table2_lbw_by_race_ethnicity.csv
│   │   ├── table3_smoking_counterfactual.csv
│   │   ├── table4_kitagawa_primary.csv
│   │   ├── table5_kitagawa_by_factor.csv
│   │   ├── table6_bw_specific_imr.csv
│   │   ├── table7_imr_kitagawa.csv
│   │   ├── table8_2021_surge_subgroups.csv
│   │   └── etables/ (supplementary)
│   ├── figures/
│   │   ├── figure1_singleton_lbw_trend.pdf
│   │   ├── figure2_lbw_by_race.pdf
│   │   ├── figure3_smoking_counterfactual.pdf
│   │   ├── figure4_kitagawa_over_time.pdf
│   │   ├── figure5_lbw_imr_divergence.pdf
│   │   ├── figure6_bw_specific_imr_trends.pdf
│   │   └── efigures/ (supplementary)
│   ├── validation/
│   │   ├── phase1_audit.json
│   │   ├── phase2_audit.json
│   │   ├── phase3_audit.json
│   │   ├── phase4_audit.json
│   │   ├── phase5_audit.json
│   │   └── phase6_audit.json
│   └── audits/ (independent audit recomputation outputs)
├── docs/
│   ├── ANALYSIS_PLAN_LBW_DIVERGENCE.md        # This file (primary source of truth)
│   ├── comparability_reference.md             # Copied from natality-harmonization
│   ├── codebook_reference.md                  # Copied from natality-harmonization
│   └── manuscript_draft.md                    # Written in Phase 8
├── config.yml
├── requirements.txt
├── .gitignore
└── README.md
```

### 0.4 .gitignore

```
# Data files (too large for git)
data/*.parquet
data/*.csv
!data/PROVENANCE.md

# Python artifacts
__pycache__/
*.pyc
.venv/

# Outputs (regenerated from code)
output/figures/*.png
output/figures/*.pdf
output/tables/*.csv
output/models/*.csv
output/validation/*.json
output/audits/*.csv

# System
.DS_Store
```

### 0.5 Data Provenance (`data/PROVENANCE.md`)

```markdown
# Data Provenance

## Source Repository
- Repo: natality-harmonization (local: ~/Desktop/natality-harmonization/)
- Documentation: docs/COMPARABILITY.md, docs/CODEBOOK.md, docs/VALIDATION.md

## File 1: natality_v2_residents_only.parquet
- Source path: natality-harmonization/output/convenience/natality_v2_residents_only.parquet
- Rows: 138,582,904 (US resident births, 1990-2024)
- Columns: 69
- Size: ~1.5 GB
- Built from: NCHS public-use natality fixed-width files (35 annual files)
- Validated against: 183/183 external NCHS targets (0 failures)
- Key variables: birthweight_grams_clean, low_birthweight, very_low_birthweight,
  singleton, maternal_age_cat, maternal_race_ethnicity_5, marital_status,
  maternal_education_cat4, smoking_any_during_pregnancy, diabetes_any,
  hypertension_chronic, hypertension_gestational, bmi_prepregnancy_recode6,
  payment_source_recode, certificate_revision

## File 2: natality_v3_linked_residents_only.parquet
- Source path: natality-harmonization/output/convenience/natality_v3_linked_residents_only.parquet
- Rows: 74,785,708 (US resident births with linked infant death, 2005-2023)
- Columns: 79
- Size: ~919 MB
- Built from: NCHS cohort linked birth-infant death public-use files (19 annual files)
- Validated against: 35/35 external NCHS targets (0 failures)
- Key additional variables: infant_death, neonatal_death, postneonatal_death,
  age_at_death_days, underlying_cause_icd10, cause_group

## How to Reproduce
1. Clone or access the natality-harmonization repo
2. Run the full pipeline (see natality-harmonization/docs/GETTING_STARTED.md)
3. Copy the two convenience files into this repo's data/ directory
```

### 0.6 Python Environment

```
# requirements.txt
pyarrow>=14.0
duckdb>=0.9
pandas>=2.0
numpy>=1.24
matplotlib>=3.8
seaborn>=0.13
pyyaml>=6.0
```

### 0.4 Configuration File

```yaml
# config.yml (in repo root: ~/Desktop/lbw-imr-divergence/config.yml)
project:
  name: "lbw-imr-divergence"
  version: "0.1.0"

paths:
  v2_residents: "data/natality_v2_residents_only.parquet"
  v3_linked_residents: "data/natality_v3_linked_residents_only.parquet"
  output_base: "output"
  tables: "output/tables"
  figures: "output/figures"
  validation: "output/validation"
  audits: "output/audits"

analysis:
  random_seed: 42

  # Universe
  residents_only: true      # Already filtered in convenience files
  singleton_filter: true     # Primary analysis on singletons

  # Outcome
  lbw_threshold: 2500        # grams
  vlbw_threshold: 1500       # grams

  # Kitagawa primary model: 2003-2024
  kitagawa_primary:
    year_start: 2003
    year_end: 2024
    reference_year: 2003
    comparison_year: 2024
    interval_years: [2003, 2008, 2013, 2018, 2024]
    factors:
      - maternal_age_cat           # 6 levels
      - maternal_race_ethnicity_5  # 5 levels
      - marital_status             # 2 levels (exclude unknown)
      - maternal_education_cat4    # 4 levels (handle 2009-2013 missingness)
      - smoking_any_during_pregnancy  # 2 levels
      - diabetes_any               # 2 levels (exclude unknown)
      - hypertension_chronic       # 2 levels (exclude unknown)
      - hypertension_gestational   # 2 levels (exclude unknown)
    expected_cells: 3840       # 6×5×2×4×2×2×2×2

  # Kitagawa extended model: 1990-2024
  kitagawa_extended:
    year_start: 1990
    year_end: 2024
    reference_year: 1990
    comparison_year: 2024
    factors:
      - maternal_age_cat
      - maternal_race_ethnicity_5
      - marital_status
      - diabetes_any
      - hypertension_chronic
      - hypertension_gestational
    expected_cells: 480        # 6×5×2×2×2×2

  # Kitagawa BMI-enriched: 2014-2024
  kitagawa_bmi:
    year_start: 2014
    year_end: 2024
    reference_year: 2014
    comparison_year: 2024
    additional_factors:
      - bmi_prepregnancy_recode6   # 6 levels
    expected_cells: 23040      # 3840×6

  # BW-specific IMR categories
  bw_categories:
    - label: "<1000g"
      min: 0
      max: 999
    - label: "1000-1499g"
      min: 1000
      max: 1499
    - label: "1500-2499g"
      min: 1500
      max: 2499
    - label: "2500-3999g"
      min: 2500
      max: 3999
    - label: "4000+g"
      min: 4000
      max: 9998

  # Smoking counterfactual
  smoking:
    clean_era_start: 2003
    clean_era_end: 2024
    reference_year: 2003

  # 2021 surge
  surge:
    window_start: 2019
    window_end: 2024

  # Education missingness handling
  education_2009_2013:
    strategy: "revised_only"   # Options: "revised_only", "unknown_category", "exclude_years"
```

---

## 1. Project Overview & Theoretical Framework

### 1.1 Research Question

Among U.S. resident births (1990-2024), how has the singleton low birthweight rate changed over 35 years, what compositional and residual factors explain the change, and how has the divergence between rising LBW and falling infant mortality been sustained?

### 1.2 Core Hypotheses

| # | Hypothesis | Test |
|---|-----------|------|
| H1 | Singleton LBW has risen broadly across racial/ethnic groups, not just in select subpopulations | Phase 1: race-stratified LBW trends |
| H2 | The smoking decline prevented ~0.5-0.7 ppt of potential LBW increase; counterfactual rise is ~1.6-1.8 ppt | Phase 2: smoking counterfactual |
| H3 | Compositional shifts (maternal age, diabetes, hypertension) explain 40-60% of the singleton LBW rise; a meaningful unexplained residual remains | Phase 3: Kitagawa decomposition |
| H4 | BW-specific IMR improved steadily 2005-2019, offsetting the worsening BW distribution; this improvement stalled in 2021-2023 | Phase 4: BW-specific IMR trends |
| H5 | The 2021 LBW surge was broadly distributed (systemic, not subgroup-specific) | Phase 5: subgroup analysis |

### 1.3 Conceptual Framework

```
COMPOSITION EFFECTS                    RATE EFFECTS
(risk profile worsening)               (within-stratum changes)

Rising maternal age ─────────┐         Medical/obstetric advances ──┐
Rising diabetes prevalence ──┤         NICU technology ─────────────┤
Rising hypertension ─────────┤         → These REDUCE BW-specific   │
Rising obesity (2014+) ──────┤           mortality (mask the        │
Declining smoking ───────────┤           composition deterioration) │
(PROTECTIVE — offsets above) │                                      │
                             │                                      │
         ┌───────────────────┘         ┌────────────────────────────┘
         ▼                             ▼
    LBW RATE                       INFANT MORTALITY
    (rising)                       (falling... until 2021)
         │                             │
         └──────────── DIVERGENCE ─────┘
                    (this paper)
```

**Kitagawa decomposition** partitions the observed LBW change into:
- **Composition effect**: change attributable to shifting proportions across risk strata (more older mothers, fewer smokers, etc.)
- **Rate effect**: change attributable to changing LBW rates within identical risk strata (unexplained residual — potentially driven by unmeasured factors like obesity pre-2014, environmental exposures, stress, substance use)

This is a purely arithmetic identity: `total Δ = composition Δ + rate Δ`. No causal claims.

### 1.4 What's New

No published paper:
1. Frames the 35-year singleton LBW rise as its central subject (NCHS reports it descriptively)
2. Provides a formal Kitagawa decomposition of U.S. LBW trends over multiple decades
3. Quantifies the smoking counterfactual ("how much worse would LBW be without the smoking decline?")
4. Presents the LBW-IMR divergence as a unified decomposition connecting two trends usually studied separately
5. Examines the 2021 singleton LBW surge as a distinct analytical question

Closest precedent: Callaghan et al. 2006 (Pediatrics) decomposed IMR trends 1990-2002 into preterm birth vs. BW-specific mortality components. Our study extends this framework by 20+ additional years, uses LBW instead of preterm (avoiding the gestational age measurement artifact), adds the Kitagawa risk-factor decomposition, and includes the smoking counterfactual.

---

## 2. Technical Stack Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Data aggregation | DuckDB (in-process) | Handles the 138M-row V2 parquet without loading into memory; SQL syntax is clear and auditable |
| Kitagawa computation | Python (numpy/pandas on aggregated cells) | Cells are small (~3,840 rows); no big-data tooling needed after aggregation |
| Smoking counterfactual | Python (pandas) | Simple weighted-average computation on yearly aggregates |
| BW-specific IMR | DuckDB on V3 linked parquet | Same rationale as V2 |
| Figures | matplotlib + seaborn | Consistent with other project outputs |
| Tables | pandas → CSV → manual formatting | Standard workflow |

**Memory management**: The V2 residents-only parquet is ~1.5 GB. DuckDB handles this natively without loading into pandas. All analysis scripts should use DuckDB for the initial aggregation step, then work with the resulting summary DataFrames (typically <10K rows).

---

## 3. Phase 1: Data Extraction & Singleton LBW Trends

### 3.1 Script: `01_extract_lbw_trends.py`

**Inputs:** V2 residents-only parquet

**Outputs:**
- `table1_singleton_lbw_by_year.csv`: Year, singleton births, singleton LBW count, LBW %, VLBW count, VLBW %, aggregate births, aggregate LBW %, multiple births, multiple LBW %
- `table2_lbw_by_race_ethnicity.csv`: Year × race/ethnicity (5 categories) × singleton LBW % (with 1990-2002 bridge caveat flag)
- `yearly_birth_counts.csv`: Year, total births, singleton births, multiple births, multiple share %

**Method:**
```sql
-- Core singleton LBW query (DuckDB)
SELECT
    year,
    COUNT(*) AS singleton_births,
    SUM(CASE WHEN low_birthweight THEN 1 ELSE 0 END) AS singleton_lbw,
    ROUND(100.0 * SUM(CASE WHEN low_birthweight THEN 1 ELSE 0 END)
          / NULLIF(SUM(CASE WHEN birthweight_grams_clean IS NOT NULL THEN 1 ELSE 0 END), 0), 3)
      AS singleton_lbw_pct,
    SUM(CASE WHEN very_low_birthweight THEN 1 ELSE 0 END) AS singleton_vlbw,
    ROUND(100.0 * SUM(CASE WHEN very_low_birthweight THEN 1 ELSE 0 END)
          / NULLIF(SUM(CASE WHEN birthweight_grams_clean IS NOT NULL THEN 1 ELSE 0 END), 0), 3)
      AS singleton_vlbw_pct
FROM read_parquet('data/natality_v2_residents_only.parquet')
WHERE singleton = true
GROUP BY year
ORDER BY year
```

Race/ethnicity stratification uses the same pattern with a `GROUP BY year, maternal_race_ethnicity_5` and a flag column indicating whether the year is in the approximate-bridge era (1990-2002) or the official-bridge era (2003+).

### 3.2 Verification Checklist

| Check | Expected | Verified? |
|-------|----------|-----------|
| Total years | 35 (1990-2024) | [ ] |
| Singleton LBW 1990 | 5.901% (+/- 0.01) | [ ] |
| Singleton LBW 2022 | 7.012% (+/- 0.01) | [ ] |
| Singleton LBW 2024 | 6.999% (+/- 0.01) | [ ] |
| Aggregate LBW 2000 | 7.574% (NCHS: 7.57) | [ ] |
| Aggregate LBW 2010 | 8.148% (NCHS: 8.15) | [ ] |
| Aggregate LBW 2020 | 8.242% (NCHS: 8.24) | [ ] |
| Singleton + multiple births = total births (each year) | Exact | [ ] |
| 5 race/ethnicity groups sum to total (each year) | Within 0.5% (some unknown) | [ ] |
| Race/ethnicity rows flagged for 1990-2002 bridge era | Yes | [ ] |

---

## 4. Phase 2: Smoking Counterfactual

### 4.1 Script: `02_smoking_counterfactual.py`

**Window:** 2003-2024 (clean smoking variable era)

**Inputs:** V2 residents-only parquet, filtered to singletons and `year >= 2003`

**Outputs:**
- `table3_smoking_counterfactual.csv`: Year, smoking prevalence, smoker LBW %, non-smoker LBW %, observed LBW %, counterfactual LBW (fixed smoking at 2003 level), counterfactual LBW (fixed non-smoking LBW at 2003 level)

**Method:**

For each year $t$, let:
- $p_t$ = proportion who smoked in year $t$
- $r_{s,t}$ = LBW rate among smokers in year $t$
- $r_{n,t}$ = LBW rate among non-smokers in year $t$

Observed LBW: $L_t = p_t \cdot r_{s,t} + (1 - p_t) \cdot r_{n,t}$

**Counterfactual A** — "What if smoking prevalence stayed at 2003 levels?":
$L_t^{A} = p_{2003} \cdot r_{s,t} + (1 - p_{2003}) \cdot r_{n,t}$

**Counterfactual B** — "What if only smoking prevalence changed but within-group LBW rates stayed at 2003 levels?":
$L_t^{B} = p_t \cdot r_{s,2003} + (1 - p_t) \cdot r_{n,2003}$

The difference $L_t^{A} - L_t$ = the protective effect of the smoking decline in year $t$.
The difference $L_t - L_t^{B}$ = the effect of changing within-group LBW rates (the non-smoking forces pushing LBW up).

**Note on 2009-2013 education missingness:** Smoking is available for revised-certificate records across 2003-2024. For 2009-2013, smoking coverage may be reduced in unrevised areas (check `smoking_any_during_pregnancy` null rate by year and document). If >10% missing, restrict to `certificate_revision == 'revised_2003'` for those years and document the sample reduction.

### 4.2 Verification Checklist

| Check | Expected | Verified? |
|-------|----------|-----------|
| Smoking prevalence 2003 | ~10-11% | [ ] |
| Smoking prevalence 2023 | ~3% | [ ] |
| Smoker LBW rate > non-smoker LBW rate (all years) | Yes | [ ] |
| Counterfactual A > observed (all years after 2003) | Yes (fixed higher smoking → higher LBW) | [ ] |
| Counterfactual B shows lower trajectory if only smoking changed | Yes | [ ] |
| Observed = p*r_s + (1-p)*r_n identity holds (each year) | Exact to 3 decimals | [ ] |
| Smoking null rate by year documented | Yes | [ ] |

---

## 5. Phase 3: Kitagawa Decomposition of LBW Trends

### 5.1 Script: `03_kitagawa_decomposition.py`

**Method:** Standard Kitagawa (two-factor) decomposition.

For two time points (reference year $r$ and comparison year $c$), with population divided into $i = 1, ..., k$ strata:

Let $w_{i,t}$ = proportion of births in stratum $i$ at time $t$, and $f_{i,t}$ = LBW rate in stratum $i$ at time $t$.

Overall LBW at time $t$: $F_t = \sum_i w_{i,t} \cdot f_{i,t}$

**Kitagawa decomposition:**
$$F_c - F_r = \underbrace{\sum_i \bar{f}_i (w_{i,c} - w_{i,r})}_{\text{composition effect}} + \underbrace{\sum_i \bar{w}_i (f_{i,c} - f_{i,r})}_{\text{rate effect}}$$

where $\bar{f}_i = (f_{i,c} + f_{i,r})/2$ and $\bar{w}_i = (w_{i,c} + w_{i,r})/2$.

This is the symmetric form (Das Gupta standardization). The composition + rate effects sum exactly to the total observed change — this is an identity, not an approximation.

### 5.2 Primary Decomposition (2003-2024)

**Strata:** maternal_age_cat (6) × maternal_race_ethnicity_5 (5) × marital_status (2) × maternal_education_cat4 (4) × smoking_any_during_pregnancy (2) × diabetes_any (2) × hypertension_chronic (2) × hypertension_gestational (2) = **3,840 cells**

**Handling unknowns/missing:** For each factor, code unknown/missing (value 9 or null) as a separate level. This preserves all births in the denominator. Document the proportion of unknowns by factor and year.

**Education 2009-2013:** Use `certificate_revision == 'revised_2003'` filter for 2009-2013 records only. Document the sample reduction (expect ~25-30% fewer births in those years for the Kitagawa cells, but the remaining records are fully comparable). Alternatively, use an "unknown" education category for unrevised records — test both approaches.

**Outputs:**
- `table4_kitagawa_primary.csv`: Total Δ LBW, composition effect, rate effect, % composition, % rate
- `table5_kitagawa_by_factor.csv`: Decomposition repeated for each factor individually (holding others at observed levels)
- `kitagawa_timeseries.csv`: Decomposition at 5-year intervals (2003→2008, 2003→2013, 2003→2018, 2003→2024)
- `kitagawa_cells_2003.csv` and `kitagawa_cells_2024.csv`: Raw cell-level data (weights and rates) for reproducibility

### 5.3 Sensitivity Decompositions

**Extended (1990-2024):** 6 factors, 480 cells. Same method but without smoking and education. Uses the approximate 1990-2002 race bridge. Document the bridge caveat.

**BMI-enriched (2014-2024):** Primary factors + BMI recode6 = ~23,040 cells. Many will be sparse. Collapse cells with <100 births into adjacent BMI categories (e.g., merge Obesity II + III). Document any collapsing.

### 5.4 Verification Checklist

| Check | Expected | Verified? |
|-------|----------|-----------|
| Composition + rate = total Δ (exact) | Identity must hold to <0.001 ppt | [ ] |
| Sum of cell weights = 1.0 for each year | Exact | [ ] |
| Sum of cell births = total singleton births for each year | Exact | [ ] |
| Weighted average of cell LBW rates = observed overall LBW for each year | Exact to 3 decimals | [ ] |
| Factor-specific decompositions each sum to total Δ | Yes (approximately — single-factor decompositions don't perfectly add due to interaction terms) | [ ] |
| Empty cells flagged and documented | Yes | [ ] |
| 2009-2013 education handling documented | Yes | [ ] |

---

## 6. Phase 4: Birthweight-Specific Infant Mortality

### 6.1 Script: `04_bw_specific_imr.py`

**Inputs:** V3 linked residents-only parquet

**Outputs:**
- `table6_bw_specific_imr.csv`: Year × BW category × births × deaths × IMR per 1,000
- `table7_imr_kitagawa.csv`: Kitagawa decomposition of overall IMR change into BW-composition effect vs. BW-specific rate effect, for 2005→2020 and 2020→2023
- `bw_distribution_by_year.csv`: Proportion of births in each BW category, by year

**BW categories:**
- <1000g (extreme LBW)
- 1000-1499g (very LBW)
- 1500-2499g (moderate LBW)
- 2500-3999g (normal)
- 4000+g (macrosomic)

**IMR Kitagawa method:** Same symmetric Kitagawa formula, where strata = BW categories, weights = proportion of births in each BW category, and rates = BW-specific IMR.

$$\Delta \text{IMR} = \underbrace{\sum_j \bar{m}_j (w_{j,c} - w_{j,r})}_{\text{BW composition effect}} + \underbrace{\sum_j \bar{w}_j (m_{j,c} - m_{j,r})}_{\text{BW-specific rate effect}}$$

For 2005→2020: expect the rate effect to dominate (IMR fell because BW-specific mortality improved faster than the BW distribution worsened).

For 2020→2023: test whether the balance shifted (BW-specific mortality improvement stalling while BW distribution continues worsening).

### 6.2 Verification Checklist

| Check | Expected | Verified? |
|-------|----------|-----------|
| Total deaths by year match V3 linked validation targets | Exact or within 1-2 | [ ] |
| IMR 2005 = 6.74-6.75 | Yes | [ ] |
| IMR 2020 = 5.35 | Yes | [ ] |
| IMR 2023 = 5.48-5.49 | Yes | [ ] |
| BW category births sum to total births (each year) | Exact (after excluding null BW) | [ ] |
| BW category deaths sum to total deaths (each year) | Exact (after excluding null BW) | [ ] |
| Kitagawa composition + rate = total Δ IMR | Identity holds | [ ] |
| BW-specific IMR at <1000g is ~500-900 per 1,000 | Plausible | [ ] |
| BW-specific IMR at 2500-3999g is ~1-3 per 1,000 | Plausible | [ ] |

---

## 7. Phase 5: The 2021 Surge Investigation

### 7.1 Script: `05_surge_2021.py`

**Window:** 2019-2024

**Inputs:** V2 residents-only parquet, filtered to singletons and year 2019-2024

**Outputs:**
- `table8_2021_surge_subgroups.csv`: Singleton LBW % by year, stratified by: race/ethnicity (5), maternal age group (6), education (4), smoking (2), diabetes (2), chronic HTN (2), gestational HTN (2), payment source (3, 2014+)

**Key question:** Was the 2021 spike (+0.247 ppt) concentrated in specific subgroups, or was it broadly distributed? If broadly distributed, this suggests a systemic exposure (COVID infection, pandemic-related stress/disruption). If concentrated, it points to specific risk pathways.

### 7.2 Verification Checklist

| Check | Expected | Verified? |
|-------|----------|-----------|
| Overall singleton LBW matches Phase 1 for 2019-2024 | Exact | [ ] |
| Weighted average across subgroups = overall (each year × factor) | Exact | [ ] |
| 2021 LBW > 2020 LBW for most subgroups | Yes (if broadly distributed) | [ ] |

---

## 8. Phase 6: Sensitivity Analyses

### S1. VLBW Trends
Repeat Phase 1 for `very_low_birthweight` (<1500g). If VLBW is flat while LBW (1500-2499g) is rising, the increase is in moderate LBW — different clinical implications than if VLBW is also rising.

### S2. Birthweight Distribution
Extract the full BW distribution (25g or 50g bins) at key timepoints (1990, 2000, 2010, 2020, 2024). Plot overlapping density curves. Test whether the distribution shifted leftward uniformly or whether specific regions changed.

### S3. Preterm-LBW vs. Term-LBW (2014-2024 only)
Within the obstetric-estimate era (consistent gestational age measurement), decompose singleton LBW into:
- Preterm LBW (GA <37 AND BW <2500g): prematurity-driven
- Term LBW (GA ≥37 AND BW <2500g): growth restriction at term

Is the LBW rise driven by more preterm births or by more growth restriction among term infants?

### S4. First-Born Singletons Only
Restrict to `live_birth_order_recode == 1` (first births). This controls for rising share of first births among older mothers (compositional shift in parity).

### S5. Education Missingness
Compare the primary Kitagawa results using three approaches for 2009-2013 education:
1. Revised-certificate-only (baseline)
2. Unknown-education category (includes all births)
3. Exclude 2009-2013 entirely

If all three produce similar decomposition results, the education handling is not driving findings.

---

## 9. Phase 7: Visualization & Tables

### Main Figures

| Figure | Description | Key message |
|--------|-------------|-------------|
| 1 | Singleton LBW rate 1990-2024 (line chart), with aggregate LBW overlay | Singleton LBW has risen 1.1 ppt; multiples aren't driving it |
| 2 | Singleton LBW by race/ethnicity 1990-2024 (5-line panel) | Rise is broadly distributed (or concentrated in specific groups) |
| 3 | Observed vs. counterfactual (smoking) singleton LBW 2003-2024 | Smoking decline masked ~0.5-0.7 ppt of additional LBW increase |
| 4 | Kitagawa composition vs. rate effects at 5-year intervals | Shows whether the unexplained residual is growing |
| 5 | LBW rate vs. IMR 2005-2023 (dual-axis divergence chart) | The central visual — diverging trends with 2021 inflection |
| 6 | BW-specific IMR trends 2005-2023 (one line per BW category) | Within-category improvement masking distributional worsening |

### Main Tables

| Table | Description |
|-------|-------------|
| 1 | Singleton and aggregate LBW/VLBW rates by year, 1990-2024 |
| 2 | Singleton LBW by race/ethnicity, 1990-2024 |
| 3 | Smoking prevalence, smoker/non-smoker LBW, and counterfactual LBW, 2003-2024 |
| 4 | Kitagawa decomposition: total Δ, composition effect, rate effect (2003→2024) |
| 5 | Kitagawa decomposition by individual risk factor |
| 6 | BW-specific IMR by year, 2005-2023 |
| 7 | Kitagawa decomposition of IMR into BW-composition vs. BW-specific rate effects |
| 8 | 2021 surge: singleton LBW by subgroup, 2019-2024 |

### Supplementary

- eTable 1: Kitagawa sensitivity (1990-2024 extended model)
- eTable 2: Kitagawa sensitivity (2014-2024 BMI-enriched model)
- eTable 3: Education missingness sensitivity
- eFigure 1: VLBW trend 1990-2024
- eFigure 2: Birthweight distribution at 5 timepoints
- eFigure 3: Preterm-LBW vs. term-LBW decomposition (2014-2024)
- eFigure 4: First-born singleton sensitivity

---

## 10. Phase 8: Write-Up & Publication

### Target Journals (ordered by stretch → safe)

| Journal | IF | Fit | Notes |
|---------|-----|-----|-------|
| JAMA | 120 | Stretch | Only if Kitagawa residual is large and narrative is compelling |
| Lancet | 98 | Stretch | Same — needs a "crisis" punchline |
| JAMA Pediatrics | 26 | Realistic upper | Strong fit for LBW + IMR + smoking counterfactual |
| Pediatrics | 8 | Realistic mid | Core audience for this question |
| AJPH | 11 | Realistic mid | Decomposition + policy angle |
| BMJ Open | 3 | Fallback | If rejected from above |

### Manuscript Structure

1. **Introduction**: LBW rising 35 years; smoking collapsed; IMR fell. How do these coexist?
2. **Methods**: Data sources, Kitagawa decomposition, smoking counterfactual, BW-specific IMR analysis
3. **Results**: (a) Singleton LBW trends; (b) Smoking counterfactual; (c) Kitagawa decomposition; (d) BW-specific IMR divergence; (e) 2021 surge
4. **Discussion**: Compositional vs. residual drivers; hidden NICU progress; potential inflection in 2021; limitations
5. **Supplement**: Sensitivity analyses, extended Kitagawa, BMI enrichment, education handling

---

## 11. File Structure

See Section 0.3 for full directory tree. The repo lives at `~/Desktop/lbw-imr-divergence/`.

---

## 12. Timeline

| Phase | Estimated sessions | Dependencies |
|-------|-------------------|--------------|
| 0: Setup | 0.5 | None |
| 1: LBW trends | 1 | Phase 0 |
| 2: Smoking counterfactual | 1 | Phase 0 |
| 3: Kitagawa decomposition | 2 | Phases 1, 2 (for validation) |
| 4: BW-specific IMR | 1 | Phase 0 |
| 5: 2021 surge | 0.5 | Phase 1 |
| 6: Sensitivity | 1 | Phases 1-5 |
| 7: Figures + tables | 1 | Phases 1-6 |
| 8: Write-up | 2-3 | All phases |
| **Total** | **~10-12 sessions** | |

Phases 1, 2, and 4 can be executed in parallel (independent data sources and computations). Phase 3 depends on Phase 1 outputs for validation. Phase 5 depends on Phase 1 for context.

---

## 13. Risk Mitigation, Audit Discipline & Stop Rules

### Known Risks

| Risk | Severity | Mitigation |
|------|----------|------------|
| Kitagawa residual is small (everything explained by known factors) | High (weakens paper) | Fall back to mid-tier framing: "first quantification of LBW compositional shift"; emphasize smoking counterfactual and IMR divergence as standalone contributions |
| 2007-2014 LBW dip complicates "continuous rise" narrative | Medium | Investigate dip in Phase 1 (coincides with Great Recession fertility selection + declining multiples); frame as "temporary improvement overwhelmed by secular forces" |
| 2009-2013 education missingness biases Kitagawa | Medium | Test three handling strategies in Phase 6 S5; if results diverge, restrict primary to revised-certificate-only and document |
| Diabetes/HTN ascertainment changes confound compositional interpretation | Medium | Document that rising reported diabetes partly reflects increased screening, not just true prevalence increase; note this as a limitation |
| Reviewer says "NCHS already reports this" | Medium | Differentiate clearly: NCHS reports rates descriptively; we decompose them formally. The Kitagawa, smoking counterfactual, and BW-specific IMR Kitagawa are all novel. |
| Paper scooped before submission | Low | Execute efficiently; the 35-year harmonized dataset is our moat |

### Audit Discipline

Every phase has a paired `*a_audit_*.py` script that independently recomputes key outputs from raw data. Audit scripts must:
1. Read from source parquet (not from prior-phase outputs)
2. Recompute at least 3 key metrics independently
3. Check summation identities
4. Compare to external NCHS validation targets where available
5. Write results to `output/analysis/lbw_divergence/validation/`

### Stop Rules

1. **Kitagawa identity violation**: If composition + rate ≠ total Δ (beyond rounding at 4th decimal), STOP. This indicates a bug.
2. **External target mismatch**: If computed aggregate LBW rate differs from NCHS-published rate by >0.1 ppt for any year, STOP and investigate.
3. **Singleton + multiple ≠ total**: If singleton and multiple births don't sum to total resident births for any year, STOP.
4. **Cell weights don't sum to 1.0**: If Kitagawa cell proportions don't sum to 1.0 (±0.001) for any year, STOP.

---

## 14. Validation Targets

### External Targets (from NCHS NVSR publications)

| Metric | Year | Expected | Tolerance | Source |
|--------|------|----------|-----------|--------|
| Aggregate LBW % | 1990 | 7.0 | ±0.05 | NVSR |
| Aggregate LBW % | 2000 | 7.57 | ±0.05 | NVSR |
| Aggregate LBW % | 2005 | 8.2 | ±0.05 | NVSR |
| Aggregate LBW % | 2010 | 8.15 | ±0.05 | NVSR |
| Aggregate LBW % | 2015 | 8.07 | ±0.05 | NVSR |
| Aggregate LBW % | 2020 | 8.24 | ±0.05 | NVSR |
| Aggregate LBW % | 2022 | 8.60 | ±0.05 | NVSR |
| Aggregate LBW % | 2023 | 8.58 | ±0.05 | NVSR |
| IMR per 1,000 | 2005 | 6.74-6.75 | ±0.02 | NVSR linked |
| IMR per 1,000 | 2010 | 6.03-6.04 | ±0.02 | NVSR linked |
| IMR per 1,000 | 2015 | 5.86-5.87 | ±0.02 | NVSR linked |
| IMR per 1,000 | 2020 | 5.35 | ±0.02 | NVSR linked |
| IMR per 1,000 | 2023 | 5.49 | ±0.02 | NVSR linked |

### Internal Consistency Targets

| Check | Condition |
|-------|-----------|
| Singleton LBW 1990 | 5.90 ± 0.01% (from prior query) |
| Singleton LBW 2022 | 7.01 ± 0.01% (from prior query) |
| Singleton LBW 2024 | 7.00 ± 0.01% (from prior query) |
| 2021 year-over-year Δ singleton LBW | +0.24-0.25 ppt |
| Kitagawa identity | composition + rate = total Δ (exact) |
| IMR Kitagawa identity | BW-composition + BW-rate = total Δ IMR (exact) |
| Smoker LBW > non-smoker LBW | All years 2003-2024 |

---

## 15. Critical Files for Implementation

### Source Data (in this repo)
- `data/natality_v2_residents_only.parquet` — V2 LBW trends (138,582,904 rows, 1990-2024)
- `data/natality_v3_linked_residents_only.parquet` — V3 mortality analysis (74,785,708 rows, 2005-2023)
- `data/external_validation_targets_v1.csv` — 183 NCHS comparison targets (optional)
- `data/external_validation_targets_v3_linked.csv` — 35 linked validation targets (optional)

### Reference Documentation (in this repo, copied from natality-harmonization)
- `docs/comparability_reference.md` — variable comparability rules
- `docs/codebook_reference.md` — variable definitions and coding

### Original Documentation (in natality-harmonization repo, read in-place if needed)
- `~/Desktop/natality-harmonization/docs/COMPARABILITY.md` — canonical comparability source
- `~/Desktop/natality-harmonization/docs/CODEBOOK.md` — canonical codebook
- `~/Desktop/natality-harmonization/metadata/harmonized_schema.csv` — machine-readable schema
- `~/Desktop/natality-harmonization/output/validation/external_validation_v1_comparison.csv` — V2 validation results (183 pass)
- `~/Desktop/natality-harmonization/output/validation/external_validation_v3_linked_comparison.csv` — V3 validation results (35 pass)

---

## Honest Assessment

### What determines the journal tier

The Kitagawa residual is the pivotal finding. If the rate effect (unexplained within-stratum LBW increase) is large after accounting for age, race, education, smoking, diabetes, and hypertension, the paper becomes: "Something unmeasured is making American babies smaller, and we can quantify it." That's a public health alarm with clear implications.

If the residual is small (known factors explain most of the LBW rise), the paper is a well-executed first-ever formal quantification — still publishable (Pediatrics, AJPH) but confirmatory rather than alarming.

### Strongest elements

1. **Singleton focus** removes the plurality confound that contaminates aggregate LBW trends
2. **Smoking counterfactual** quantifies a hidden protective effect that no prior paper has isolated
3. **LBW-IMR divergence** connects two trends usually studied separately into a unified narrative
4. **BW-specific IMR Kitagawa** extends Callaghan 2006 by 20+ years with a cleaner outcome (LBW vs. preterm)
5. **35 years of fully comparable birthweight** — this is the single cleanest variable for long-term trend analysis

### Biggest risks

1. **2007-2014 LBW dip** complicates the "continuous deterioration" narrative. Must explain convincingly.
2. **Reviewer objection**: "NCHS already publishes LBW rates." Response: they report; we decompose.
3. **Diabetes ascertainment confound**: rising reported diabetes partly reflects screening changes, inflating the apparent composition effect.
4. **The story may not be dramatic enough.** +1.1 ppt over 35 years is real but slow. The smoking counterfactual and the IMR divergence add drama, but the core finding is gradual deterioration, not a crisis.
