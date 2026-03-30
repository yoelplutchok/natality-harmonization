# Comparability across years (1990–2024)

This document records comparability decisions for the harmonized U.S. natality microdata.

Canonical sources of truth:

- `metadata/variable_crosswalk_working.csv`: benchmark-year crosswalk evidence and field-level notes
- `metadata/harmonized_schema.csv`: per-variable provenance, derivation rules, and comparability class

## Guiding policy

- **Default analysis universe**: use **resident births** only (exclude foreign residents: `restatus == 4`). This matches NCHS “residence-based” tabulations used throughout validation.
- **Prefer deterministic transforms**: when a variable is “partial,” the pipeline provides an explicit derivation and labels the break/coverage issue rather than silently imputing.
- **When coverage differs by certificate revision**: use `certificate_revision` to define revision-consistent subsets.

## Comparability class definitions

- **full**: stable concept + coding across the full 1990–2024 range; trend-safe after documented sentinel handling.
- **partial**: usable across years, but has known breaks (coding changes, revision-coverage differences, or definitional series breaks). Trend work must follow the rules below.
- **within-era**: meaningful only within an era (e.g., 1990–2002 vs 2003–2013 vs 2014–2023, or revision-specific coding frames). Not trend-safe across eras.
- **excluded**: not shipped because the public-use files do not support a reliable harmonization.

## Known structural breaks / constraints

1. **1990–2002 → 2003 certificate transition**
   - The 2003 revised certificate introduced new field names, coding frames, and measurement methods. Fields like education changed from years-of-schooling (0–17) to categorical codes (1–8).
   - The harmonization maps 1990–2002 fields to the common schema via explicit crosswalks (e.g., `_dmeduc_years_to_cat4()`), but the underlying measurement differs.

2. **1990–2002 race bridge is approximate**
   - Official NCHS bridged race was introduced with the 2003 certificate. For 1990–2002, `maternal_race_bridged4` uses an **approximate** crosswalk from `MRACE` detail codes (01→White, 02→Black, 03→AIAN, 04-08/18-68→Asian/PI, 09+→null).
   - This is adequate for broad race-group tabulations but should not be treated as equivalent to the official NCHS bridged race available from 2003.

3. **2003 maternal age is a recode**
   - The 2003 public-use file suppresses single-year maternal age (all 99) and provides only `MAGER41` (41-category recode). The harmonization converts this to approximate single-year age. Ages <15 map to 14; ages 50-54 map to 50; ages 55+ map to 55.

4. **1990–2002 smoking: independent source fields**
   - `smoking_any_during_pregnancy` comes from `TOBACCO` (yes/no/unknown) and `smoking_intensity_max_recode6` comes from `CIGAR6`. These are **independent** source fields, so ~398K records have smoker status with unknown intensity. From 2003 onward, `smoke_any` is derived from intensity, ensuring internal consistency.

5. **Gestation source breaks (three eras)**
   - 1990–2002: **LMP-only** (`DGESTAT`; source = `lmp`)
   - 2003–2013: **combined gestation** (`COMBGEST`; source = `combined`)
   - 2014–2023: **obstetric estimate** (`OEGEST_COMB`; source = `obstetric_estimate`)
   - Each transition changes the preterm rate level. Do not assume gestation-based outcomes are continuous across 2003 or 2014. Use `gestational_age_weeks_source` to identify or stratify.

6. **2009–2013 “U-only” fields are blank in the public-use files**
   - Empirically confirmed by scanning the raw zips: unrevised-only (“U”) fields such as `DMEDUC`, `MEDUC_REC`, `MPCB`, `TOBUSE`, `CIGS`, `CIG_REC6` are **entirely blank** in 2009–2013.
   - Consequence: **education, prenatal-care initiation, and smoking measures are effectively revised-only** in 2009–2013 and have substantial missingness for unrevised records.

7. **Race/ethnicity depends on bridged/imputed constructs**
   - Bridged race and Hispanic-origin recodes are broadly usable, but the bridging/editing context changes over time; cross-year race/ethnicity is treated as **partial**.

## Variable decisions (summary)

### Full comparability (trend-safe 1990–2024)

- **Counts/universe**: `year`, `restatus`, `is_foreign_resident`
- **Core demographics**: `marital_status`, `live_birth_order_recode`, `total_birth_order_recode`
- **Infant/birth**: `plurality_recode`, `infant_sex`, `birthweight_grams`, `apgar5`
- **Derived from full variables**: `birthweight_grams_clean`, `low_birthweight`, `very_low_birthweight`, `singleton`, `maternal_age_cat`

### Partial comparability (usable with explicit rules)

- **Maternal age**
  - **Why partial**: 2003 uses `MAGER41` recode (41-category) converted to approximate single-year age. All other years have true single-year age.
  - **Rule**: the 2003 approximation is adequate for age-group analyses but not for precise single-year-of-age work.

- **Race/ethnicity**
  - **Primary construct**: `maternal_race_ethnicity_5` (derived from `maternal_hispanic` + `maternal_race_bridged4`)
  - **Why partial**: 1990–2002 uses approximate bridge from detail codes (no official NCHS bridged race); 2003+ uses official bridged race. Bridging/editing context changes over time.
  - **Rule**: treat as a consistent *high-level* series, but document that 1990–2002 race bridge is approximate and levels may shift with reporting/bridging changes.

- **Education**
  - **Primary construct**: `maternal_education_cat4`
  - **Why partial**: 1990–2002 uses years-of-schooling→cat4 crosswalk (conceptually different from category-based education); 2003+ uses revision-specific fields; 2009–2013 is revised-only.
  - **Rule**:
    - 1990–2002: `_dmeduc_years_to_cat4()` maps 0–11→lt_hs, 12→hs_grad, 13–15→some_college, 16–17→ba_plus, 99→null.
    - 2003–2008: combine revised `MEDUC` with unrevised `MEDUC_REC`.
    - 2009–2013: revised-only → use `certificate_revision == 'revised_2003'` for consistent analysis.

- **Prenatal care initiation**
  - **Primary constructs**: `prenatal_care_start_month`, `prenatal_care_start_trimester`
  - **Why partial**: 1990–2002 uses `MONPRE`; 2003+ uses revision-specific fields; 2009–2013 is revised-only.
  - **Rule**: same revised-only guidance as education for 2009–2013.

- **Smoking during pregnancy**
  - **Primary constructs**: `smoking_any_during_pregnancy`, `smoking_intensity_max_recode6`
  - **Why partial**: 1990–2002 derives `smoke_any` from `TOBACCO` and intensity from `CIGAR6` independently (~398K records with smoker+unknown intensity). 2003+ derives `smoke_any` from intensity. 2009–2013 is revised-only.
  - **Rule**: for 1990–2002, expect some inconsistency between `smoke_any` and `smoke_intensity`. For 2009–2013, use `certificate_revision == 'revised_2003'`.

- **Gestation + preterm**
  - **Primary constructs**: `gestational_age_weeks`, `preterm_recode3`, `preterm_lt37`, `very_preterm_lt32`
  - **Why partial**: three distinct measurement sources across eras (LMP, combined, obstetric estimate). Each transition changes the preterm rate level.
  - **Rule**: use `gestational_age_weeks_source` to stratify. Recommended gestation-era subsets:
    - 1990–2002: `gestational_age_weeks_source == 'lmp'`
    - 2003–2013: `gestational_age_weeks_source == 'combined'`
    - 2014–2023: `gestational_age_weeks_source == 'obstetric_estimate'`

- **Medical risk factors** (`diabetes_any`, `hypertension_chronic`, `hypertension_gestational`)
  - **Why partial**: 1990–2002 uses individual Y/N/unknown flags (`DIABETES`, `CHYPER`, `PHYPER`); 2003+ uses combined U/R flags (`URF_DIAB`, `URF_CHYPER`, `URF_PHYPER`). Coding is harmonized (1=yes, 2=no, 9=unknown) but underlying ascertainment changed with the certificate revision.

- **Delivery method**
  - **Primary construct**: `delivery_method_recode`
  - **Why partial**: two distinct coding frames with the boundary at **2005** (not 2003):
    - **1990–2004 (DELMETH5-style)**: 1=vaginal, 2=VBAC, 3=primary cesarean, 4=repeat cesarean, 5=not stated (→9). In 2003–2004, the field is labeled "DMETH_REC" at position 401 but still uses DELMETH5 codes (confirmed by value distributions: codes 3, 4 present; code 2 means VBAC not cesarean). Codes 6 (vaginal, unknown if previous CS) and 7 (cesarean, unknown if previous CS) also appear in 2003–2004 (~750 records total).
    - **2005+ (DMETH_REC)**: 1=vaginal, 2=cesarean, 9=not stated.
  - **Cesarean crosswalk**: for 1990–2004, cesarean = codes 3 + 4 among known (1–4). For 2005+, cesarean = code 2 among known (1–2). This crosswalk is validated against NVSR published cesarean rates for 1990–2024 (all within 0.07 pct-pts).
  - **Rule**: the cesarean/vaginal binary is comparable across the full 1990–2024 range via the crosswalk above. Finer categories (primary vs repeat cesarean, VBAC) are available only for 1990–2004.

- **Father's age** (`father_age`)
  - **Why partial**: 1990–2002 uses `DFAGE`; 2003+ uses various recodes/combined age fields. `99` → null.
  - **Rule**: usable for broad age-group analyses across all years.

- **Birth facility** (`birth_facility`)
  - **Why partial**: 1990–2002 uses `BIRPLA`; 2003–2013 uses `PLDEL`/`UBFACIL`; 2014+ uses `BFACIL`. Coarse 4-category mapping (hospital, birth_center, clinic_other, home) is comparable across eras.

- **Attendant at birth** (`attendant_at_birth`)
  - **Why partial**: coding is harmonized (1=MD, 2=DO, 3=CNM, etc.) but underlying certification/reporting context changed with the 2003 certificate.

- **Prior cesarean** (`prior_cesarean`)
  - **Why partial**: 1990–2002 uses `DCSEZD`; 2003+ uses `RF_CESAR`/`URF_CESAR`. Both are boolean (yes/no/unknown), but ascertainment changed with the certificate revision.

- **Father Hispanic origin** (`father_hispanic`)
  - **Why partial**: 1990–2002 uses `ORFATH`; 2003–2013 uses `UFHISP`; 2014+ uses `FHISP_R`. All use 0=non-Hispanic, 1–5=Hispanic coding, but reporting context and item non-response rates differ by era.

- **Father race/ethnicity** (`father_race_ethnicity_5`)
  - **Why partial**: 1990–2002 uses `ORRACEF`; 2003+ uses `FRACEHISP`. **Critical**: code 8 means "NH_other" for 1990–2002 but "origin unknown" (→null) for 2003+. The `NH_other` category exists only in 1990–2002 data.

- **Father education** (`father_education_cat4`)
  - **Why partial**: 1990–1994 uses `DFEDUC` (years-of-schooling→cat4); 2009+ uses `FEDUC` (categorical codes→cat4). **Null for 1995–2008** (field dropped from public-use files). Partial coverage 2009–2010 (2003-revision early-adopter states only; ~58–65% non-null). Full coverage 2011+.

### Within-era only

- `maternal_race_detail` (1990–2002 MRACE detail codes vs 2003–2013 MRACE vs 2014+ MRACE6; not a single comparable series)
- `smoking_pre_pregnancy_recode6` (2014–2023 only; `CIG0_R`)
- `bmi_prepregnancy`, `bmi_prepregnancy_recode6` (2014–2023 only; NCHS positions 283-287. Null for all pre-2014 years.)
- `payment_source_recode` (2009–2023; partial coverage 2009–2010 from early-adopter states; near-full coverage 2011+; complete 2014+. Null for all pre-2009 years.)
- `prior_cesarean_count` (2014–2023 only; `RF_CESARN`. 0–30 = count, 99→null.)
- `fertility_enhancing_drugs` (2014–2023 only; `RF_FEDRG`. Note: high null rate because X="Not Applicable" → null.)
- `assisted_reproductive_tech` (2014–2023 only; `RF_ARTEC`.)
- 12 congenital anomaly bools: `ca_anencephaly`, `ca_spina_bifida`, `ca_cchd`, `ca_cdh`, `ca_omphalocele`, `ca_gastroschisis`, `ca_limb_reduction`, `ca_cleft_lip`, `ca_cleft_palate`, `ca_down_syndrome`, `ca_chromosomal_disorder`, `ca_hypospadias` (2014–2023 only. `ca_down_syndrome` and `ca_chromosomal_disorder` use C/P/N/U coding where C=Confirmed and P=Pending both map to true.)
- 5 infection bools: `infection_gonorrhea`, `infection_syphilis`, `infection_chlamydia`, `infection_hep_b`, `infection_hep_c` (2014–2023 only.)

### Excluded

- `maternal_nativity`
  - **Reason**: `MBCNTRY` appears blank/suppressed in the 2005–2013 public-use files and `MBSTATE_REC` is a non-comparable 2014-only recode. Excluded rather than shipping a misleading within-era series.

## Recommended analytic subsets

- **Residents-only** (default): `is_foreign_resident == false`
- **Revision-consistent subset** (for education/prenatal care/smoking in 2009–2013): `certificate_revision == 'revised_2003'`
- **Gestation-era subsets**:
  - 1990–2002: `gestational_age_weeks_source == 'lmp'`
  - 2003–2013: `gestational_age_weeks_source == 'combined'`
  - 2014–2023: `gestational_age_weeks_source == 'obstetric_estimate'`

## V3 Linked birth-infant death comparability (2005–2023)

The V3 linked file shares all birth-side comparability constraints from V2 above. Additional death-side considerations:

### Death-side variables (full comparability within scope)

- **`infant_death`**: stable across all 19 years. FLGND coding differs (1/2 for 2005-2013; 1/blank for 2014-2020) but is normalized to a consistent boolean.
- **`age_at_death_days`**: comparable 2005-2018. **Minor break at 2019**: NCHS switched to calculating age at death from birth certificate time-of-birth (not death certificate). This improves sub-24-hour accuracy but means the `<1 hour` and `1 day` categories are not perfectly comparable with earlier years. Total neonatal/postneonatal splits are minimally affected.
- **`underlying_cause_icd10`**: ICD-10 throughout (2005-2023). Comparable, though coding rule updates occur periodically.
- **`cause_recode_130`**: NCHS 130-cause infant death recode. Consistent across the period.
- **`record_weight`**: available for all years. NCHS recommends **not** applying the weight for cohort analyses — use unweighted data.

### Linked file format transition (2016)

The raw data format changed from denominator-plus (2005-2015) to period-cohort (2016-2020). This is handled transparently by the pipeline and does not affect the harmonized output schema.

### Bridged race dropped in 2020

Starting with 2020 data, NCHS no longer provides bridged race in the linked file. The `maternal_race_bridged4` field may have different coverage or derivation for 2020. For race/ethnicity trend work, consider using 2005-2019 or using `maternal_race_detail` / `maternal_race_ethnicity_5` with awareness of this change.

### Recommended linked analysis subsets

- **Residents-only** (default): `is_foreign_resident == false`
- **Consistent age-at-death**: 2005-2018 for sub-day age categories; 2005-2023 for neonatal/postneonatal
- **Consistent race/ethnicity**: 2005-2019 for bridged race; 2005-2023 for Hispanic origin

## Change log

- 2026-03-19 (Session 13): Finalized benchmark-year crosswalk decisions for key V1 domains; documented 2009–2013 public-use blanking of U-only fields; excluded nativity; defined revision-consistent analysis guidance.
- 2026-03-19 (Session 16): Expanded to 1990–2020 scope. Added 1990–2002 era breaks (certificate transition, approximate race bridge, LMP gestation, independent smoking fields, years-based education, MAGER41 recode for 2003). Updated gestation era guidance to three eras.
- 2026-03-23 (Session 18): Added V3 linked birth-infant death comparability section (2005-2020). Documented age-at-death 2019 break, bridged race 2020 change, record weight guidance.
- 2026-03-24 (Session 19): Extended year range from 1990-2020 to 1990-2023. Added BMI within-era entry (2014-2024).
- 2026-03-26 (Session 20): Added 5 new harmonized variables: father_age, birth_facility, attendant_at_birth, payment_source_recode, prior_cesarean.
- 2026-03-27 (Session 21): Added 23 new harmonized variables: father_hispanic, father_race_ethnicity_5, father_education_cat4, 12 congenital anomaly bools, 5 infection bools, prior_cesarean_count, fertility_enhancing_drugs, assisted_reproductive_tech. Extended payment_source_recode to 2009+. Classified new variables as partial (paternal demographics) or within-era (2014+ clinical fields).
- 2026-03-29 (Session 23): Rewrote delivery method section with validated cesarean crosswalk. Documented that 2003–2004 files store DELMETH5-style codes at the DMETH_REC position (boundary at 2005, not 2003). Cesarean binary (codes 3+4 pre-2005, code 2 post-2005) validated against NVSR published rates 1990–2024.
