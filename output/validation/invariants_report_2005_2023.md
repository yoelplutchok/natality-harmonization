# V1 invariants report

Input: `output/harmonized/natality_v3_linked_harmonized_derived.parquet`
Years: 2005–2023
- Year summary CSV: `output/validation/invariants_year_summary_2005_2023.csv`

Mode: **V3 linked** (auto-detected from schema)

In V3 linked mode, three V2-only structural-coverage invariants (`unrevised_2009_2013_has_educ`, `unrevised_2009_2013_has_pnmonth`, `unrevised_2009_2013_has_smokeint`) are **skipped** because the linked denominator-plus layout for 2005–2013 retains MEDUC_REC/MPCB/CIG_1-3 bytes that the natality 2009–2010 public-use layout drops. See `docs/COMPARABILITY.md` §"V3 linked vs V2 natality: 2009–2010 unrevised-cert field retention". Also, `record_weight_null_when_survivor` is allowed up to 2 (upstream NCHS quirk: 1 row in 2014 + 1 in 2015).

## Invariant checks (should all be 0 unless a known exception applies)

- `apgar99_clean_not_null`: 0
- `art_nonnull_pre2014`: 0
- `bw9999_clean_not_null`: 0
- `bw_out_of_range`: 0
- `ca_nonnull_pre2014`: 0
- `cert_rev_2014plus_not_revised`: 0
- `cert_rev_invalid_value`: 0
- `delivery_method_recode_invalid_value`: 0
- `delivery_method_recode_post2004_out_of_set`: 0
- `father_educ_nonnull_1995_2008`: 0
- `father_hisp_race_eth_mismatch`: 0
- `fertility_nonnull_pre2014`: 0
- `ga99_clean_not_null`: 0
- `ga_out_of_range`: 0
- `gest_src_pre2014_obstetric`: 0
- `hisp_origin_0_not_false`: 0
- `hisp_origin_1_5_not_true`: 0
- `hisp_origin_9_not_null`: 0
- `infection_nonnull_pre2014`: 0
- `lbw_logic_mismatch`: 0
- `mrace15_invalid_when_nonnull`: 0
- `payment_source_nonnull_pre2009`: 0
- `preterm_logic_mismatch`: 0
- `prior_ces_count_99_post2014`: 0
- `prior_ces_count_nonnull_pre2005`: 0
- `race_eth_hisp_not_hispanic`: 0
- `race_eth_nh_bad_mapping`: 0
- `race_eth_null_when_hisp_false_and_race_detail_valid`: 0
- `race_eth_null_when_hisp_true`: 0
- `record_weight_null_when_death`: 0
- `record_weight_null_when_survivor`: 2 _(within known-exception budget of 2)_
- `singleton_logic_mismatch`: 0
- `smoke_any_false_bad_int`: 0
- `smoke_any_true_bad_int`: 0
- `smoke_int_0_not_false`: 0
- `smoke_int_1_5_not_true`: 0
- `smoke_int_6_not_null_any`: 0
- `unrevised_2009_2013_has_educ`: 2012896 _(skipped — V2-only, see §4.2 note)_
- `unrevised_2009_2013_has_pnmonth`: 2026581 _(skipped — V2-only, see §4.2 note)_
- `unrevised_2009_2013_has_smokeint`: 2026581 _(skipped — V2-only, see §4.2 note)_
- `year_outside_expected_range`: 0

## Certificate revision by year (counts)

| year | total | revised_2003 | unrevised_1989 | unknown |
|---:|---:|---:|---:|---:|
| 2005 | 4,145,887 | 1,275,242 | 2,870,645 | 0 |
| 2006 | 4,273,264 | 2,080,175 | 2,193,089 | 0 |
| 2007 | 4,324,008 | 2,388,236 | 1,687,076 | 248,696 |
| 2008 | 4,255,188 | 2,761,407 | 1,342,513 | 151,268 |
| 2009 | 4,137,836 | 2,816,719 | 1,219,573 | 101,544 |
| 2010 | 4,007,105 | 3,101,037 | 807,008 | 99,060 |
| 2011 | 3,961,221 | 3,398,490 | 518,062 | 44,669 |
| 2012 | 3,960,797 | 3,495,711 | 434,448 | 30,638 |
| 2013 | 3,940,764 | 3,564,417 | 345,865 | 30,482 |
| 2014 | 3,998,175 | 3,998,175 | 0 | 0 |
| 2015 | 3,988,733 | 3,988,733 | 0 | 0 |
| 2016 | 3,956,112 | 3,956,112 | 0 | 0 |
| 2017 | 3,864,754 | 3,864,754 | 0 | 0 |
| 2018 | 3,801,534 | 3,801,534 | 0 | 0 |
| 2019 | 3,757,582 | 3,757,582 | 0 | 0 |
| 2020 | 3,619,826 | 3,619,826 | 0 | 0 |
| 2021 | 3,669,928 | 3,669,928 | 0 | 0 |
| 2022 | 3,676,029 | 3,676,029 | 0 | 0 |
| 2023 | 3,605,081 | 3,605,081 | 0 | 0 |

## 2009–2013 unrevised coverage checks (should be 0 non-null)

| year | unrevised rows | educ non-null | pn month non-null | smoke intensity non-null |
|---:|---:|---:|---:|---:|
| 2009 | 1,219,573 | 1,210,388 | 1,219,573 | 1,219,573 |
| 2010 | 807,008 | 802,508 | 807,008 | 807,008 |
| 2011 | 518,062 | 0 | 0 | 0 |
| 2012 | 434,448 | 0 | 0 | 0 |
| 2013 | 345,865 | 0 | 0 | 0 |

## Null-rate discontinuities (>5.0 ppt year-over-year change)

**22 break(s) detected** (informational — these reflect known structural changes, not bugs):

| Variable | Year transition | Null % (from → to) | Delta (ppt) |
|----------|----------------|---------------------|-------------|
| `marital_status` | 2016→2017 | 0.0% → 12.2% | +12.2 |
| `maternal_race_bridged4` | 2019→2020 | 0.0% → 100.0% | +100.0 |
| `maternal_education_cat4` | 2010→2011 | 1.1% → 15.2% | +14.1 |
| `maternal_education_cat4` | 2013→2014 | 10.6% → 4.7% | -5.9 |
| `prenatal_care_start_month` | 2010→2011 | 0.0% → 14.2% | +14.2 |
| `prenatal_care_start_month` | 2013→2014 | 9.6% → 3.6% | -6.0 |
| `smoking_any_during_pregnancy` | 2006→2007 | 19.6% → 6.8% | -12.7 |
| `smoking_any_during_pregnancy` | 2007→2008 | 6.8% → 12.4% | +5.6 |
| `smoking_any_during_pregnancy` | 2010→2011 | 12.1% → 20.8% | +8.7 |
| `smoking_any_during_pregnancy` | 2013→2014 | 13.9% → 5.5% | -8.5 |
| `smoking_intensity_max_recode6` | 2006→2007 | 5.6% → 0.0% | -5.6 |
| `smoking_intensity_max_recode6` | 2010→2011 | 0.0% → 14.2% | +14.2 |
| `smoking_intensity_max_recode6` | 2013→2014 | 9.6% → 3.6% | -6.0 |
| `father_age` | 2011→2012 | 13.1% → 23.1% | +10.0 |
| `father_age` | 2013→2014 | 21.0% → 15.7% | -5.2 |
| `maternal_race_detail` | 2005→2006 | 30.8% → 48.7% | +17.9 |
| `maternal_race_detail` | 2006→2007 | 48.7% → 61.0% | +12.3 |
| `maternal_race_detail` | 2007→2008 | 61.0% → 68.5% | +7.5 |
| `maternal_race_detail` | 2009→2010 | 70.5% → 79.9% | +9.3 |
| `maternal_race_detail` | 2010→2011 | 79.9% → 86.9% | +7.1 |
| `maternal_race_detail` | 2013→2014 | 91.2% → 3.3% | -87.9 |
| `maternal_race_detail_15cat` | 2013→2014 | 100.0% → 3.3% | -96.7 |

## Status

**PASS**: all invariant checks passed.

