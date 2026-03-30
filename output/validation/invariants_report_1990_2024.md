# V1 invariants report

Input: `/Users/yoelplutchok/Desktop/natality-harmonization/output/harmonized/natality_v2_harmonized_derived.parquet`
Years: 1990–2024
- Year summary CSV: `/Users/yoelplutchok/Desktop/natality-harmonization/output/validation/invariants_year_summary_1990_2024.csv`

## Invariant checks (should all be 0)

- `apgar99_clean_not_null`: 0
- `art_nonnull_pre2014`: 0
- `bw9999_clean_not_null`: 0
- `bw_out_of_range`: 0
- `ca_nonnull_pre2014`: 0
- `cert_rev_2014plus_not_revised`: 0
- `cert_rev_invalid_value`: 0
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
- `payment_source_nonnull_pre2009`: 0
- `preterm_logic_mismatch`: 0
- `prior_ces_count_99_post2014`: 0
- `prior_ces_count_nonnull_pre2014`: 0
- `race_eth_hisp_not_hispanic`: 0
- `race_eth_nh_bad_mapping`: 0
- `singleton_logic_mismatch`: 0
- `smoke_any_false_bad_int`: 0
- `smoke_any_true_bad_int`: 0
- `smoke_int_0_not_false`: 0
- `smoke_int_1_5_not_true`: 0
- `smoke_int_6_not_null_any`: 0
- `unrevised_2009_2013_has_educ`: 0
- `unrevised_2009_2013_has_pnmonth`: 0
- `unrevised_2009_2013_has_smokeint`: 0
- `year_outside_expected_range`: 0

## Certificate revision by year (counts)

| year | total | revised_2003 | unrevised_1989 | unknown |
|---:|---:|---:|---:|---:|
| 1990 | 4,162,917 | 0 | 4,162,917 | 0 |
| 1991 | 4,115,342 | 0 | 4,115,342 | 0 |
| 1992 | 4,069,428 | 0 | 4,069,428 | 0 |
| 1993 | 4,004,523 | 0 | 4,004,523 | 0 |
| 1994 | 3,956,925 | 0 | 3,956,925 | 0 |
| 1995 | 3,903,012 | 0 | 3,903,012 | 0 |
| 1996 | 3,894,874 | 0 | 3,894,874 | 0 |
| 1997 | 3,884,329 | 0 | 3,884,329 | 0 |
| 1998 | 3,945,192 | 0 | 3,945,192 | 0 |
| 1999 | 3,963,465 | 0 | 3,963,465 | 0 |
| 2000 | 4,063,823 | 0 | 4,063,823 | 0 |
| 2001 | 4,031,531 | 0 | 4,031,531 | 0 |
| 2002 | 4,027,376 | 0 | 4,027,376 | 0 |
| 2003 | 4,096,092 | 225,965 | 3,870,127 | 0 |
| 2004 | 4,118,907 | 758,833 | 3,360,074 | 0 |
| 2005 | 4,145,619 | 1,275,018 | 2,870,601 | 0 |
| 2006 | 4,273,225 | 2,080,152 | 2,193,073 | 0 |
| 2007 | 4,324,008 | 2,388,236 | 1,687,076 | 248,696 |
| 2008 | 4,255,156 | 2,761,379 | 1,342,509 | 151,268 |
| 2009 | 4,137,836 | 2,816,719 | 1,219,573 | 101,544 |
| 2010 | 4,007,105 | 3,101,037 | 807,008 | 99,060 |
| 2011 | 3,961,220 | 3,398,489 | 518,062 | 44,669 |
| 2012 | 3,960,796 | 3,495,710 | 434,448 | 30,638 |
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
| 2024 | 3,638,436 | 3,638,436 | 0 | 0 |

## 2009–2013 unrevised coverage checks (should be 0 non-null)

| year | unrevised rows | educ non-null | pn month non-null | smoke intensity non-null |
|---:|---:|---:|---:|---:|
| 2009 | 1,219,573 | 0 | 0 | 0 |
| 2010 | 807,008 | 0 | 0 | 0 |
| 2011 | 518,062 | 0 | 0 | 0 |
| 2012 | 434,448 | 0 | 0 | 0 |
| 2013 | 345,865 | 0 | 0 | 0 |

## Status

**PASS**: all invariant checks passed.

