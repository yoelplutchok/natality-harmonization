# V1 invariants report

Input: `output/harmonized/natality_v2_harmonized_derived.parquet`
Years: 2005–2016
- Year summary CSV: `output/validation/invariants_year_summary_2005_2016.csv`

## Invariant checks (should all be 0)

- `bw9999_clean_not_null`: 0
- `cert_rev_2014plus_not_revised`: 0
- `cert_rev_invalid_value`: 0
- `ga99_clean_not_null`: 0
- `gest_src_pre2014_obstetric`: 0
- `hisp_origin_0_not_false`: 0
- `hisp_origin_1_5_not_true`: 0
- `hisp_origin_9_not_null`: 0
- `lbw_logic_mismatch`: 0
- `preterm_logic_mismatch`: 0
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

## Certificate revision by year (counts)

| year | total | revised_2003 | unrevised_1989 | unknown |
|---:|---:|---:|---:|---:|
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

