# Row-count validation (1994–2023)

## What this checks

- **Parquet rows vs raw zip size**: verifies no records were dropped during parsing.
- **Parquet rows vs NCHS published annual births (residence-based)**: highlights that the public-use microdata include births to **U.S. residents and nonresidents**, while residence tabulations exclude nonresidents.

## Sources

- NCHS annual births (resident births): `https://data.cdc.gov/NCHS/NCHS-Births-and-General-Fertility-Rates-United-Sta/e6fc-ccez`
- Nonresident inclusion note: see `raw_docs/UserGuide2015.pdf` (Introduction).

## Results

| year | parquet rows (file records) | zip method | size-implied rows match? |
|---:|---:|:---|:---:|
| 1994 | 3,956,925 | deflate | yes |
| 1995 | 3,903,012 | deflate | yes |
| 1996 | 3,894,874 | deflate | yes |
| 1997 | 3,884,329 | deflate | yes |
| 1998 | 3,945,192 | deflate | yes |
| 1999 | 3,963,465 | deflate | yes |
| 2000 | 4,063,823 | deflate | yes |
| 2001 | 4,031,531 | deflate | yes |
| 2002 | 4,027,376 | deflate | yes |
| 2003 | 4,096,092 | deflate | yes |
| 2004 | 4,118,907 | deflate | yes |
| 2005 | 4,145,619 | deflate | yes |
| 2006 | 4,273,225 | deflate | yes |
| 2007 | 4,324,008 | deflate | yes |
| 2008 | 4,255,156 | deflate | yes |
| 2009 | 4,137,836 | deflate64 | yes |
| 2010 | 4,007,105 | deflate64 | yes |
| 2011 | 3,961,220 | deflate64 | yes |
| 2012 | 3,960,796 | deflate64 | yes |
| 2013 | 3,940,764 | deflate64 | yes |
| 2014 | 3,998,175 | deflate | yes |
| 2015 | 3,988,733 | ppmd | yes |
| 2016 | 3,956,112 | deflate64 | yes |
| 2017 | 3,864,754 | deflate64 | yes |
| 2018 | 3,801,534 | deflate | yes |
| 2019 | 3,757,582 | deflate | yes |
| 2020 | 3,619,826 | deflate64 | yes |
| 2021 | 3,669,928 | deflate64 | yes |
| 2022 | 3,676,029 | deflate | yes |
| 2023 | 3,605,081 | deflate64 | yes |

All years matched the raw zip size-implied record counts.

