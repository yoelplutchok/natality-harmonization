# Row-count validation (1994–2004)

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

All years matched the raw zip size-implied record counts.

