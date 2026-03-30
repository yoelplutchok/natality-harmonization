# Row-count validation (2005–2015)

## What this checks

- **Parquet rows vs raw zip size**: verifies no records were dropped during parsing.
- **Parquet rows vs NCHS published annual births (residence-based)**: highlights that the public-use microdata include births to **U.S. residents and nonresidents**, while residence tabulations exclude nonresidents.

## Sources

- NCHS annual births (resident births): `https://data.cdc.gov/NCHS/NCHS-Births-and-General-Fertility-Rates-United-Sta/e6fc-ccez`
- Nonresident inclusion note: see `raw_docs/UserGuide2015.pdf` (Introduction).

## Results

| year | file records | foreign residents (RESTATUS=4) | resident rows | NCHS births (residence) | match? | zip method | size-implied rows match? |
|---:|---:|---:|---:|---:|:---:|:---|:---:|
| 2005 | 4,145,619 | 7,270 | 4,138,349 | 4,138,349 | yes | deflate | yes |
| 2006 | 4,273,225 | 7,670 | 4,265,555 | 4,265,555 | yes | deflate | yes |
| 2007 | 4,324,008 | 7,775 | 4,316,233 | 4,316,233 | yes | deflate | yes |
| 2008 | 4,255,156 | 7,462 | 4,247,694 | 4,247,694 | yes | deflate | yes |
| 2009 | 4,137,836 | 7,171 | 4,130,665 | 4,130,665 | yes | deflate64 | yes |
| 2010 | 4,007,105 | 7,719 | 3,999,386 | 3,999,386 | yes | deflate64 | yes |
| 2011 | 3,961,220 | 7,630 | 3,953,590 | 3,953,590 | yes | deflate64 | yes |
| 2012 | 3,960,796 | 7,955 | 3,952,841 | 3,952,841 | yes | deflate64 | yes |
| 2013 | 3,940,764 | 8,583 | 3,932,181 | 3,932,181 | yes | deflate64 | yes |
| 2014 | 3,998,175 | 10,099 | 3,988,076 | 3,988,076 | yes | deflate | yes |
| 2015 | 3,988,733 | 10,236 | 3,978,497 | 3,978,497 | yes | ppmd | yes |

All years matched the raw zip size-implied record counts.

