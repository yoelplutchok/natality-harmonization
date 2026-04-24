# Row-count validation (1994–2024)

## What this checks

- **Parquet rows vs raw zip size**: verifies no records were dropped during parsing.
- **Parquet rows vs NCHS published annual births (residence-based)**: highlights that the public-use microdata include births to **U.S. residents and nonresidents**, while residence tabulations exclude nonresidents.

## Sources

- NCHS annual births (resident births): `https://data.cdc.gov/NCHS/NCHS-Births-and-General-Fertility-Rates-United-Sta/e6fc-ccez`
- Nonresident inclusion note: see `raw_docs/UserGuide2015.pdf` (Introduction).

## Results

| year | file records | foreign residents (RESTATUS=4) | resident rows | NCHS births (residence) | match? | zip method | size-implied rows match? |
|---:|---:|---:|---:|---:|:---:|:---|:---:|
| 1994 | 3,956,925 | 4,158 | 3,952,767 | 3,952,767 | yes | deflate | yes |
| 1995 | 3,903,012 | 3,423 | 3,899,589 | 3,899,589 | yes | deflate | yes |
| 1996 | 3,894,874 | 3,380 | 3,891,494 | 3,891,494 | yes | deflate | yes |
| 1997 | 3,884,329 | 3,435 | 3,880,894 | 3,880,894 | yes | deflate | yes |
| 1998 | 3,945,192 | 3,639 | 3,941,553 | 3,941,553 | yes | deflate | yes |
| 1999 | 3,963,465 | 4,048 | 3,959,417 | 3,959,417 | yes | deflate | yes |
| 2000 | 4,063,823 | 5,009 | 4,058,814 | 4,058,814 | yes | deflate | yes |
| 2001 | 4,031,531 | 5,598 | 4,025,933 | 4,025,933 | yes | deflate | yes |
| 2002 | 4,027,376 | 5,650 | 4,021,726 | 4,021,726 | yes | deflate | yes |
| 2003 | 4,096,092 | 6,142 | 4,089,950 | 4,089,950 | yes | deflate | yes |
| 2004 | 4,118,907 | 6,855 | 4,112,052 | 4,112,052 | yes | deflate | yes |
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
| 2016 | 3,956,112 | 10,237 | 3,945,875 | 3,945,875 | yes | deflate64 | yes |
| 2017 | 3,864,754 | 9,254 | 3,855,500 | 3,855,500 | yes | deflate64 | yes |
| 2018 | 3,801,534 | 9,822 | 3,791,712 | 3,791,712 | yes | deflate | yes |
| 2019 | 3,757,582 | 10,042 | 3,747,540 | — | — | deflate | yes |
| 2020 | 3,619,826 | 6,179 | 3,613,647 | — | — | deflate64 | yes |
| 2021 | 3,669,928 | 5,636 | 3,664,292 | — | — | deflate64 | yes |
| 2022 | 3,676,029 | 8,271 | 3,667,758 | — | — | deflate | yes |
| 2023 | 3,605,081 | 9,064 | 3,596,017 | — | — | deflate64 | yes |
| 2024 | 3,638,436 | 9,502 | 3,628,934 | — | — | deflate64 | yes |

All years matched the raw zip size-implied record counts.

