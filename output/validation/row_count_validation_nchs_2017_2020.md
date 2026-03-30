# Row-count validation (2017–2020)

## What this checks

- **Parquet rows vs raw zip size**: verifies no records were dropped during parsing.
- **Parquet rows vs NCHS published annual births (residence-based)**: highlights that the public-use microdata include births to **U.S. residents and nonresidents**, while residence tabulations exclude nonresidents.

## Sources

- NCHS annual births (resident births): `https://data.cdc.gov/NCHS/NCHS-Births-and-General-Fertility-Rates-United-Sta/e6fc-ccez`
- Nonresident inclusion note: see `raw_docs/UserGuide2015.pdf` (Introduction).

## Results

| year | file records | foreign residents (RESTATUS=4) | resident rows | NCHS births (residence) | match? | zip method | size-implied rows match? |
|---:|---:|---:|---:|---:|:---:|:---|:---:|
| 2017 | 3,864,754 | 9,254 | 3,855,500 | 3,855,500 | yes | deflate64 | yes |
| 2018 | 3,801,534 | 9,822 | 3,791,712 | 3,791,712 | yes | deflate | yes |
| 2019 | 3,757,582 | 10,042 | 3,747,540 | — | — | deflate | yes |
| 2020 | 3,619,826 | 6,179 | 3,613,647 | — | — | deflate64 | yes |

All years matched the raw zip size-implied record counts.

