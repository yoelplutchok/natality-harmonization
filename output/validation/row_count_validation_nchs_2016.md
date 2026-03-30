# Row-count validation (2016)

## What this checks

- **Parquet rows vs raw zip size**: verifies no records were dropped during parsing.
- **Parquet rows vs NCHS published annual births (residence-based)**: highlights that the public-use microdata include births to **U.S. residents and nonresidents**, while residence tabulations exclude nonresidents.

## Sources

- NCHS annual births (resident births): `https://data.cdc.gov/NCHS/NCHS-Births-and-General-Fertility-Rates-United-Sta/e6fc-ccez`
- Nonresident inclusion note: see `raw_docs/UserGuide2015.pdf` (Introduction).

## Results

| year | file records | foreign residents (RESTATUS=4) | resident rows | NCHS births (residence) | match? | zip method | size-implied rows match? |
|---:|---:|---:|---:|---:|:---:|:---|:---:|
| 2016 | 3,956,112 | 10,237 | 3,945,875 | 3,945,875 | yes | deflate64 | yes |

All years matched the raw zip size-implied record counts.

