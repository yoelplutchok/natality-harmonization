# Data sources

These are the raw inputs to the harmonization pipeline. All are **public-use** files from the National Center for Health Statistics (NCHS), Division of Vital Statistics.

## 1. Natality files (1990–2024)

**Annual natality (birth) public-use microdata** — one record per registered live birth. Each file is a **period** file: births with date of birth in that calendar year.

### URL patterns

| Years | ZIP pattern | Doc pattern | Notes |
|-------|------------|-------------|-------|
| 1990–1993 | `Nat{YYYY}.zip` | `Nat{YYYY}doc.pdf` | US + territories combined; filter on `RECTYPE=1` for US-only |
| 1994–2024 | `Nat{YYYY}us.zip` | `UserGuide{YYYY}.pdf` | US-only |

Base URLs:

- **Data**: `https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/natality/`
- **Documentation**: `https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/DVS/natality/`
- **Portal** (human-readable index): [Vital Statistics Online — Downloadable Data Files](https://www.cdc.gov/nchs/data_access/Vitalstatsonline.htm)

**Documentation addenda** (use alongside the main guide):

- 2009: `UserGuide2009_Addendum.pdf`
- 2010: `UserGuide2010_Addendum.pdf`

### File format

After unzip: one or more **large ASCII text files** with **fixed-width fields** (positions and widths defined in the annual User Guide). Record lengths vary by era:

| Era | Years | Record length | Certificate |
|-----|-------|---------------|-------------|
| Pre-2003 | 1990–2002 | 350 bytes | Unrevised 1989 |
| Transition | 2003 | 1350 bytes | Dual (first year of 2003 revision) |
| Transition | 2004 | 1500 bytes | Dual |
| Transition | 2005–2013 | 775 bytes (2006–2013), 1500 bytes (2005) | Dual |
| Revised-only | 2014–2024 | 1345 bytes | Revised 2003 |

### Compression caveats

Most zips use standard **deflate** and stream fine with Python's `zipfile`. Exceptions:

- **2009–2013**: deflate64 (zip method 9) — requires `7z`
- **2015**: PPMd (zip method 98) — requires `7z`
- **2016–2017, 2020**: deflate64 — requires `7z`
- **2014, 2018–2019, 2021–2024**: standard deflate

Install: `brew install p7zip` (macOS) or `apt install p7zip-full` (Linux). The import scripts automatically fall back to `7z` when needed.

## 2. Linked birth-infant death files (2005–2023)

**NCHS cohort linked birth-infant death files** link each infant death certificate back to the corresponding birth certificate. Two formats:

| Years | Format | Source directory | ZIP pattern | User guide |
|-------|--------|-----------------|-------------|------------|
| 2005–2015 | Denominator-plus (birth + death in single record) | `.../DVS/cohortlinkedus/` | `LinkCO{YY}US.zip` | `LinkCO{YY}Guide.pdf` (available for 2005, 2010, 2015) |
| 2016–2023 | Period-cohort (separate denominator + numerator, merged by CO_SEQNUM) | `.../DVS/period-cohort-linked/` | `{Y+1}PE{Y}CO.zip` | `{Y+1}PE{Y}CO_linkedUG.pdf` |

Base URLs:

- **2005–2015**: `https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/cohortlinkedus/`
- **2016–2023**: `https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/period-cohort-linked/`

### Linked compression caveats

- **2005–2012, 2015**: standard deflate
- **2013**: deflate64 — requires `7z`
- **2014**: LZMA — requires `7z`
- **2016–2020**: deflate64 — requires `7z`
- **2021–2023**: standard deflate (verify after download)

## 3. Restricted-use data (out of scope)

County/city/sub-state geography and some sensitive fields exist only on **restricted** files (RDC / application process). This harmonization targets **public-use** fields only.

## 4. Terms of use

Review and cite NCHS materials:

- [Data Users Agreement](https://www.cdc.gov/nchs/data_access/restrictions.htm)
- [Vital Statistics Data Release Policy](https://www.cdc.gov/nchs/nvss/dvs_data_release.htm)

## 5. Local storage conventions

| Kind | Put it here |
|------|-------------|
| User Guide PDFs (natality) | `raw_docs/` |
| User Guide PDFs (linked) | `raw_docs/linked/` |
| Natality zips | `raw_data/` |
| Linked zips | `raw_data/linked/` |

The **inventory** in `metadata/file_inventory.csv` tracks what was retrieved and when. These folders are in `.gitignore`.
