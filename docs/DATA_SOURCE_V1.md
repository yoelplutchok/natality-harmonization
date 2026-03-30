# Version 1 data source — locked decisions

These choices match the project brief (natality only, 2005–2015) and the standard way researchers obtain **public-use** U.S. birth microdata from NCHS.

## 1. Dataset: period natality public-use file

Use the **annual natality (birth) public-use microdata** from the National Center for Health Statistics (NCHS), Division of Vital Statistics — **not** the linked birth–infant death files. Linked files are a separate product (your roadmap’s **Version 3**).

Each file is a **period** file: births with **date of birth** in that calendar year (as defined in the User Guide for that year).

## 2. Geography: U.S. national file (`…us.zip`)

For Version 1, use the **United States** zips:

- Pattern: `Nat{YYYY}us.zip` (e.g. `Nat2015us.zip`).

**Exclude** `Nat{YYYY}ps.zip` (Puerto Rico / territories) unless you later add an explicit extension for territories. Mixing `us` and `ps` without documentation would confuse users.

## 3. Official access and URLs

- **Portal (human-readable index):** [Vital Statistics Online — Downloadable Data Files](https://www.cdc.gov/nchs/data_access/Vitalstatsonline.htm) (NCHS).
- **Data (HTTPS, same host as documentation links on that page):**  
  `https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/natality/Nat{YYYY}us.zip`
- **Documentation (User Guide PDF per year):**  
  `https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/DVS/natality/UserGuide{YYYY}.pdf`

**Addenda (use alongside the main guide):**

- 2009: [UserGuide2009_Addendum.pdf](https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/DVS/natality/UserGuide2009_Addendum.pdf)
- 2010: [UserGuide2010_Addendum.pdf](https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/DVS/natality/UserGuide2010_Addendum.pdf)

## 4. File format inside the zip

The public-use files are **not** ready-made CSV. After unzip, you typically get one or more **large ASCII text** files with **fixed-width fields** (positions and widths are in the annual User Guide). Your import code should treat **layout as year-specific** until harmonization.

Record the exact internal filename(s) after your first unzip in `metadata/file_inventory.csv` (`notes` column).

## 5. Restricted-use data (out of scope for V1)

County/city/sub-state geography and some sensitive fields exist only on **restricted** files (RDC / application process). This harmonization targets **public-use** fields only; say so clearly in the FAQ and comparability docs.

## 6. Terms of use

Review and cite NCHS materials:

- [Data Users Agreement](https://www.cdc.gov/nchs/data_access/restrictions.htm)
- [Vital Statistics Data Release Policy](https://www.cdc.gov/nchs/nvss/dvs_data_release.htm)

## 7. Implementation language (recommendation)

**Python + pandas** is a practical default: `read_fwf` or explicit column positions from the guide, plus Parquet/CSV for your cleaned outputs. R works equally well (`read.fwf`, `readr`). Pick one stack for the repo and stay consistent.

---

**Summary:** Version 1 raw inputs = **NCHS public-use period natality `NatYYYYus.zip` for 2005–2015**, documented yearly by **UserGuideYYYY.pdf** (plus 2009/2010 addenda), parsed as **fixed-width ASCII** per the guide.
