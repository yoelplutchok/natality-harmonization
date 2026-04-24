# About the source data

U.S. natality **public-use** microdata are derived from birth certificates and released annually by the [National Center for Health Statistics](https://www.cdc.gov/nchs/) (NCHS), a division of the CDC. These files contain one record per registered live birth and are the basis for official U.S. birth statistics. They support research on birth outcomes, maternal characteristics, prenatal care, and health disparities.

Layouts and coding change across years and certificate revisions. This project harmonizes those changes into a stable schema.

## What this project uses

### Natality files (V2: 1990-2024)

**NCHS period natality public-use files**:

- `Nat{year}.zip` for 1990-1993 (lacks the "us" suffix; contains a small foreign-resident tail of ~0.1%, identifiable via `RESTATUS == 4`)
- `Nat{year}us.zip` for 1994-2024 (US-only)

Each zip contains a single fixed-width text file. Layout varies by year (350 to 1500 bytes per record). The annual User Guide PDF is the layout authority.

Source: `ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/natality/`

See **[DATA_SOURCE_V1.md](DATA_SOURCE_V1.md)** for the full list of URLs and format details.

### Linked birth-infant death files (V3: 2005-2023)

**NCHS cohort linked birth-infant death files** link each infant death certificate back to the corresponding birth certificate, enabling infant mortality analysis by birth characteristics.

Two formats are used:

| Years | Format | Source directory | File pattern |
|-------|--------|-----------------|--------------|
| 2005-2015 | Denominator-plus (birth + death fields in single record) | `.../DVS/cohortlinkedus/` | `LinkCO{YY}US.zip` |
| 2016-2023 | Period-cohort (separate denominator + numerator files, merged by CO_SEQNUM) | `.../DVS/period-cohort-linked/` | `{Y+1}PE{Y}CO.zip` |

User guides: `LinkCO05Guide.pdf`, `LinkCO10Guide.pdf`, `LinkCO15Guide.pdf`, `21PE20CO_linkedUG.pdf` (in `raw_docs/linked/`).

## What these data contain

Each birth record includes:

- **Maternal demographics**: age, race/ethnicity, Hispanic origin, education, marital status, state of residence
- **Paternal demographics**: age, race/ethnicity, Hispanic origin, education (coverage varies by era)
- **Prenatal care**: month care began, number of visits
- **Health behaviors**: smoking during pregnancy (and pre-pregnancy for 2014+)
- **Medical risk factors**: diabetes, chronic hypertension, gestational hypertension, prior cesarean delivery (and prior cesarean count for 2014+)
- **Infections present** (2014+): gonorrhea, syphilis, chlamydia, hepatitis B, hepatitis C
- **Congenital anomalies** (2014+): 12 conditions including Down syndrome, spina bifida, cleft lip/palate, hypospadias
- **Fertility treatment** (2014+): fertility-enhancing drugs, assisted reproductive technology
- **Birth outcomes**: gestational age, birthweight, plurality, sex, Apgar score, delivery method, birth facility, attendant
- **Clinical detail** (2014+): pre-pregnancy BMI, weight gain during pregnancy, induction of labor, NICU admission, breastfed at discharge, pre-pregnancy diabetes, gestational diabetes
- **Payment source** (2009+): Medicaid, private insurance, self-pay, other
- **Administrative**: certificate revision, resident status, marital status reporting flag (2014+)

Linked files additionally contain:

- **Death-side fields**: age at death (days), underlying cause of death (ICD-10), 130-cause recode, manner of death, record weight

## What these data do NOT contain

- **Sub-state geography**: public-use files suppress county/city identifiers
- **Restricted-use variables**: some variables (e.g., exact date of birth, paternal information in some years) are only in restricted-use versions
- **Territory births after 1993**: from 1994 onward, US-only files exclude territories

## Local copies

Store downloaded zips under `raw_data/` (natality) and `raw_data/linked/` (linked files). User Guide PDFs go under `raw_docs/` and `raw_docs/linked/`. `metadata/file_inventory.csv` tracks what was retrieved and when.
