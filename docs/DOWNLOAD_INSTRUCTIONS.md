# How to download NCHS natality and linked data

Everything below is the **official NCHS** public-use product described in [DATA_SOURCE_V1.md](DATA_SOURCE_V1.md).

## Where the files live on the web

1. Open the NCHS portal: **[Vital Statistics Online — Downloadable Data Files](https://www.cdc.gov/nchs/data_access/Vitalstatsonline.htm)**.
2. Scroll to **Birth Data Files** (natality) or **Linked Birth / Infant Death Data** (linked).

Direct folder URLs (same files as the portal links):

| Product | Documentation | Data |
|---------|--------------|------|
| Natality | `https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/DVS/natality/` | `https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/natality/` |
| Linked (2005–2015) | Same folder or `raw_docs/linked/` | `https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/cohortlinkedus/` |
| Linked (2016–2023) | Same folder or `raw_docs/linked/` | `https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/period-cohort-linked/` |

## What to save on your machine

| Kind | Put it here |
|------|-------------|
| Natality User Guide PDFs | `raw_docs/` |
| Linked User Guide PDFs | `raw_docs/linked/` |
| Natality zips | `raw_data/` |
| Linked zips | `raw_data/linked/` |

These folders are in `.gitignore`. The **inventory** in `metadata/file_inventory.csv` is what you version-control instead.

## Downloading natality files (1990–2024)

### User Guides (PDF)

```bash
cd raw_docs

# 1990–2002: Nat{YYYY}doc.pdf
for y in $(seq 1990 2002); do
  curl -fL -O "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/DVS/natality/Nat${y}doc.pdf"
done

# 2003–2024: UserGuide{YYYY}.pdf
for y in $(seq 2003 2024); do
  curl -fL -O "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/DVS/natality/UserGuide${y}.pdf"
done

# Addenda (2009, 2010)
curl -fL -O "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/DVS/natality/UserGuide2009_Addendum.pdf"
curl -fL -O "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/DVS/natality/UserGuide2010_Addendum.pdf"
```

**Note**: Some recent User Guides have variant filenames (e.g., `UserGuide2019-508.pdf`). If a download fails, check the NCHS portal for the exact filename.

### Data (ZIP)

```bash
cd raw_data

# 1990–1993: Nat{YYYY}.zip (US + territories; import script filters to US)
for y in $(seq 1990 1993); do
  curl -fL -O "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/natality/Nat${y}.zip"
done

# 1994–2024: Nat{YYYY}us.zip (US only)
for y in $(seq 1994 2024); do
  curl -fL -O "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/natality/Nat${y}us.zip"
done
```

## Downloading linked files (2005–2023)

```bash
cd raw_data/linked

# 2005–2015: denominator-plus format
for yy in 05 06 07 08 09 10 11 12 13 14 15; do
  curl -fL -O "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/cohortlinkedus/LinkCO${yy}US.zip"
done

# 2016–2023: period-cohort format ({Y+1}PE{Y}CO.zip)
curl -fL -O "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/period-cohort-linked/2017PE2016CO.zip"
curl -fL -O "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/period-cohort-linked/2018PE2017CO.zip"
curl -fL -O "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/period-cohort-linked/2019PE2018CO.zip"
curl -fL -O "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/period-cohort-linked/2020PE2019CO.zip"
curl -fL -O "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/period-cohort-linked/2021PE2020CO.zip"
curl -fL -O "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/period-cohort-linked/2022PE2021CO.zip"
curl -fL -O "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/period-cohort-linked/2023PE2022CO.zip"
curl -fL -O "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/period-cohort-linked/2024PE2023CO.zip"
```

### Linked User Guides

```bash
cd raw_docs/linked
curl -fL -O "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/DVS/cohortlinkedus/LinkCO05Guide.pdf"
curl -fL -O "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/DVS/cohortlinkedus/LinkCO10Guide.pdf"
curl -fL -O "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/DVS/cohortlinkedus/LinkCO15Guide.pdf"
# Period-cohort user guides (one per cohort year starting 2020)
curl -fL -O "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/DVS/period-cohort-linked/21PE20CO_linkedUG.pdf"
curl -fL -O "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/DVS/period-cohort-linked/22PE21CO_linkedUG.pdf"
curl -fL -O "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/DVS/period-cohort-linked/23PE22CO_linkedUG.pdf"
curl -fL -O "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/DVS/period-cohort-linked/24PE23CO_linkedUG.pdf"
```

**Note**: Not every linked cohort year has its own user guide. The 2005, 2010, 2015 guides cover the denominator-plus format; the 2020 guide covers the period-cohort format. Positions are consistent within each format era.

## SSL certificate errors

If `curl` fails with SSL certificate errors (common on some Mac setups), either fix your system trust store or, as a last resort for this known `.gov` host:

```bash
curl -fkL -O "https://ftp.cdc.gov/…"
```

(`-k` disables certificate verification; use only if you understand the tradeoff.)

## Listing the file inside a zip

The ASCII file inside is very large once uncompressed. You only need the **name** for the inventory:

```bash
unzip -l raw_data/Nat2015us.zip
```

Unzipping is **optional**. The import scripts stream directly from the zip.

## Zip compression caveats

Some years use compression methods that Python's `zipfile` cannot stream. The import scripts detect this and fall back to `7z` automatically.

| Years | Method | Needs `7z`? |
|-------|--------|-------------|
| 1990–2008, 2014, 2018–2019, 2021–2024 | deflate | No |
| 2009–2013, 2016–2017, 2020 | deflate64 | Yes |
| 2015 | PPMd | Yes |
| Linked 2005–2012, 2015, 2021–2023 | deflate | No |
| Linked 2013, 2016–2020 | deflate64 | Yes |
| Linked 2014 | LZMA | Yes |

Install `7z`: `brew install p7zip` (macOS) or `apt install p7zip-full` (Linux).

## Update `metadata/file_inventory.csv`

For each year row:

1. **`imported`** — set to `true` when the zip is present under `raw_data/` (or `raw_data/linked/`).
2. **`notes`** — record the inner ASCII filename from `unzip -l`, the documentation filename, and optionally the download date.

Commit the CSV; do **not** commit the zips or the large text files.

## Terms of use

Review NCHS [Data Users Agreement](https://www.cdc.gov/nchs/data_access/restrictions.htm) and [Vital Statistics Data Release Policy](https://www.cdc.gov/nchs/nvss/dvs_data_release.htm) when distributing derivatives of these files.
