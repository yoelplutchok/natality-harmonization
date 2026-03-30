# How to download NCHS natality documentation and data

Everything below is the **official NCHS** public-use natality product described in [DATA_SOURCE_V1.md](DATA_SOURCE_V1.md).

## Where the files live on the web

1. Open the NCHS portal: **[Vital Statistics Online — Downloadable Data Files](https://www.cdc.gov/nchs/data_access/Vitalstatsonline.htm)**.
2. Scroll to **Birth Data Files**.
3. You will see two parallel lists:
   - **User’s Guide (.pdf)** — documentation for that year’s layout and codes.
   - **U.S. Data (.zip)** — the microdata (`Nat{year}us.zip`).

Direct folder pattern (same files as the portal links):

- Documentation:  
  `https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/DVS/natality/`
- Data zips:  
  `https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/natality/`

## What to save on your machine (this repo)

| Kind | Put it here |
|------|----------------|
| User Guide PDFs | `raw_docs/` |
| Natality zips | `raw_data/` |

These folders are listed in `.gitignore` for large/binary content so you do not accidentally commit gigabytes of data. The **inventory** in `metadata/file_inventory.csv` is what you version-control instead.

## Exact files for your benchmark years

### User guides (PDF)

Save into **`raw_docs/`**:

| File | URL |
|------|-----|
| `UserGuide2005.pdf` | https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/DVS/natality/UserGuide2005.pdf |
| `UserGuide2009.pdf` | https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/DVS/natality/UserGuide2009.pdf |
| `UserGuide2009_Addendum.pdf` | https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/DVS/natality/UserGuide2009_Addendum.pdf |
| `UserGuide2010.pdf` | https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/DVS/natality/UserGuide2010.pdf |
| `UserGuide2010_Addendum.pdf` | https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/DVS/natality/UserGuide2010_Addendum.pdf |
| `UserGuide2015.pdf` | https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/DVS/natality/UserGuide2015.pdf |

### Data (ZIP)

Save into **`raw_data/`** (example for a small test year; 2015 is among the smaller U.S. annual zips in this period):

| File | URL |
|------|-----|
| `Nat2015us.zip` | https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/natality/Nat2015us.zip |

Repeat the same URL pattern for other years: `Nat2005us.zip` … `Nat2014us.zip`.

## Method A — Browser (simplest)

1. Click each URL above (or use the Vital Statistics Online page).
2. When the PDF or ZIP downloads, **move or save** it into:
   - `natality-harmonization/raw_docs/` for PDFs  
   - `natality-harmonization/raw_data/` for ZIPs  
3. Keep the **exact** filenames (`UserGuide2015.pdf`, `Nat2015us.zip`, …) so they match `metadata/file_inventory.csv`.

## Method B — Terminal (`curl`)

From the project root:

```bash
cd raw_docs
curl -fL -O "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/DVS/natality/UserGuide2005.pdf"
curl -fL -O "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/DVS/natality/UserGuide2009.pdf"
curl -fL -O "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/DVS/natality/UserGuide2009_Addendum.pdf"
curl -fL -O "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/DVS/natality/UserGuide2010.pdf"
curl -fL -O "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/DVS/natality/UserGuide2010_Addendum.pdf"
curl -fL -O "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/DVS/natality/UserGuide2015.pdf"

cd ../raw_data
curl -fL -O "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/natality/Nat2015us.zip"
```

If `curl` fails with **SSL certificate** errors (common on some Mac setups), either fix your system trust store or, as a last resort for this known `.gov` host:

```bash
curl -fkL -O "https://ftp.cdc.gov/…"
```

(`-k` disables certificate verification; use only if you understand the tradeoff.)

## List the file inside a zip (without unpacking the whole thing)

The ASCII file inside is **very large** once uncompressed (billions of characters). You only need the **name** for the inventory:

```bash
cd raw_data
unzip -l Nat2015us.zip
```

Example (2015 U.S. file): one text file named like `Nat2015PublicUS.c20160517.r20160907.txt`.

Unzipping is **optional**. The import scripts in this repo stream directly from the zip, so you do **not** need to unpack the multi-GB ASCII file just to parse it.

If you still want an uncompressed local copy (needs **several GB** free disk space):

```bash
unzip Nat2015us.zip
```

## Update `metadata/file_inventory.csv`

For each year row:

1. **`imported`** — set to `true` when the **`Nat{year}us.zip`** for that year is present under `raw_data/` (and you intend to use it). If you only downloaded PDFs so far, leave `false`.
2. **`notes`** — record:
   - that the User Guide (and addendum if any) is in `raw_docs/`
   - the **inner** ASCII filename from `unzip -l` once you have the zip
   - optional: download date

Commit the CSV; do **not** commit the zip or huge `.txt` unless you deliberately use Git LFS or another data hosting approach.

### Zip compression caveats (deflate64, PPMd)

Some years use zip compression methods that Python’s `zipfile` cannot stream:

- **2009–2013**: **deflate64** (zip method 9) → requires `7z`
- **2015**: **PPMd** (zip method 98) → requires `7z`

Years **2005–2008** and **2014** use standard **deflate** and stream fine with Python alone. The import script automatically falls back to `7z` when needed (install: `brew install p7zip`).

### Status in this repository

As of the last automated fetch, **2005–2015** User Guides (including 2009/2010 addenda) and all **`Nat{year}us.zip`** files are present locally; `metadata/file_inventory.csv` is filled with inner member names (e.g. `Nat2005us.dat` for 2005–2008, revision-tagged names for 2009–2013, `.txt` for 2014–2015). Re-download from NCHS if they post a revised zip.

## Terms of use

Review NCHS [Data Users Agreement](https://www.cdc.gov/nchs/data_access/restrictions.htm) and [Vital Statistics Data Release Policy](https://www.cdc.gov/nchs/nvss/dvs_data_release.htm) when distributing derivatives of these files.
