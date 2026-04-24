# Downloads

This project is structured as a reproducible data release. The **large Parquet outputs are not committed** to the repository. Build them locally from NCHS raw inputs.

## Prerequisites

```bash
pip install -r requirements.txt
brew install p7zip   # required for deflate64 (2009-2013, 2016-2017, 2020) and PPMd (2015) zips
```

## Step 1: Download raw inputs

Follow `docs/DOWNLOAD_INSTRUCTIONS.md` for detailed URLs and instructions.

### Natality files (V2)

You should have in `raw_data/`:

- `Nat{year}.zip` for 1990-1993
- `Nat{year}us.zip` for 1994-2024

And in `raw_docs/`:

- `Nat{year}doc.pdf` for 1990-2004
- `UserGuide{year}.pdf` for 2005-2024 (plus 2009/2010 addenda)

### Linked files (V3)

You should have in `raw_data/linked/`:

- `LinkCO{YY}US.zip` for 2005-2015 (from `.../DVS/cohortlinkedus/`)
- `2017PE2016CO.zip` through `2024PE2023CO.zip` (from `.../DVS/period-cohort-linked/`)

And in `raw_docs/linked/`:

- `LinkCO05Guide.pdf`, `LinkCO10Guide.pdf`, `LinkCO15Guide.pdf`, `21PE20CO_linkedUG.pdf`

## Step 2: Build V2 Natality (1990-2024)

### Parse yearly extracts

```bash
python scripts/01_import/parse_all_v1_years.py --years 1990-2024
```

Outputs: `output/yearly_clean/natality_{year}_core.parquet` (one per year)

### Harmonize

```bash
python scripts/03_harmonize/harmonize_v1_core.py --years 1990-2024 \
  --out output/harmonized/natality_v2_harmonized.parquet
```

### Derive

```bash
python scripts/04_derive/derive_v1_core.py \
  --in output/harmonized/natality_v2_harmonized.parquet \
  --out output/harmonized/natality_v2_harmonized_derived.parquet
```

### Validate

```bash
python scripts/05_validate/validate_row_counts_vs_nchs.py
python scripts/05_validate/validate_v1_invariants.py \
  --in output/harmonized/natality_v2_harmonized_derived.parquet --years 1990-2024
python scripts/05_validate/compare_external_targets_v1.py
python scripts/05_validate/harmonized_missingness.py
```

## Step 2b: Generate convenience residents-only files (optional)

After building both V2 and V3 (Steps 2 and 3), generate pre-filtered residents-only subsets:

```bash
python scripts/06_convenience/write_residents_only.py
```

Outputs:
- `output/convenience/natality_v2_residents_only.parquet` (138.58M rows, 82 cols)
- `output/convenience/natality_v3_linked_residents_only.parquet` (74.79M rows, 92 cols)
- `output/convenience/PROVENANCE.md` (SHA-256 checksums for verification)

These drop `restatus` and `is_foreign_resident` (redundant after filtering) and are recommended for most analyses.

## Step 3: Build V3 Linked (2005-2023)

### Parse 2005-2015 (denominator-plus format)

```bash
python scripts/01_import/parse_all_linked_years.py --years 2005-2015
```

### Parse 2016-2023 (period-cohort format)

```bash
for cohort_year in 2016 2017 2018 2019 2020 2021 2022 2023; do
  period_year=$((cohort_year + 1))
  python scripts/01_import/parse_linked_cohort_year.py \
    --zip "raw_data/linked/${period_year}PE${cohort_year}CO.zip" \
    --year "$cohort_year" \
    --out "output/linked/linked_${cohort_year}_denomplus.parquet"
done
```

### Harmonize + derive

```bash
python scripts/03_harmonize/harmonize_linked_v3.py --years 2005-2023
python scripts/04_derive/derive_linked_v3.py
```

### Validate

```bash
python scripts/05_validate/validate_linked_parquets.py --years 2005-2023
python scripts/05_validate/compare_external_targets_v3_linked.py
```

## Smaller subsets

Build benchmark-year-only files for quick experiments:

```bash
# V2: just 1995, 2000, 2005, 2010, 2015, 2020
python scripts/03_harmonize/harmonize_v1_core.py \
  --years 1995,2000,2005,2010,2015,2020 \
  --out output/harmonized/natality_v2_harmonized_bench.parquet

# V3: just 2005, 2010, 2015, 2020
python scripts/03_harmonize/harmonize_linked_v3.py \
  --years 2005,2010,2015,2020 \
  --out output/harmonized/natality_v3_linked_bench.parquet
```

## Output file inventory

After a full build, `output/` contains:

```
output/
├── yearly_clean/
│   └── natality_{1990-2024}_core.parquet          # 35 raw yearly extracts
├── linked/
│   └── linked_{2005-2023}_denomplus.parquet        # 19 raw linked yearly extracts
├── harmonized/
│   ├── natality_v2_harmonized.parquet              # V2 stacked (138.8M rows, 71 cols)
│   ├── natality_v2_harmonized_derived.parquet      # V2 + derived (138.8M rows, 84 cols)
│   ├── natality_v3_linked_harmonized.parquet       # V3 stacked (74.9M rows, 78 cols)
│   └── natality_v3_linked_harmonized_derived.parquet  # V3 + derived (74.9M rows, 94 cols)
├── convenience/
│   ├── natality_v2_residents_only.parquet          # V2 residents only (138.6M rows, 82 cols)
│   ├── natality_v3_linked_residents_only.parquet   # V3 residents only (74.8M rows, 92 cols)
│   └── PROVENANCE.md                               # SHA-256 checksums + pipeline version
└── validation/
    ├── external_validation_v1_comparison.csv        # V2 external targets (183 targets)
    ├── external_validation_v3_linked_comparison.csv # V3 external targets (35 targets)
    ├── invariants_report_1990_2024.md               # V2 invariant checks + null-rate breaks
    ├── harmonized_missingness_by_year.csv           # Per-variable per-year null rates
    ├── harmonized_missingness_breaks.csv            # >5 ppt year-over-year null-rate jumps
    └── linked_validation_2005_2023.csv              # V3 parsing validation
```
