# Smoke validation: parse pipeline (benchmark years)

## Checks

| Check | Result |
|-------|--------|
| Tooling | `parse_public_us_year.py` + `field_specs.py` (year-specific layouts) |
| 2005 zip | Deflate; parsed sample successfully |
| 2006 zip | Deflate; parsed sample successfully with configured 775-byte layout |
| 2009–2013 zips | Deflate64; parsed sample via `7z` fallback with configured 775-byte layout |
| 2014 zip | Deflate; parsed sample successfully |
| 2015 zip | PPMd; parsed sample via `7z` fallback |
| Record width checks | 2005: 1500, 2006-2013: 775, 2014/2015: 1345 (after stripping `\r\n`) |
| `DOB_YY` | Matches file year on sample rows (2005/2006/2009/2010/2013/2014/2015) |
| `DBWT` / `MAGER` | Numeric-looking strings, plausible ranges on small sample |

## Sample outputs (regenerable)

- `output/yearly_clean/natality_2005_core_sample.parquet`
- `output/yearly_clean/natality_2006_core_sample.parquet`
- `output/yearly_clean/natality_2009_core_sample.parquet`
- `output/yearly_clean/natality_2010_core_sample.parquet`
- `output/yearly_clean/natality_2013_core_sample.parquet`
- `output/yearly_clean/natality_2014_core_sample.parquet`
- `output/yearly_clean/natality_2015_core_sample.parquet`

These Parquet files are gitignored; re-run the script to reproduce.

## Full production parse (2005–2015)

On 2026-03-19, `scripts/01_import/parse_all_v1_years.py` wrote **`output/yearly_clean/natality_{year}_core.parquet`** for each year (chunked Parquet, ~285 MB total with legacy `*_sample.parquet` files). Row counts are recorded in **`metadata/validation_tracking.csv`**.

## Not done yet

- Row counts vs. **published NCHS** totals (external validation)  
- Missingness and frequency QA on full files  
- Harmonization, derived variables, full external validation tables  
