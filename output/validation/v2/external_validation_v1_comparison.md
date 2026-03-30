# External validation comparison (V1)

Computed from `output/harmonized/natality_v2_harmonized_derived.parquet` (resident-only universes use `is_foreign_resident == false`).

- Targets: `metadata/external_validation_targets_v2.csv`
- Output CSV: `output/validation/v2/external_validation_v1_comparison.csv`

## Summary

- pass: 82
- fail: 0
- missing expected or actual: 0

Notes:
- For many V1 variables (e.g., education/smoking in 2009–2013), the recommended comparison universe is `resident_revised`.

