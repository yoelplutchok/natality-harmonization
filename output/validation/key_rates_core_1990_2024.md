# Key rates from harmonized derived core (resident-only)

Computed from `output/harmonized/natality_v2_harmonized_derived.parquet` after excluding foreign residents (`restatus=4`).

Years: 1990–2024

- CSV: `/Users/yoelplutchok/Desktop/natality-harmonization/output/validation/key_rates_core_1990_2024.csv`

## Notes

- **Low birthweight** uses `birthweight_grams_clean` (treats `9999` as missing).
- **Preterm** uses the harmonized `gestational_age_weeks` field, which is **best-available by year** (combined gestation for 2005–2013; obstetric estimate for 2014+).

