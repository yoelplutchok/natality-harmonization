# QA: yearly core Parquet

## Outputs

- Missingness (blank/whitespace-only): `/Users/yoelplutchok/Desktop/natality-harmonization/output/validation/qa_missingness_core_2016.csv`
- Frequencies (selected low-cardinality columns): `/Users/yoelplutchok/Desktop/natality-harmonization/output/validation/qa_frequencies_core_2016.csv`

## Notes

- These Parquet files store **raw fixed-width substrings**. Missingness here is defined as a field being blank after whitespace trimming.
- Some blanks are expected due to nonreporting areas and certificate revision differences (see NCHS user guides).

