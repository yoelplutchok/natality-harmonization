# The U.S. Natality Harmonization Project: A Complete Explanation

## What this project is, in one sentence

We took 35 years of U.S. birth certificate data (1990-2024) — scattered across dozens of inconsistent government files — and turned it into a single, clean, ready-to-use dataset of 138.8 million birth records with 84 standardized columns, plus a companion dataset of 74.9 million linked birth-infant death records (94 columns) for mortality research.

---

## The big picture: why this matters

Every time a baby is born in the United States, a birth certificate is filed. That certificate captures detailed information: the mother's age, race, education, and marital status; whether she smoked during pregnancy; her medical conditions; how the baby was delivered; the baby's weight, gestational age, and sex; and much more. The National Center for Health Statistics (NCHS), part of the CDC, compiles these certificates into annual data files and makes them publicly available for research.

These files are one of the most important data sources in public health. Researchers use them to study trends in preterm birth, racial disparities in infant mortality, the effects of Medicaid expansion on prenatal care, the rise of cesarean deliveries, and hundreds of other questions. The data covers every single registered birth in the country — not a sample, but the entire population.

The problem is that using these files is extraordinarily difficult.

---

## The problem: what researchers faced before this project

### The raw data is a mess

The NCHS releases one file per year. Each file is a compressed archive containing a single enormous text file — no headers, no column names, just a wall of characters. Each line is one birth record, and the fields are defined by their exact position in the line: characters 70-71 might be the mother's age, character 189 might be the baby's sex, characters 206-209 might be the birthweight.

Here is what an actual raw record looks like (abbreviated). This is a single birth:

```
1 2WV1219991211  12 ...
```

To know that the `12` at position 70-71 means the mother is 12 years old, you need to look it up in a separate PDF user guide published by NCHS for that specific year.

### The format keeps changing

Over 35 years, NCHS has used **five different record layouts**:

| Years | Record length | What changed |
|-------|--------------|--------------|
| 1990-2002 | 350 bytes per line | Original format (1989 birth certificate) |
| 2003 | 1,350 bytes per line | New certificate introduced; format expanded |
| 2004 | 1,500 bytes per line | Layout shifted again |
| 2005-2013 | 775-1,500 bytes per line | Dual certificate period (old and new running simultaneously) |
| 2014-2024 | 1,345 bytes per line | All states on the new certificate |

This means the mother's age is at position 70-71 in one era, position 89-90 in another, and position 75-76 in yet another. Every field jumps around.

### The variable names and codes keep changing

It's not just the positions. The actual names and meaning of fields changed too:

- **Maternal age** was called `DMAGE` from 1990-2002, then became `MAGER` from 2004 onward. In 2003 specifically, the single-year age was suppressed entirely and replaced with a 41-category recode called `MAGER41`.

- **Education** was measured as years of schooling (0-17) from 1990-2002, then changed to categorical codes (less than high school, high school, some college, bachelor's+) with the new certificate. The two aren't directly comparable.

- **Delivery method** used five codes (vaginal, VBAC, primary cesarean, repeat cesarean, not stated) from 1990-2004, then switched to three codes (vaginal, cesarean, not stated) from 2005 onward — and to make it worse, the field was labeled the same thing (`DMETH_REC`) in 2003-2004 but actually used the old coding.

- **Gestational age** was measured by last menstrual period (LMP) from 1990-2002, by a combination of LMP and clinical estimate from 2003-2013, and by obstetric estimate from 2014 onward. Each method gives systematically different results, so the preterm birth rate appears to jump at each transition.

- **Smoking** was recorded using two independent fields from 1990-2002 (one for yes/no, one for intensity), but from 2003 onward it was derived from a single intensity field. And from 2009-2013, smoking data was only available for states that had adopted the new certificate.

### Some years have missing data by design

From 2003 to 2013, states were gradually adopting the new (2003 revision) birth certificate. During this transition, some births were recorded on the old form and some on the new form. Starting in 2009, NCHS stopped carrying forward the old-form versions of certain fields in the public-use files, so variables like education, prenatal care, and smoking are simply blank for all old-form births in 2009-2013.

This isn't random missingness — it's structural. In 2009, about 44% of births are missing smoking data, not because those women's smoking status is unknown, but because their states hadn't adopted the new form yet. By 2013 it dropped to 14%, and by 2014 it was gone (all states on the new form).

### Special codes masquerade as real values

NCHS uses numeric sentinel values for "unknown": 99 for age, 99 for gestational weeks, 9999 for birthweight, 9 for medical risk factors. These aren't null — they're actual numbers in the data. A researcher who runs `WHERE diabetes IS NOT NULL` will include all the unknowns in their denominator. A researcher who calculates average birthweight without filtering will include records coded as 9,999 grams (22 pounds).

### The bottom line before this project

To study something as straightforward as "how has the low birthweight rate changed from 1990 to 2024," a researcher would need to:

1. Download 35 separate zip files from the CDC FTP server
2. Handle three different compression algorithms (some files use non-standard compression that Python's built-in library can't read)
3. Parse fixed-width text using five different layouts, looking up byte positions in 35 separate PDF user guides
4. Reconcile field names that changed (DMAGE vs MAGER vs MAGER41)
5. Handle the 2003 special case where age is a recode
6. Map sentinel values (9999) to null
7. Filter to resident births only (to match official statistics)
8. Know that gestational age measurement changed twice and adjust accordingly

This work has been duplicated by hundreds of research teams, each solving the same problems independently, often with subtle errors and rarely with enough documentation for others to reproduce their work.

---

## What this project built: the solution

### The pipeline

We built an automated pipeline that handles all of the above. It has five stages:

```
Raw NCHS files (.zip)
    |
    v
[1. Parse] -- Extract fields from fixed-width records using year-specific byte positions
    |
    v
Per-year Parquet files (raw field values, one file per year)
    |
    v
[2. Harmonize] -- Map era-specific fields to a single common schema
    |
    v
Single stacked Parquet file: 71 columns, all years unified
    |
    v
[3. Derive] -- Compute analysis-ready indicators (LBW, preterm, age groups, etc.)
    |
    v
Single stacked Parquet file: 84 columns, all years unified
    |
    v
[4. Validate] -- Check against 183+ published NCHS benchmarks
    |
    v
Validation reports (all pass)
    |
    v
[5. Convenience] -- Filter to residents-only subsets
    |
    v
Final output files ready for analysis
```

### Stage 1: Parsing

The parser reads each year's zip file and extracts the relevant fields based on that year's layout. For example:

- For 1995 (350-byte records): maternal age is characters 70-71, birthweight is characters 193-196, infant sex is character 189
- For 2010 (775-byte records): maternal age is characters 89-90, birthweight is characters 463-466, infant sex is character 436
- For 2020 (1,345-byte records): maternal age is characters 75-76, birthweight is characters 504-507, infant sex is character 475

The parser handles the compression problems too — some files use deflate64 or PPMd compression that Python can't open natively, so the pipeline automatically falls back to the 7z utility for those years.

Output: one Parquet file per year with raw string values, named columns instead of byte positions.

### Stage 2: Harmonization

This is the core of the project. The harmonizer takes 35 years of inconsistently named, differently coded, variably positioned fields and maps them all to a single 71-column schema. Every column has one name, one data type, and one set of allowed values across all 35 years.

Here are some concrete examples of what harmonization looks like:

**Maternal age:**
- 1990-2002: Read `DMAGE` field (two-digit integer, straightforward)
- 2003 only: Read `MAGER41` (41-category recode) and convert back to approximate single-year age using the formula: code 1 maps to age 14, codes 2-41 map to code + 13
- 2004-2024: Read `MAGER` field (two-digit integer, straightforward)
- All eras: 99 is mapped to null
- Output: a single `maternal_age` column (integer) for all 35 years

**Education:**
- 1990-2002: Read `DMEDUC` (years of schooling, 0-17) and convert: 0-11 becomes "less than high school," 12 becomes "high school graduate," 13-15 becomes "some college," 16-17 becomes "bachelor's degree or higher"
- 2003-2024: Read the appropriate categorical field for each era and map to the same four categories
- Output: a single `maternal_education_cat4` column (string) for all 35 years

**Race/ethnicity:**
- 1990-2002: Apply an approximate crosswalk from raw race detail codes to four categories (White, Black, American Indian/Alaska Native, Asian/Pacific Islander)
- 2003-2019: Use the official NCHS bridged race field
- 2020-2024: NCHS dropped bridged race from the public file, so the pipeline reconstructs a five-category race/ethnicity variable from the MRACE6 detail codes. Multiracial births (~3% of the total) can't be bridged to a single group and are set to null.
- Output: `maternal_race_ethnicity_5` plus a `race_bridge_method` column that tells you which derivation was used for each record

**Medical risk factors (diabetes, hypertension):**
- 1990-2002: Read individual yes/no/unknown flag fields
- 2003-2013: Read combined "unified risk factor" fields
- 2014-2024: The unified fields are blank in the public file, so the pipeline derives values from the revised-certificate-specific fields (e.g., combining pre-pregnancy diabetes and gestational diabetes into a single diabetes indicator)
- Output: `diabetes_any`, `hypertension_chronic`, `hypertension_gestational` — all coded as 1 (yes), 2 (no), 9 (unknown)

**Delivery method:**
- 1990-2004: Five-code system (vaginal, VBAC, primary cesarean, repeat cesarean, not stated)
- 2005-2024: Three-code system (vaginal, cesarean, not stated)
- The pipeline preserves the era-specific codes but documents the validated crosswalk: for 1990-2004, cesarean = codes 3 + 4; for 2005+, cesarean = code 2
- Output: `delivery_method_recode` with a documented comparability note

The harmonizer also adds metadata columns that don't exist in the original data:
- `certificate_revision`: which birth certificate version was used for each record (critical for understanding 2003-2013 missingness)
- `gestational_age_weeks_source`: which measurement method was used (LMP, combined, or obstetric estimate)
- `race_bridge_method`: which race derivation was applied
- `marital_reporting_flag`: whether the birth occurred in a state that reports marital status (important because California stopped reporting in 2017, creating ~11% missingness)

### Stage 3: Derivation

The derivation step adds 13 computed columns that researchers commonly need:

**Sentinel cleanup (turning fake values into proper nulls):**
- `birthweight_grams_clean`: birthweight with 9999 replaced by null
- `gestational_age_weeks_clean`: gestational age with 99 replaced by null
- `apgar5_clean`: Apgar score with 99 replaced by null

**Binary outcome indicators:**
- `low_birthweight`: true if birthweight < 2,500 grams
- `very_low_birthweight`: true if birthweight < 1,500 grams
- `preterm_lt37`: true if gestational age < 37 weeks
- `very_preterm_lt32`: true if gestational age < 32 weeks
- `singleton`: true if not a multiple birth

**Age categories:**
- `maternal_age_cat`: standard groups (<20, 20-24, 25-29, 30-34, 35-39, 40+)
- `father_age_cat`: same grouping

**Medical risk factor booleans (the sentinel fix):**
- `diabetes_any_bool`: converts 1/2/9 integer coding to true/false/null
- `hypertension_chronic_bool`: same conversion
- `hypertension_gestational_bool`: same conversion

This last group is particularly important. In the raw harmonized fields, the value 9 means "unknown" but it's stored as the integer 9, not as null. A standard SQL or pandas filter like `WHERE diabetes_any IS NOT NULL` would include unknowns in the denominator — a common source of errors. The boolean versions fix this: 9 becomes null, so standard null-handling logic works correctly.

For the linked birth-infant death dataset, the derivation step adds 16 columns including:
- `neonatal_death`: true if the infant died before 28 days of age
- `postneonatal_death`: true if the infant died between 28 days and 1 year
- `cause_group`: a 13-category classification of the cause of death (congenital anomalies, short gestation/low birthweight, SIDS, bacterial sepsis, etc.) based on ICD-10 codes, following the standard NCHS framework

### Stage 4: Validation

This is where we prove the data is correct. The validation suite runs three types of checks:

**Row count validation (31 years checked):**
For every year from 1994 to 2024, the number of records we parsed is compared against the expected count implied by the file size divided by the record length. All 31 match exactly. Resident birth counts are also compared against NCHS published totals — all match.

**External validation against published statistics (183 targets for natality, 35 for linked):**
We computed rates from our harmonized data and compared them against values published in official NCHS reports:

- Low birthweight rates (1990-2023): matched against NCHS National Vital Statistics Reports
- Preterm birth rates (1990-2023): matched against NCHS reports and childstats.gov
- Cesarean delivery rates (1990-2024): matched against published NVSR tables
- Twin and triplet+ rates: matched against NCHS Data Briefs
- Smoking prevalence (2016-2023): matched against NVSR
- Medicaid share (2016-2023): matched against NVSR

All 183 natality targets pass within stated tolerances (exact match for counts; tiny rounding differences for rates). All 35 linked targets pass — infant mortality rates, neonatal deaths, postneonatal deaths all match the NCHS user guide values.

**Internal invariant checks (38 checks):**
Deterministic rules that must hold true for every record:
- No birthweight above 8,165 grams or coded 9999 after cleaning
- No gestational age of 99 after cleaning
- No Apgar score of 99 after cleaning
- If a woman is coded as a smoker (2003+), her smoking intensity must be nonzero
- Congenital anomaly fields must be null for all pre-2014 records (they didn't exist yet)
- Father education must be null for 1995-2008 (the field was dropped from public files)

All 41 checks produce zero violations across all 138.8 million records (V2). The V3 linked file is validated separately — see `docs/COMPARABILITY.md` for the three V2-only invariants that are skipped in V3 mode and the two NCHS-upstream survivor rows with null `record_weight` that are counted as known exceptions.

### Stage 5: Convenience outputs

The final stage produces filtered subsets for common use cases:
- Residents-only files (excluding births to foreign residents) — this is the standard NCHS analysis universe
- Parquet files with embedded provenance metadata (git hash, build timestamp, SHA-256 checksums)

---

## The before and after: a practical comparison

### Before (raw NCHS files)

**What a researcher gets:** 35 zip files, each containing a wall of text with no headers. To use them, you need 35 PDF user guides and substantial programming expertise.

**To answer "what was the low birthweight rate in 2005?":**
1. Download Nat2005us.zip
2. Discover it uses deflate64 compression (Python's zipfile can't open it)
3. Install 7z, extract the file
4. Read the 2005 User Guide PDF to find that birthweight is at byte positions 467-470
5. Parse 4,138,349 lines of fixed-width text
6. Know that 9999 means unknown (not 9,999 grams)
7. Know that RESTATUS field at position 138 needs to equal 1, 2, or 3 (not 4) for resident births
8. Calculate: count of records where birthweight < 2500 and birthweight != 9999, divided by all records where birthweight != 9999

**To answer "how has the low birthweight rate changed from 1990 to 2024?":**
Repeat the above 35 times, with different byte positions, field names, and compression formats for each year. Then reconcile the fact that birthweight is at position 206-209 for 1990-2002 and at completely different positions for later years. Hope you didn't make an off-by-one error in any of the 35 layouts.

### After (harmonized dataset)

**What a researcher gets:** A single Parquet file (or the residents-only convenience file) that opens directly in Python, R, or any modern analytics tool. Every column has a meaningful name. Every year uses the same schema.

**To answer "what was the low birthweight rate in 2005?":**
```python
import pyarrow.parquet as pq

df = pq.read_table(
    "natality_v2_residents_only.parquet",
    columns=["year", "low_birthweight"],
    filters=[("year", "=", 2005)]
).to_pandas()

rate = df["low_birthweight"].mean() * 100
print(f"LBW rate 2005: {rate:.2f}%")
```

**To answer "how has the low birthweight rate changed from 1990 to 2024?":**
```python
df = pq.read_table(
    "natality_v2_residents_only.parquet",
    columns=["year", "low_birthweight"]
).to_pandas()

trend = df.groupby("year")["low_birthweight"].mean() * 100
print(trend)
```

That's it. No byte positions, no PDF lookups, no compression workarounds, no sentinel value traps. The column is already a boolean, already cleaned, and already filtered to residents.

### More examples of what becomes easy

**Infant mortality by cause of death over time:**
```python
linked = pq.read_table("natality_v3_linked_residents_only.parquet",
                        columns=["year", "infant_death", "cause_group"])

deaths = linked.filter(linked["infant_death"] == True)
deaths.groupby(["year", "cause_group"]).count()
```

**Cesarean rate by maternal age group:**
```python
df = pq.read_table("natality_v2_residents_only.parquet",
                    columns=["year", "maternal_age_cat", "delivery_method_recode"],
                    filters=[("year", ">=", 2005)])

# delivery_method_recode == 2 means cesarean for 2005+
df["cesarean"] = df["delivery_method_recode"] == 2
df.groupby(["year", "maternal_age_cat"])["cesarean"].mean()
```

**Smoking rate trend on the revised-certificate subset (avoiding the 2009-2013 missingness trap):**
```python
df = pq.read_table("natality_v2_residents_only.parquet",
                    columns=["year", "certificate_revision",
                             "smoking_any_during_pregnancy"],
                    filters=[("certificate_revision", "=", "revised_2003")])

df.groupby("year")["smoking_any_during_pregnancy"].mean()
```

---

## The comparability system: what researchers need to know

Not all 84 columns are perfectly comparable across all 35 years. The project is explicit about this. Every variable is assigned a comparability class:

### Full comparability (trend-safe across 1990-2024)

These variables have stable definitions and coding across all 35 years. You can use them in trend analyses without any special handling:

- Birth year, resident status, infant sex, plurality
- Birthweight (in grams, after sentinel cleanup)
- Apgar score (after sentinel cleanup)
- Live birth order, total birth order
- All derived indicators based on the above (low birthweight, singleton, etc.)

### Partial comparability (usable with documented rules)

These variables are available across all or most years, but have known structural breaks that researchers need to account for:

- **Maternal age**: Fully comparable except for 2003, where age is approximated from a 41-category recode
- **Race/ethnicity**: Three different derivation methods across eras (approximate bridge, official NCHS bridge, reconstructed from detail codes). A `race_bridge_method` column tells you which was used.
- **Education**: Conceptual break between years-of-schooling (1990-2002) and categorical coding (2003+). Structural missingness for old-form births in 2009-2013.
- **Smoking**: Two independent source fields in 1990-2002 vs. one in 2003+. Structural missingness in 2009-2013.
- **Gestational age / preterm**: Three measurement methods (LMP, combined, obstetric estimate) with transitions at 2003 and 2014. A `gestational_age_weeks_source` column identifies the method.
- **Delivery method**: Five-code system through 2004, three-code system from 2005. Cesarean binary is comparable via documented crosswalk.
- **Marital status**: 0% missing through 2016, then ~11-12% missing from 2017 onward due to California's nonreporting policy. A `marital_reporting_flag` column distinguishes nonreporting from genuinely unknown.
- **Medical risk factors**: Comparable coding (1/2/9) but underlying ascertainment changed with the certificate revision.

### Within-era only (meaningful only in a specific time window)

These variables exist only for recent years and should not be compared across eras:

- **2014-2024 only**: Pre-pregnancy BMI, weight gain, NICU admission, induction of labor, breastfed at discharge, 12 congenital anomaly indicators, 5 infection indicators, fertility treatment, prior cesarean count, pre-pregnancy diabetes (separate from gestational)
- **2009-2024 only**: Payment source (Medicaid, private insurance, etc.)

---

## The linked birth-infant death dataset

In addition to the natality files, NCHS produces "linked" files that connect each infant death certificate back to the birth certificate of the same child. This allows researchers to study infant mortality by birth characteristics — for example, what is the infant mortality rate for babies born at low birthweight, or for babies born to teenage mothers?

The linked files present their own challenges:

- **Two different formats**: For 2005-2015, the death information is appended directly to the birth record (one long line per birth). For 2016-2023, deaths are in a separate file that must be merged with the birth file using a sequence number.
- **Different death flag coding**: 2005-2013 uses 1 = death, 2 = survivor. 2014-2023 uses 1 = death, blank = survivor.
- **Birthweight is at a different position** in the linked files than in the natality files (because the linked files use an imputed birthweight field).
- **The 2017 file** required special handling: sequence numbers had leading zeros that needed to be stripped before the merge would work.

The pipeline handles all of this transparently and produces a single 94-column Parquet file covering 2005-2023, with both birth-side and death-side information for all 74.9 million births in those cohorts.

---

## What's in the output files

### V2 Natality (1990-2024)

| File | Rows | Columns | Use case |
|------|------|---------|----------|
| natality_v2_harmonized.parquet | 138,819,655 | 71 | Core harmonized fields only |
| natality_v2_harmonized_derived.parquet | 138,819,655 | 84 | Harmonized + derived indicators (most users want this) |
| natality_v2_residents_only.parquet | 138,582,904 | 82 | Pre-filtered to U.S. residents (recommended starting point) |

### V3 Linked Birth-Infant Death (2005-2023)

| File | Rows | Columns | Use case |
|------|------|---------|----------|
| natality_v3_linked_harmonized.parquet | 74,943,824 | 78 | Core harmonized with death fields |
| natality_v3_linked_harmonized_derived.parquet | 74,943,824 | 94 | Harmonized + derived (neonatal/postneonatal death, cause groups) |
| natality_v3_linked_residents_only.parquet | 74,785,708 | 92 | Pre-filtered to U.S. residents |

All files use Apache Parquet format, which is readable by Python (pandas, PyArrow), R (arrow package), Stata (via conversion), and most modern data tools. Parquet supports column selection and row filtering at read time, so you never need to load the full 138.8 million rows into memory if you only need a subset.

---

## Project structure

```
natality-harmonization/
|
|-- raw_data/                  # NCHS zip files (not committed to git; ~40 GB)
|   |-- linked/                # Linked birth-infant death zips
|
|-- raw_docs/                  # NCHS User Guide PDFs (layout documentation)
|   |-- linked/                # Linked file user guides
|
|-- metadata/                  # Schema definitions, field inventories, validation targets
|   |-- harmonized_schema.csv  # The canonical column-level schema (provenance, types, rules)
|   |-- file_inventory.csv     # Tracks all source file URLs and download status
|
|-- scripts/
|   |-- 01_import/             # Stage 1: Parse fixed-width -> per-year Parquet
|   |-- 03_harmonize/          # Stage 2: Map era-specific fields -> common schema
|   |-- 04_derive/             # Stage 3: Compute LBW, preterm, age groups, booleans
|   |-- 05_validate/           # Stage 4: Invariant checks, external benchmarks, missingness
|   |-- 06_convenience/        # Stage 5: Residents-only subsets with provenance
|   |-- 07_figures/            # Publication-ready figures for the data descriptor paper
|
|-- output/
|   |-- yearly_clean/          # Per-year Parquet (intermediate; one file per year)
|   |-- linked/                # Per-year linked Parquet (intermediate)
|   |-- harmonized/            # Final stacked harmonized + derived Parquet files
|   |-- convenience/           # Residents-only subsets
|   |-- validation/            # Validation reports and machine-readable results
|
|-- figures/                   # Publication-ready figures (PDF + PNG)
|-- notebooks/                 # Quickstart examples
|-- docs/                      # Documentation (codebook, comparability, FAQ, etc.)
```

---

## Validation: how we know the data is correct

This project doesn't just transform the data — it proves the transformation is correct.

**183 external benchmarks** were compiled from official NCHS publications (National Vital Statistics Reports, NCHS Data Briefs, the CDC Data API). For each benchmark, we computed the same metric from our harmonized data and compared. Examples:

- The published resident birth count for 2023 is 3,596,017. Our data produces 3,596,017. Exact match.
- The published low birthweight rate for 2019 is 8.31%. Our data produces 8.31%. Exact match.
- The published cesarean rate for 1995 is 20.8%. Our data produces 20.84% (= 806,722 / 3,870,446 cesareans among known delivery method; rounds to the published 20.8%). Match within tolerance.

All 183 targets pass. For the linked dataset, all 35 targets pass (infant mortality rates, neonatal deaths, postneonatal deaths, etc.).

**41 internal invariant checks** verify logical consistency (V2 natality; V3 linked adds one allowed exception — two NCHS-upstream survivor rows with null `record_weight` — and skips three V2-only structural-coverage invariants that don't apply to linked 2009–2010 files):
- No impossible birthweights (the range is 227-8,165 grams after sentinel cleanup)
- No impossible gestational ages (the observed range is 17-47 weeks after sentinel cleanup; the validator-enforced bound is 12-47)
- If a record says the mother smoked (2003+), the smoking intensity is nonzero
- Congenital anomaly fields are null for all pre-2014 records
- Delivery method codes are only in the allowed set per era ({1,2,3,4,9} for 1990-2004; {1,2,9} for 2005+)
- Father's Hispanic-vs-race-eth consistency (if `father_hispanic == true` then `father_race_ethnicity_5 == "Hispanic"`)
- And 35 more checks

Zero violations across 138.8 million V2 records; V3 linked passes with the two documented `record_weight` exceptions.

---

## Who this is for

- **Public health researchers** studying birth outcomes, infant mortality, health disparities, or the effects of policy interventions across time
- **Epidemiologists** who need record-level microdata (not pre-tabulated aggregates) for regression, decomposition, or causal inference
- **Data scientists** who want a large, well-documented, real-world dataset
- **Students and educators** who need accessible population-level health data for teaching or coursework
- **Anyone** who has ever tried to use raw NCHS natality files and wished they didn't have to parse fixed-width text from 1992

---

## Citation and access

The dataset is deposited on Zenodo: [https://doi.org/10.5281/zenodo.19363075](https://doi.org/10.5281/zenodo.19363075)

The full processing pipeline is open source: [https://github.com/yoelplutchok/natality-harmonization](https://github.com/yoelplutchok/natality-harmonization)

The source data are produced by the National Center for Health Statistics, Centers for Disease Control and Prevention. Always cite NCHS as the source of the underlying public-use microdata.
