# Analysis Plan: Quantifying the Preterm Birth Measurement Artifact

## Working title

**"How Much of the U.S. Preterm Birth Trend Is Real? Quantifying the Gestational Age Measurement Artifact Across Three Estimation Eras, 1990-2024"**

## Motivation

U.S. preterm birth rates appear to follow a dramatic trajectory: rising from 10.4% (1990) to 12.8% (2006), declining to 11.4% (2013), then dropping sharply to 9.6% (2014) before climbing again to 10.4% (2023). However, the gestational age measurement method changed twice — from LMP-only (1990-2002) to a combined measure (2003-2013) to the obstetric estimate (2014-2024) — and each transition shifts the apparent preterm rate.

The 2014 transition is particularly stark: the published preterm rate dropped 1.82 percentage points in a single year. Using 2014 data where **both methods are simultaneously populated** for ~4 million births, we can directly quantify that 1.75 of those 1.82 percentage points — **96% of the apparent drop** — is pure measurement artifact.

Despite NCHS flagging this comparability break (Martin et al., NVSR 64/5, 2015), no published paper has formally decomposed the multi-decade preterm trend into measurement artifact vs. real epidemiological change. Hundreds of published papers report preterm trends spanning these boundaries without adjustment.

## Key prior work

- **Martin et al. 2015 (NVSR 64/5)**: Foundational NCHS transition report. Quantified the level difference (OE preterm 9.62% vs LMP 11.39% in 2013). Showed parallel trends 2007-2013. Did NOT decompose artifact vs. real change across full trend.
- **Duryea et al. 2015 (AJOG)**: Confirmed OE captures "real" prematurity better (higher LBW/NICU rates among OE-defined preterms).
- **Ambrose et al. 2015 (J Perinatol)**: LMP overestimates prematurity by ~20%.
- **Barfield et al. 2016 (MMWR)**: Kitagawa decomposition of preterm decline by maternal age — NOT by measurement method.
- **Ananth 2007**: 1990-2002 analysis showing LMP and clinical estimate trends were "fairly similar" in direction.

**The gap**: No formal decomposition partitioning the observed preterm trend into measurement artifact vs. real change. No measurement-adjusted trend series. No quantification of bias in published literature.

## Data source

- **V2 Natality** (1990-2024): 138.8M births, harmonized with `gestational_age_weeks_source` tracking measurement era
- **Key variable**: `gestational_age_weeks_source` — values: `lmp` (1990-2002), `combined` (2003-2013), `obstetric_estimate` (2014-2024)
- **Natural experiment**: Raw yearly_clean parquet files for 2014-2024 contain BOTH `COMBGEST` and `OEGEST_COMB` for every birth (~4M/year, 11 years, ~44M births total)

## The natural experiment (2014-2024)

In all years 2014-2024, the raw NCHS files populate both gestational age fields:
- **COMBGEST** (combined/LMP-based measure)
- **OEGEST_COMB** (obstetric estimate)

Key statistics from 2014:
- Both valid: 3,994,872 births (99.9%)
- Methods disagree: 1,465,261 births (**36.7%**)
- Preterm by COMBGEST: **11.31%**
- Preterm by OEGEST_COMB: **9.56%**
- **Direct measurement artifact: 1.75 percentage points**
- Mean GA difference: -0.14 weeks (OE slightly lower)

## Analysis structure

### Part 1: The dual-method comparison (2014-2024)

**Table 1. Preterm rates by measurement method on identical births, 2014-2024**
- For each year: preterm rate using COMBGEST vs OEGEST_COMB on the same cohort
- Agreement rate, discordance rate, mean GA difference
- Shows: the artifact is stable across years (~1.7-1.8 ppt consistently)

**Table 2. Discordance by gestational age week**
- Cross-tabulation: GA by COMBGEST vs GA by OEGEST_COMB
- Show where reclassification concentrates (likely 35-38 week range)

**Table 3. Discordance by maternal characteristics**
- Does the artifact vary by race/ethnicity, maternal age, BMI, education?
- Prior work (Wingate 2007) suggests LMP inflates racial disparities
- If artifact is larger for certain groups, published disparity estimates are biased

### Part 2: The measurement-adjusted trend (1990-2024)

**Figure 1. Published vs. adjusted preterm rate, 1990-2024**
- Line 1: Published preterm rate (as reported, using the prevailing method each year)
- Line 2: Adjusted preterm rate — apply bridging factor from Part 1 to back-adjust the entire series to a common OE-equivalent basis
- Show: the apparent 2014 cliff disappears; the "true" trend is smoother

**Method for bridging**: Use the 2014 dual-method data to compute year-specific and subgroup-specific adjustment factors. Apply these to the 2003-2013 combined-era rates and (with appropriate caveats) to the 1990-2002 LMP-era rates.

**Table 4. Adjusted preterm rates by era**
- For each year: published rate, adjustment factor, adjusted rate
- Show the "true" trend net of measurement changes

### Part 3: Impact on published research

**Table 5. Bias in published preterm trend analyses**
- Scoping review: identify N papers (2015-2024) in PubMed that report preterm trends spanning the 2014 boundary
- Classify: did they acknowledge the measurement change? Did they adjust? What was their reported trend magnitude vs. adjusted magnitude?
- Quantify systematic bias in the literature

### Part 4: Impact on preterm-specific mortality (V3 linked, 2014-2023)

**Table 6. Preterm-specific IMR by measurement method**
- Using the linked file for 2014-2023: compute IMR for preterm births defined by COMBGEST vs OEGEST_COMB
- OE-defined preterm IMR should be higher (smaller, sicker denominator)
- Quantify how much the measurement method affects mortality rate estimates

## Key deliverables

1. The bridging factor: a simple, reusable adjustment that other researchers can apply
2. The adjusted 35-year trend: the "true" preterm trajectory on a common measurement basis
3. Evidence that the published literature contains systematic measurement bias
4. Subgroup-specific artifact sizes (for disparities research)

## Data access

- Published-method rates: `/output/convenience/natality_v2_residents_only.parquet` (harmonized, uses OE when available)
- Dual-method comparison: `/output/yearly_clean/natality_{2014-2024}_core.parquet` (raw fields with both COMBGEST and OEGEST_COMB)
- Mortality impact: `/output/convenience/natality_v3_linked_residents_only.parquet`

## Target journals

- **Research letter**: AJOG or Obstetrics & Gynecology (the core finding in 600 words + 1 table)
- **Full paper** (with Parts 2-4): BMJ, Epidemiology, or Paediatric and Perinatal Epidemiology
- **Methods note**: American Journal of Epidemiology

## Estimated effort

- Part 1 (dual-method comparison): 1 session
- Part 2 (adjusted trend series): 1 session
- Part 3 (literature scoping): separate effort (manual)
- Part 4 (mortality impact): 1 session
- Write-up: 1-2 sessions

## Honest assessment

**Strengths**: The 2014 same-cohort comparison is irrefutable — it's a mechanical finding, not a statistical argument. The adjusted trend series would be a genuine public good. The `gestational_age_weeks_source` variable in our harmonization is unique infrastructure.

**Limitations**: NCHS already flagged the break (Martin 2015). The level difference is known (~1.5-2 ppt). Our contribution is the formal decomposition, the adjusted trend, and the literature bias quantification — synthesis novelty, not discovery novelty. Most likely a mid-tier publication unless the literature bias analysis reveals widespread errors in high-impact papers.
