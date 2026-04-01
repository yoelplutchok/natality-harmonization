# Harmonized missingness report

Input: `/Users/yoelplutchok/Desktop/natality-harmonization/output/harmonized/natality_v2_harmonized_derived.parquet`
Break threshold: 5.0 percentage points

## Structural breaks detected (81 total)

| Variable | Year transition | Null % (from → to) | Delta (ppt) |
|----------|----------------|---------------------|-------------|
| `apgar5_clean` | 2004→2005 | 23.0% → 13.7% | -9.2 |
| `apgar5_clean` | 2006→2007 | 13.5% → 0.8% | -12.8 |
| `attendant_at_birth` | 2003→2004 | 0.2% → 100.0% | +99.8 |
| `attendant_at_birth` | 2004→2005 | 100.0% → 0.1% | -99.9 |
| `bmi_prepregnancy` | 2013→2014 | 100.0% → 7.2% | -92.8 |
| `bmi_prepregnancy_recode6` | 2013→2014 | 100.0% → 7.2% | -92.8 |
| `breastfed_at_discharge` | 2013→2014 | 100.0% → 18.2% | -81.8 |
| `ca_anencephaly` | 2013→2014 | 100.0% → 3.9% | -96.1 |
| `ca_cchd` | 2013→2014 | 100.0% → 3.9% | -96.1 |
| `ca_cdh` | 2013→2014 | 100.0% → 3.9% | -96.1 |
| `ca_chromosomal_disorder` | 2013→2014 | 100.0% → 3.9% | -96.1 |
| `ca_cleft_lip` | 2013→2014 | 100.0% → 3.9% | -96.1 |
| `ca_cleft_palate` | 2013→2014 | 100.0% → 3.9% | -96.1 |
| `ca_down_syndrome` | 2013→2014 | 100.0% → 3.9% | -96.1 |
| `ca_gastroschisis` | 2013→2014 | 100.0% → 3.9% | -96.1 |
| `ca_hypospadias` | 2013→2014 | 100.0% → 3.9% | -96.1 |
| `ca_limb_reduction` | 2013→2014 | 100.0% → 3.9% | -96.1 |
| `ca_omphalocele` | 2013→2014 | 100.0% → 3.9% | -96.1 |
| `ca_spina_bifida` | 2013→2014 | 100.0% → 3.9% | -96.1 |
| `father_age` | 2011→2012 | 13.1% → 100.0% | +86.9 |
| `father_age` | 2013→2014 | 100.0% → 15.7% | -84.3 |
| `father_age_cat` | 2011→2012 | 13.1% → 100.0% | +86.9 |
| `father_age_cat` | 2013→2014 | 100.0% → 15.7% | -84.3 |
| `father_education_cat4` | 1994→1995 | 17.5% → 100.0% | +82.5 |
| `father_education_cat4` | 2008→2009 | 100.0% → 42.3% | -57.7 |
| `father_education_cat4` | 2009→2010 | 42.3% → 34.5% | -7.7 |
| `father_education_cat4` | 2010→2011 | 34.5% → 27.6% | -7.0 |
| `father_education_cat4` | 2013→2014 | 23.1% → 18.0% | -5.1 |
| `gestational_diabetes` | 2013→2014 | 100.0% → 3.7% | -96.3 |
| `induction_of_labor` | 2013→2014 | 100.0% → 3.7% | -96.3 |
| `infection_chlamydia` | 2013→2014 | 100.0% → 4.0% | -96.0 |
| `infection_gonorrhea` | 2013→2014 | 100.0% → 4.0% | -96.0 |
| `infection_hep_b` | 2013→2014 | 100.0% → 4.0% | -96.0 |
| `infection_hep_c` | 2013→2014 | 100.0% → 4.0% | -96.0 |
| `infection_syphilis` | 2013→2014 | 100.0% → 4.0% | -96.0 |
| `marital_status` | 2016→2017 | 0.0% → 12.2% | +12.2 |
| `maternal_education_cat4` | 2008→2009 | 1.2% → 32.9% | +31.7 |
| `maternal_education_cat4` | 2009→2010 | 32.9% → 23.6% | -9.3 |
| `maternal_education_cat4` | 2010→2011 | 23.6% → 15.2% | -8.4 |
| `maternal_education_cat4` | 2013→2014 | 10.6% → 4.7% | -5.9 |
| `maternal_race_bridged4` | 2019→2020 | 0.0% → 100.0% | +100.0 |
| `maternal_race_detail` | 2002→2003 | 0.0% → 5.5% | +5.5 |
| `maternal_race_detail` | 2003→2004 | 5.5% → 18.4% | +12.9 |
| `maternal_race_detail` | 2004→2005 | 18.4% → 30.8% | +12.3 |
| `maternal_race_detail` | 2005→2006 | 30.8% → 48.7% | +17.9 |
| `maternal_race_detail` | 2006→2007 | 48.7% → 61.0% | +12.3 |
| `maternal_race_detail` | 2007→2008 | 61.0% → 68.4% | +7.5 |
| `maternal_race_detail` | 2009→2010 | 70.5% → 79.9% | +9.3 |
| `maternal_race_detail` | 2010→2011 | 79.9% → 86.9% | +7.1 |
| `maternal_race_detail` | 2013→2014 | 91.2% → 3.3% | -87.9 |
| `maternal_race_ethnicity_5` | 2019→2020 | 1.0% → 100.0% | +99.0 |
| `nicu_admission` | 2013→2014 | 100.0% → 3.8% | -96.2 |
| `payment_source_recode` | 2008→2009 | 100.0% → 33.3% | -66.7 |
| `payment_source_recode` | 2009→2010 | 33.3% → 24.3% | -9.0 |
| `payment_source_recode` | 2010→2011 | 24.3% → 15.7% | -8.6 |
| `payment_source_recode` | 2013→2014 | 10.7% → 4.5% | -6.2 |
| `pre_pregnancy_diabetes` | 2013→2014 | 100.0% → 3.7% | -96.3 |
| `prenatal_care_start_month` | 2008→2009 | 0.0% → 31.9% | +31.9 |
| `prenatal_care_start_month` | 2009→2010 | 31.9% → 22.6% | -9.3 |
| `prenatal_care_start_month` | 2010→2011 | 22.6% → 14.2% | -8.4 |
| `prenatal_care_start_month` | 2013→2014 | 9.6% → 3.6% | -6.0 |
| `prenatal_care_start_trimester` | 2008→2009 | 0.0% → 31.9% | +31.9 |
| `prenatal_care_start_trimester` | 2009→2010 | 31.9% → 22.6% | -9.3 |
| `prenatal_care_start_trimester` | 2010→2011 | 22.6% → 14.2% | -8.4 |
| `prenatal_care_start_trimester` | 2013→2014 | 9.6% → 3.6% | -6.0 |
| `prior_cesarean` | 2013→2014 | 100.0% → 3.7% | -96.3 |
| `prior_cesarean_count` | 2013→2014 | 100.0% → 3.8% | -96.2 |
| `smoking_any_during_pregnancy` | 1998→1999 | 20.1% → 14.5% | -5.6 |
| `smoking_any_during_pregnancy` | 2006→2007 | 19.6% → 6.8% | -12.7 |
| `smoking_any_during_pregnancy` | 2007→2008 | 6.8% → 12.4% | +5.6 |
| `smoking_any_during_pregnancy` | 2008→2009 | 12.4% → 44.0% | +31.6 |
| `smoking_any_during_pregnancy` | 2009→2010 | 44.0% → 34.6% | -9.4 |
| `smoking_any_during_pregnancy` | 2010→2011 | 34.6% → 20.8% | -13.8 |
| `smoking_any_during_pregnancy` | 2013→2014 | 13.9% → 5.5% | -8.5 |
| `smoking_intensity_max_recode6` | 2006→2007 | 5.6% → 0.0% | -5.6 |
| `smoking_intensity_max_recode6` | 2008→2009 | 0.0% → 31.9% | +31.9 |
| `smoking_intensity_max_recode6` | 2009→2010 | 31.9% → 22.6% | -9.3 |
| `smoking_intensity_max_recode6` | 2010→2011 | 22.6% → 14.2% | -8.4 |
| `smoking_intensity_max_recode6` | 2013→2014 | 9.6% → 3.6% | -6.0 |
| `smoking_pre_pregnancy_recode6` | 2013→2014 | 100.0% → 3.6% | -96.4 |
| `weight_gain_pounds` | 2013→2014 | 100.0% → 4.4% | -95.6 |

## Output files

- Full missingness by year: `/Users/yoelplutchok/Desktop/natality-harmonization/output/validation/harmonized_missingness_by_year.csv`
- Structural breaks: `/Users/yoelplutchok/Desktop/natality-harmonization/output/validation/harmonized_missingness_breaks.csv`

