# Version 1 candidate core variables (≤40)

Target domains from the project spec. Names below are **concept labels**; final `harmonized_name` values go in `harmonized_schema.csv` after benchmark-year review.

## Maternal demographics

- Maternal age (years)
- Maternal race (bridged / single race per NCHS coding)
- Hispanic origin
- Maternal education
- Marital status
- Nativity (mother born outside U.S.) — *feasibility TBD*

## Pregnancy / obstetric

- Parity or live birth order
- Plurality
- Month prenatal care began (or trimester)
- Number of prenatal visits
- Prepregnancy / gestational diabetes — *separate if documentation allows*
- Hypertension (chronic / pregnancy-associated) — *as coded in file*
- Tobacco use during pregnancy — *feasibility TBD*

## Infant / delivery

- Infant sex
- Gestational age (best available field per year)
- Birthweight (grams)
- Five-minute Apgar — *if feasible*
- Mode of delivery — *if feasible*

## Derived (in `04_derive/`)

- Low birthweight (&lt;2500 g)
- Very low birthweight (&lt;1500 g)
- Preterm (&lt;37 weeks completed)
- Very preterm (&lt;32 weeks)
- Singleton vs multiple birth
- Maternal age categories
- Education categories

*Drop or mark “partial comparability” any field that is inconsistent across 2005–2015 after inspecting 2005, 2010, and 2015.*
