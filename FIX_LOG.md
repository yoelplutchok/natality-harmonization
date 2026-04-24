# Fix Log — 2026-04-22 audit remediation

Applied in response to the two independent audits captured in `AUDIT_PROMPT_V3.md` / `AUDIT_REPORT_V3.md` (existing report) and this session's parallel audit. Every finding that the two audits agreed on, plus findings unique to either, is addressed below. Each entry cross-references the audit finding ID(s).

---

## Critical

### FIX-1 `MRACE6` byte position (Audit C1 / byte position)

- **File**: `scripts/01_import/field_specs.py:214`
- **Before**: `("MRACE6", 107, 108)` — 2-byte slice that concatenated `MRACE6` (byte 107, 1 byte, values 1–6) with the first byte of `MRACE15` (byte 108). Empirically produced codes `'10' '20' '30' '40' '41' '51' '61'`.
- **After**: `("MRACE6", 107, 107)` plus a new explicit `("MRACE15", 108, 109)` entry so neither adjacent field is silently mis-read.
- **Source**: `raw_docs/UserGuide2015.pdf` p.9, `UserGuide2020.pdf` p.13, `UserGuide2021.pdf` p.13 — MRACE6 listed at position 107 length 1; MRACE15 at positions 108–109 length 2.
- **Empirical verification post-fix** (2014 yearly parquet): `MRACE6` distribution is now `{'1': 2,892,897; '2': 589,658; '4': 243,342; ' ': 131,542; '6': 89,896; '3': 39,060; '5': 11,780}` — true 1-byte values 1–6 plus blank.

### FIX-2 Kleene null-safe race_eth (Audit C1(b) / C2)

- **Files**:
  - `scripts/03_harmonize/harmonize_v1_core.py` (both 1990–2002 and 2003+ branches)
  - `scripts/03_harmonize/harmonize_linked_v3.py`
- **Before**: compound conditions like
  ```python
  race_eth = pc.if_else(pc.and_(is_nh, pc.equal(race_bridged, 1)), "NH_white", race_eth)
  ```
  were using `pc.and_`, which in pyarrow is NOT Kleene-aware: `pc.and_(False, null) == null` and `pc.and_(True, null) == null`. When `race_bridged` was entirely null (2020+, MBRACE filler), every subsequent `pc.if_else(null_cond, new, previous)` overwrote the already-assigned `"Hispanic"` label with null.
- **After**: introduced a helper `_safe_cond(*parts)` that applies `pc.fill_null(p, False)` to every component before `pc.and_`. The Hispanic-branch condition is also `fill_null`-wrapped. Applied in all three code blocks.
- **Empirical verification**: to be re-validated after harmonized regen — target is ~0 null `maternal_race_ethnicity_5` rows among Hispanic records and ~3% null (multiracial) among non-Hispanic for 2020–2024.

---

## Major

### FIX-3 `delivery_method_recode` 2003–2004 sentinel remap (Audit M1 / M2)

- **File**: `scripts/03_harmonize/harmonize_v1_core.py:982–988`
- **Before**: 2003–2004 DMETH_REC values passed through unchanged, leaking codes 5 (~20 K/year, "not stated"), 6, and 7 (tiny counts) into `delivery_method_recode`. This was inconsistent with the 1990–2002 remap of 5→9 (which happens one era earlier) and with the 2005+ codes {1, 2, 9}.
- **After**: for `year in (2003, 2004)`, any `dmeth >= 5` is remapped to `9`. This unifies the "not stated" sentinel across all eras.
- **Doc**: `docs/COMPARABILITY.md` delivery-method section rewritten to state the remap explicitly.

### FIX-4 `record_weight` = 1.0 for 2016–2023 survivors (Audit M3)

- **File**: `scripts/01_import/parse_linked_cohort_year.py:177–189`
- **Before**: survivor rows in the period-cohort era (2016–2023) had `RECWT` populated with 8 blank bytes → parsed as null → 100 % null `record_weight` for survivors in those years.
- **After**: `blank_death["RECWT"] = "1.000000"` padded to 8 bytes. This mirrors NCHS's denominator-plus convention (2005–2015) where survivor RECWT is `1.000000` and matches the audit-prompt §5 invariant "every survivor should have weight 1.0 (exactly)".
- **Requires re-parsing**: 2016–2023 linked cohort parquets (pipeline rerun).

### FIX-5 Invariant-check null-propagation sweep (Audit M1)

- **File**: `scripts/05_validate/validate_v1_invariants.py`
- **Problem**: every check of the form `pc.and_(A, pc.invert(pc.equal(X, scalar)))` silently counted 0 violations whenever `X` was null, because `pc.equal(null, v) = null`, `pc.invert(null) = null`, `pc.and_(A, null) = null`, and `_count_true(null) = 0`. This is the mechanism that made the 2020+ 100 %-null race_eth bug invisible to the self-check (which reported PASS).
- **After**:
  - Added helpers `_safe_and(*parts)` (null-safe conjunction) and `_ne(a, b)` (null-safe inequality that treats null as a distinct value) at the top of the module.
  - Rewrote every vulnerable invariant: `smoke_int_0_not_false`, `smoke_int_1_5_not_true`, `smoke_int_6_not_null_any`, `smoke_any_true_bad_int`, `smoke_any_false_bad_int`, `hisp_origin_0_not_false`, `hisp_origin_1_5_not_true`, `race_eth_hisp_not_hispanic`, `race_eth_nh_bad_mapping`, `gest_src_pre2014_obstetric`, `ga99_clean_not_null`, `bw9999_clean_not_null`, `lbw_logic_mismatch`, `preterm_logic_mismatch`, `singleton_logic_mismatch`.
  - Added two new non-null-when-expected invariants: `race_eth_null_when_hisp_true` and `race_eth_null_when_hisp_false_and_race_detail_valid`. These would have caught C1/C2 on their own.

---

## Minor

### FIX-6 `docs/COMPARABILITY.md` MAGER41 description (Audit m4)

- **Before**: "Ages <15 map to 14; ages 50-54 map to 50; ages 55+ map to 55."
- **After**: Documents the actual code path — `code + 13` gives distinct ages 50, 51, 52, 53, 54 for MAGER41 codes 37–41; MAGER41 99 or >41 maps to null; the recode does not expose any ages ≥ 55.

### FIX-7 `docs/COMPARABILITY.md` `prior_cesarean` coverage (Audit m1 from my audit)

- **Before**: "1990–2002 uses `DCSEZD`; 2003+ uses `RF_CESAR`/`URF_CESAR`" (doc implied 1990–2013 were populated; the code set them null).
- **After**: Documents that `prior_cesarean` is null for 1990–2013 and populated only from `RF_CESAR` (2014–2024).

### FIX-8 `docs/COMPARABILITY.md` new section: `certificate_revision` values (Audit m4 from my audit)

- Added an explicit section at the top of the "Known structural breaks / constraints" area listing the four possible values (`"unrevised_1989"`, `"revised_2003"`, `"unknown"`, null), empirical counts of the `"unknown"` bucket for 2009 and 2013, and a rule for filtering.

### FIX-9 `docs/COMPARABILITY.md` record_weight availability (Audit m6 from my audit)

- **Before**: "available for all years".
- **After**: Clarified that for 2016–2023 the pipeline fills `1.0` for survivors (so the column is now genuinely populated for every row). Not a doc-only fix — depends on FIX-4.

### FIX-10 Stale counts in `docs/LLM_HANDOFF_LOG.md` (Audit m1)

- Line 128: `"67 harmonized + 10 derived = 77 columns"` → `"69 harmonized + 13 derived = 82 columns"`.
- Line 134: `"IMR trend 6.75→5.35 (2005-2020)"` → `"IMR trend 6.74→5.49 (2005-2023)"`.
- Line 137: `"74 harmonized + 13 derived = 87 columns"` → `"76 harmonized + 16 derived = 92 columns"`.
- Line 129: invariant count updated `33 checks → 37 checks` to reflect the 2 new + 2 renamed invariants.

### FIX-11 Stale IMR endpoint in `docs/GETTING_STARTED.md` (Audit m2)

- Line 73 example comment updated to `"IMR from 6.74 (2005) to 5.49 (2023)"`.

---

## Not applied (knowingly deferred)

- **Audit m7 (provenance SHA staleness)**: residents-only parquets stamped with git hash `087915f` while HEAD is `4016cc8`. Will naturally resolve when the residents-only files are regenerated at the end of this fix pass.
- **Audit N2 (`father_age` range 9–98 allows age 9)**: no empirical bad records. Defensive-programming gap, not a data bug. Leaving until a real case surfaces.
- **Audit N6 (`_cause_group()` 2-char P-code edge case)**: no such malformed codes exist in the data. Leaving as defensive-programming note.

---

## Data regeneration scope

Because FIX-1 changes a raw byte slice, every yearly parquet from 2014+ must be re-parsed. FIX-4 requires re-parsing the 2016–2023 linked cohort files. FIX-2, FIX-3, FIX-5 require re-running harmonize → derive → convenience → validate. Regeneration order:

1. `scripts/01_import/parse_all_v1_years.py --years 2014-2024` (natality yearly, ~45 min)
2. `scripts/01_import/parse_all_linked_years.py --years 2014-2015` (denominator-plus linked, 2014–2015 only — 2005–2013 unaffected by FIX-1)
3. `scripts/01_import/parse_linked_cohort_year.py` for 2016–2023 (period-cohort linked — both FIX-1 and FIX-4)
4. `scripts/03_harmonize/harmonize_v1_core.py --years 1990-2024` (captures FIX-2 and FIX-3)
5. `scripts/03_harmonize/harmonize_linked_v3.py --years 2005-2023` (captures FIX-2)
6. `scripts/04_derive/derive_v1_core.py`
7. `scripts/04_derive/derive_linked_v3.py`
8. `scripts/06_convenience/write_residents_only.py` (V2 and V3)
9. `scripts/05_validate/validate_v1_invariants.py` (captures FIX-5 + new invariants)
10. `scripts/05_validate/compare_external_targets_v1.py`, `compare_external_targets_v3_linked.py`
11. `scripts/05_validate/harmonized_missingness.py`
12. `scripts/05_validate/key_rates_from_derived_core.py`

Post-regen expectations:
- `maternal_race_ethnicity_5` null rate in 2020–2024 drops from 100 % to ~3 % (multiracial only).
- `maternal_race_ethnicity_5` null rate in 1990–2002 drops by ~500–1,000 rows/year (Kleene-collapse fix for Hispanic mothers whose MRACE detail mapped to null).
- `maternal_race_detail` values for 2014+ change from 2-digit concatenated codes to 1-digit MRACE6 codes (1–6).
- `delivery_method_recode` 2003–2004 codes 5/6/7 collapse into 9.
- `record_weight` null rate for 2016–2023 survivors drops from 100 % to 0 %.
- All invariant counts re-computed; several that silently reported 0 under the old validator may legitimately report higher numbers (this is the fix working, not new bugs).

---

## Post-regen verification (ran 2026-04-22)

Pipeline completed cleanly (all 11 stages). Measured outcomes:

**Anchor numbers unchanged** (row/column counts match pre-fix):
- V2 harmonized: 138,819,655 rows × 69 cols
- V2 derived: 138,819,655 × 82
- V3 linked harmonized: 74,943,824 × 76
- V3 linked derived: 74,943,824 × 92
- V2 residents-only: 138,582,904 × 80
- V3 linked residents-only: 74,785,708 × 90

**FIX-1 / FIX-2 (race_ethnicity_5)**: 2019 null rate = 0.96 % (baseline unchanged); 2020 = 3.27 %; 2021 = 3.37 %; 2023 = 3.39 %; 2024 = 3.50 %. All non-null rates populate Hispanic + NH_white + NH_black + NH_asian_pi + NH_aian correctly for every year. Matches the documented ~3 % multiracial residual exactly.

**FIX-3 (delivery_method_recode 2003–2004)**: distributions are now `{1, 2, 3, 4, 9}` only — codes 5, 6, 7 fully folded into 9.
- 2003: {1: 2,902,529; 2: 51,646; 3: 685,237; 4: 435,146; 9: 21,534}
- 2004: {1: 2,862,529; 2: 45,907; 3: 741,276; 4: 450,412; 9: 18,783}
- 2005 (control): {1: 2,879,290; 2: 1,250,676; 9: 15,653}

**FIX-4 (record_weight 2020 cohort survivors)**: 3,600,450 survivor rows, all have `record_weight = 1.0` exactly. Zero nulls. Period-cohort-era survivors (2016–2023) are now indistinguishable from denominator-plus-era survivors (2005–2015) in this column.

**FIX-5 (invariant-check null-propagation)**: invariant report now shows **PASS** on all 36 checks, including the 2 newly added non-null-when-expected invariants (`race_eth_null_when_hisp_true` = 0; `race_eth_null_when_hisp_false_and_race_detail_valid` = 0 with era-aware bridge rule that correctly excludes MRACE6=6 multiracial for 2020+ and 2-digit codes 09+ for 1990–2002). The same invariant, run against the pre-fix parquet, would have flagged ~18 M rows for the 2020+ race bug — so the original `race_eth_hisp_not_hispanic: 0` PASS was confirmed to be a false negative.

**External validation**: 183/183 V1 targets pass, 35/35 V3 linked targets pass (both match pre-fix counts). LBW, preterm, cesarean, twin, IMR, neonatal/postneonatal rates all still within tolerance.

**Residents-only provenance**: re-stamped with current git hash at build time. (Git SHA will reflect whatever HEAD is when the user commits these fixes.)

No new defects detected. Pipeline is clean.

---

## Post-V4-audit patches (ran 2026-04-22, same day as original fix pass)

The V4 re-audit (`AUDIT_REPORT_V4.md`) independently verified every fix. It found no data regressions and no fix-introduced bugs, but surfaced four loose ends: one doc drift from the original fix pass (**N1**), two latent invariant gaps that the V3 audit also missed (**G1**, **G2**), and one pre-existing NCHS source quirk (**M1**). All four are addressed here.

### FIX-12 N1: `docs/LLM_HANDOFF_LOG.md:129` invariant count off by one

- **Before** (my FIX-10 edit): `"invariants (37 checks, 0 violations)"`.
- **Correct now**: `"invariants (38 checks, 0 violations)"` — the actual count is 36 pre-patch + 2 new post-V4 invariants (G1 + the V4-suggested survivor-weight check) = 38. FIX-10 originally wrote 37, which was off whether or not the post-V4 invariants were added.

### FIX-13 G1: new invariant `delivery_method_recode_invalid_value`

- **File**: `scripts/05_validate/validate_v1_invariants.py` — added counter key in the `violations` dict and a per-batch check just before the null-rate discontinuity pass.
- **Check**: `delivery_method_recode ∈ {1, 2, 3, 4, 9}` when populated; null passes (null is a legitimate missing value). Uses `_safe_and` + `fill_null(False)` so null dmeth is not silently counted as a violation and any other integer (0, 5, 6, 7, 99, etc.) IS counted.
- **Adversarial test**: on a synthetic array `[1, 2, 3, 4, 9, 5, 6, 7, None, 0, 99]`, the invariant correctly counts **5** (the 5/6/7/0/99 rows) and passes through the null and the five allowed values. The class of regression the V4 auditor injected (500 rows `dmr=5` in 2020) would now be caught.
- **Post-V4 V2 run**: count = 0. ✓

### FIX-14 G2: `cert_rev_invalid_value` made null-safe (defense-in-depth)

- **File**: `scripts/05_validate/validate_v1_invariants.py:324–332`
- **Before**: raw `pc.invert(pc.or_(pc.equal(cert_rev, …)))` — if cert_rev were ever null, `pc.equal(null, x) = null`, `pc.or_(null, null) = null`, `pc.invert(null) = null`, and `_count_true(null) = 0` → silent null-FN.
- **After**: `pc.invert(pc.fill_null(is_valid_cert_raw, False))` — null cert_rev is now counted as a violation.
- **Post-V4 V2 run**: count = 0 (cert_rev currently has no nulls in the harmonized output, but the check is now defense-in-depth).

### FIX-15 New invariant `record_weight_null_when_survivor`

- **File**: `scripts/05_validate/validate_v1_invariants.py`
- **Check**: for rows where `infant_death == False`, `record_weight` must not be null. V3-linked-only; silently skipped when both columns aren't present (V2 natality).
- **Post-V4 V3 run**: count = **2** — surfaces the M1 finding exactly (1 survivor row each in 2014 and 2015 denom-plus source data). FIX-15 therefore passes as a *discovery* invariant on V3 while remaining at 0 on V2. If the M1 rows are later patched (see FIX-16), FIX-15 goes to 0 on V3 too.

### FIX-16 Document M1 in `docs/COMPARABILITY.md`

- **File**: `docs/COMPARABILITY.md` (V3 `record_weight` subsection).
- Added a "Known minor quirk" paragraph noting that 2 survivor rows (1 in 2014 year=2014, age=28, bw=2800 g, M; 1 in 2015 age=21, bw=2840 g, F) have null `record_weight` from the upstream NCHS denominator-plus files. These are NOT pipeline-introduced (parse_linked_year.py did not modify survivor RECWT for 2005–2015, and the upstream bytes are literally blank). Analytic guidance: `record_weight.fill_null(1.0)` or drop those 2 rows explicitly.

### Post-V4 verification

Ran `validate_v1_invariants.py` against both the V2 derived parquet and the V3 linked derived parquet:

- **V2 (1990–2024)**: 38 invariants, **PASS** (all 0). The two new invariants (FIX-13 + FIX-15) both report 0 (no delivery-method-value regressions; V2 has no record_weight column so FIX-15 is silently skipped).
- **V3 linked (2005–2023)**: FIX-15 surfaces exactly 2 (the M1 rows); the V1 script running on V3 also surfaces V2-specific checks (`unrevised_2009_2013_has_educ` etc.) that don't apply to the linked file's field layout — these are V1-on-V3 cross-validation noise, not pipeline bugs. The intended canonical run of `validate_v1_invariants.py` is against `natality_v2_harmonized_derived.parquet`; a V3-dedicated invariants script would be a future-work follow-up.

### Files touched in this patch

- `scripts/05_validate/validate_v1_invariants.py` — added G1, fixed G2, added FIX-15; violations dict updated.
- `docs/LLM_HANDOFF_LOG.md:129` — invariant count corrected to 38.
- `docs/COMPARABILITY.md` — added M1 note under V3 `record_weight`.
- `output/validation/invariants_report_1990_2024.md` + `.csv` — regenerated.

No parquet files needed regeneration (validator-only and doc-only changes).

---

# Fix Log — 2026-04-23 audit v5 remediation

Applied in response to the `AUDIT_REPORT_FRESH_5.md` findings. Every one of the 6 findings is addressed below, each with a reproducer that shows the fix landing.

## Material

### V5-FIX-1 Committed the uncommitted working tree so PROVENANCE hash reproduces the shipped parquets (Audit v5 Finding 1)

- **What was wrong**: PROVENANCE.md listed `Pipeline git hash: 4016cc8`, which was HEAD, but HEAD was 680 lines behind the working tree that actually built the Apr 23 parquets. A third party checking out that hash would not reproduce the published SHA-256s.
- **What was done**:
  1. Committed every pipeline/validation/doc edit that was live when the 2026-04-23 harmonized + convenience parquets were built. After commit, HEAD contains the code that produced those parquets' **content**. Commit hash: `098f958`.
  2. Re-ran `scripts/06_convenience/write_residents_only.py` so the embedded `pipeline_git_hash` metadata inside the convenience parquets reads `098f958` (previously `4016cc8`, which pre-dated the commit). Row counts unchanged (138,582,904 V2; 74,785,708 V3 linked); filter and schema unchanged; only the embedded metadata timestamp + git hash differ from the 2026-04-23 build.
  3. Regenerated `output/convenience/PROVENANCE.md` with the new SHA-256s:
     - `natality_v2_residents_only.parquet`: `0f0dc844c4b0f9ec03ca302f71d6f1f1b4b05ffaf0b42ad108346217c5cdb0ae`
     - `natality_v3_linked_residents_only.parquet`: `ef2eb1a2eb678c9273dbbfa4b7f830c96054a3fea56cc835443c274d7eb0c2e1`
     The old SHA-256s (`13c4fdff…` and `8bd5b019…`) are preserved in PROVENANCE.md's "Previous build" block for anyone verifying against the existing Zenodo 10.5281/zenodo.19363075 download.
- **Zenodo action needed (user)**: the new SHA-256s apply to a freshly reproduced copy; the SHA-256s on the current Zenodo deposit reflect the superseded build. Publish a new Zenodo version with the rebuilt parquets + new PROVENANCE.md if you want downloaders to bit-verify against HEAD. The content is equivalent either way.
- **Verify**:
  ```bash
  $ git log -1 --format='%H' HEAD
  098f958…
  $ grep 'Pipeline git hash' output/convenience/PROVENANCE.md
  - **Pipeline git hash**: `098f958`
  $ python -c "
  import pyarrow.parquet as pq
  md = pq.read_metadata('output/convenience/natality_v2_residents_only.parquet').metadata
  print(md[b'pipeline_git_hash'].decode())"
  098f958
  $ shasum -a 256 output/convenience/*.parquet   # matches PROVENANCE.md
  ```

### V5-FIX-2 V3-aware invariant validator + documented V2/V3 2009–2010 divergence (Audit v5 Finding 2)

- **What was wrong**: `validate_v1_invariants.py` defines 41 invariants. It had only ever been run on V2 natality. When I ran it on the V3 linked parquet it surfaced 4 non-zero counts. One (`record_weight_null_when_survivor = 2`) was already documented as an upstream NCHS quirk, but the report still said FAIL. The other three (`unrevised_2009_2013_has_educ/pnmonth/smokeint`, ~2 M each) revealed a **real, previously undocumented source-file divergence**: the linked denominator-plus layout for 2005–2013 retains `MEDUC_REC` / `MPCB` / `CIG_1-3` bytes at positions that the natality 2009–2010 public-use layout drops, so V3 populates those three columns on ~2 M unrevised-cert 2009/2010 rows where V2 leaves them null.
- **What was done**:
  1. `scripts/05_validate/validate_v1_invariants.py`: auto-detect V3 linked via presence of `infant_death` column; when detected, skip the 3 V2-only coverage invariants (report them with a `_(skipped — V2-only, …)_` annotation rather than failing the run) and allow `record_weight_null_when_survivor ≤ 2` as a known exception.
  2. `docs/COMPARABILITY.md`: added §"V3 linked vs V2 natality: 2009–2010 unrevised-cert field retention" with per-column non-null counts and analyst guidance.
  3. `docs/CODEBOOK.md`: annotated the `maternal_education_cat4`, `prenatal_care_start_month`, and `smoking_intensity_max_recode6` rows with a "V3 linked exception" pointer to the new COMPARABILITY section.
  4. Committed a V3 invariants report to `output/validation/invariants_report_2005_2023.md` (mode-labeled "V3 linked"; PASS status with the documented carve-outs).
- **Why skip in V3 rather than null in V3**: the data in the linked denominator-plus file is real upstream NCHS content. Nulling it out to match V2 would destroy usable information. Researchers who need education/prenatal/smoking coverage on 2009–2010 unrevised rows should use V3 linked for those analyses, per the new COMPARABILITY note.
- **Verify**:
  ```bash
  $ python scripts/05_validate/validate_v1_invariants.py \
      --in output/harmonized/natality_v3_linked_harmonized_derived.parquet \
      --years 2005-2023
  # → output/validation/invariants_report_2005_2023.md with "Mode: V3 linked" and PASS
  $ python scripts/05_validate/validate_v1_invariants.py \
      --in output/harmonized/natality_v2_harmonized_derived.parquet \
      --years 1990-2024
  # → output/validation/invariants_report_1990_2024.md with "Mode: V2 natality" and PASS
  ```

## Cosmetic

### V5-FIX-3 Corrected invariant count `38 → 41` in three docs (Audit v5 Finding 3)

- **Files**: `README.md` line 27; `docs/VALIDATION.md` line 137; `docs/PROJECT_EXPLAINER.md` lines 242 and (indirectly) 458 and 464.
- **Reproducer of the bug**:
  ```bash
  $ grep -oE 'violations\["[^"]+"\]' scripts/05_validate/validate_v1_invariants.py | sort -u | wc -l
  41
  $ grep -E '^- `' output/validation/invariants_report_1990_2024.md | wc -l
  41
  ```
  The docs previously said "38". They now say "41", with an inline footnote for V3 explaining the 3 skipped invariants and the 2-row `record_weight` exception.

### V5-FIX-4 Corrected 2010 infant-sex byte position (Audit v5 Finding 4)

- **File**: `docs/PROJECT_EXPLAINER.md` line 133.
- **Before**: "For 2010 (775-byte records): … infant sex is character 475" — wrong; 475 is the 2014+ position.
- **After**: "… infant sex is character 436" — matches the schema CSV and the raw-zip probe (byte 436 contains M/F in the 2010 file; byte 475 is blank).
- **Verify**:
  ```bash
  $ 7z x -so raw_data/Nat2010us.zip 2>/dev/null | head -c 2000 | \
      python3 -c "import sys; line=sys.stdin.buffer.readline(); print(repr(line[435:436]), repr(line[474:475]))"
  b'M' b' '
  ```

### V5-FIX-5 Corrected 1995 cesarean quoted rate `20.80% → 20.84%` (Audit v5 Finding 5)

- **File**: `docs/PROJECT_EXPLAINER.md` line 454 (the cesarean rate claim).
- **Before**: "Our data produces 20.80%. Match within tolerance."
- **After**: "Our data produces 20.84% (= 806,722 / 3,870,446 cesareans among known delivery method; rounds to the published 20.8%). Match within tolerance."
- **Verify**:
  ```python
  # In the residents-only convenience parquet:
  # 1995 cesarean = DMR in {3,4} / (DMR in {1,2,3,4}) = 806722 / 3870446 = 20.8431%
  ```

### V5-FIX-6 Corrected FAGECOMB first-populated year `2013 → 2011` (Audit v5 Finding 6)

- **File**: `docs/VALIDATION.md` line 162.
- **Before**: "2013 `FAGECOMB` and `RF_CESAR`: both first populated in 2013 (not 2014 …)"
- **After**: "`FAGECOMB` lives at bytes 182–183; the harmonizer first uses it for 2012 (NCHS moved father age there in the 2012 file; ~61% populated in 2012 and 2013). `RF_CESAR` lives at byte 324 and is first populated in 2013."
- **Verify**:
  ```bash
  $ python3 -c "
  import pyarrow.parquet as pq, pyarrow.compute as pc, pyarrow as pa
  for y in [2011, 2012, 2013]:
      pf = pq.ParquetFile(f'output/yearly_clean/natality_{y}_core.parquet')
      for b in pf.iter_batches(batch_size=500_000, columns=['FAGECOMB']):
          col = b.column(0)
          blank = int(pc.sum(pc.cast(pc.equal(pc.utf8_trim_whitespace(col), ''), pa.int64())).as_py() or 0)
          print(f'{y} FAGECOMB blank {blank/len(col)*100:.1f}%')
          break
  "
  2011 FAGECOMB blank 38.6%
  2012 FAGECOMB blank 38.5%
  2013 FAGECOMB blank 36.0%
  ```

### Files touched in this patch

- `scripts/05_validate/validate_v1_invariants.py` — added V3-auto-detect, V3_SKIP set, KNOWN_EXCEPTIONS dict, mode label in report
- `README.md` — invariant count 38→41 + V3 caveat pointer
- `docs/VALIDATION.md` — invariant count 38→41; FAGECOMB correction
- `docs/PROJECT_EXPLAINER.md` — invariant count 38→41; 2010 sex byte 475→436; 1995 cesarean 20.80%→20.84%
- `docs/COMPARABILITY.md` — new §"V3 linked vs V2 natality: 2009–2010 unrevised-cert field retention"
- `docs/CODEBOOK.md` — V3-exception pointer on three affected columns
- `output/validation/invariants_report_1990_2024.md` + `.csv` — regenerated with mode label
- `output/validation/invariants_report_2005_2023.md` + `.csv` — new V3 linked report

No parquet files were regenerated. Data is unchanged; only docs, validator code, and validation reports changed.

---

# Fix Log — 2026-04-24 audit v5.1 remediation

Applied in response to a follow-up external audit ("fresh-external-audit-v5.canvas.tsx") that surfaced two real bugs the prior round missed — one pre-existing docs drift, one regression I introduced in V5-FIX-6. Both are docs-only.

## Material

### V5.1-FIX-1 `prior_cesarean_count` years_available was stale (schema said 2014–2024; shipped data is 2005–2024)

- **What was wrong**: `metadata/harmonized_schema.csv` listed `prior_cesarean_count` as `years_available=2014-2024`, `raw_source_by_year="2014-2024: RF_CESARN@332-333"`, `comparability_class=within-era`, note "Not available before 2014." In reality `scripts/01_import/field_specs.py` reads `RF_CESARN@325-326` in the 2005–2013 era and the shipped parquet carries real data for those years (1.27 M rows populated in 2005, 3.48 M in 2012, 3.55 M in 2013 — matching `prior_cesarean` coverage exactly). The `prior_cesarean` (bool) schema entry was already correct at "2005-2024"; only the `_count` entry, CODEBOOK, COMPARABILITY, FAQ, PROJECT_EXPLAINER, and ABOUT_SOURCE_DATA were stale. Consequence: a researcher filtering `year >= 2014` based on the schema would silently drop ~15 M valid pre-2014 observations.
- **What was done**: updated the 6 stale doc locations to state 2005–2024 with revised-cert-only coverage (30.7% of 2005 rows ramping to 90.2% in 2013 and ~96–100% from 2014+). Also fixed a sibling error in `docs/COMPARABILITY.md` §"Prior cesarean" that had claimed `prior_cesarean` (bool) was "null for 1990–2013" when it's actually null only for 1990–2004. Specific files:
  - `metadata/harmonized_schema.csv` — `prior_cesarean_count` row: `years_available` 2014-2024 → 2005-2024; `raw_source_by_year` now lists both era positions; `comparability_class` within-era → partial; note rewritten to mirror `prior_cesarean`.
  - `docs/CODEBOOK.md` — same updates in the `prior_cesarean_count` row.
  - `docs/COMPARABILITY.md` — the `- **Prior cesarean**` subsection rewritten to cover both bool and count, with corrected availability (null 1990–2004, ramping 2005–2013, ~96–100% 2014+); the stray `- prior_cesarean_count (2014–2024 only; ...)` bullet removed from the "Within-era only" list.
  - `docs/FAQ.md` — `prior_cesarean_count` removed from the 2014+-only list; a one-line note added to the existing "Why is `prior_cesarean` null…" Q&A stating the `_count` column has identical coverage.
  - `docs/PROJECT_EXPLAINER.md` — `prior cesarean count` removed from the "2014-2024 only" bullet; a new "2005-2024 with revised-certificate coverage ramp-up" bullet added that covers both `prior_cesarean` and `prior_cesarean_count`.
  - `docs/ABOUT_SOURCE_DATA.md` — "(and prior cesarean count for 2014+)" → accurate era-ramp description.
- **Verify**:
  ```bash
  $ python3 -c "
  import pyarrow.parquet as pq, pyarrow.compute as pc, pyarrow as pa
  pf = pq.ParquetFile('output/harmonized/natality_v2_harmonized_derived.parquet')
  counts = {}
  for b in pf.iter_batches(batch_size=500_000, columns=['year','prior_cesarean_count']):
      ya, pcc = b.column(0), b.column(1)
      for y in [2005, 2012, 2013, 2014]:
          m = pc.equal(ya, y)
          t = int(pc.sum(pc.cast(m, pa.int64())).as_py() or 0)
          n = int(pc.sum(pc.cast(pc.fill_null(pc.and_(m, pc.is_valid(pcc)), False), pa.int64())).as_py() or 0)
          counts[y] = (counts.get(y, (0,0))[0] + t, counts.get(y, (0,0))[1] + n)
  for y, (t, n) in sorted(counts.items()):
      print(f'{y}: total={t:,} populated={n:,} ({n/t*100:.2f}%)')
  "
  2005: total=4,145,619 populated=1,272,269 (30.69%)
  2012: total=3,960,796 populated=3,482,581 (87.93%)
  2013: total=3,940,764 populated=3,552,706 (90.15%)
  2014: total=3,998,175 populated=3,845,909 (96.19%)
  ```

### V5.1-FIX-2 VALIDATION.md FAGECOMB/RF_CESAR description was wrong (V5-FIX-6 regression)

- **What was wrong**: the V5-FIX-6 edit to `docs/VALIDATION.md` line 162 said "FAGECOMB … ~61% populated in 2012 and 2013" and "RF_CESAR … first populated in 2013." Both numbers are wrong:
  - Raw `FAGECOMB` is **88.26% nonblank** in 2012 and **90.45% in 2013** (exactly matching revised-cert share in each year) — the "~61%" came from a 500 k-row first-batch sample, not the full file. After the harmonizer maps sentinel 99→null, the resulting `father_age` column is populated on ~77% (2012) and ~79% (2013) of rows — that's the number that belongs in a user-facing doc, and it's already correctly stated in `docs/FAQ.md` line 106.
  - `RF_CESAR` is **first populated in 2005** (30.8% of rows), ramping with cert adoption to 90.2% by 2013 — exactly as already documented correctly in `docs/FAQ.md` line 112. The "first populated in 2013" claim in VALIDATION.md contradicted FAQ.md.
- **What was done**: rewrote `docs/VALIDATION.md` bullet so it: (a) gives the correct `FAGECOMB` raw-nonblank rates (88.26% / 90.45%) and the correct post-99→null `father_age` coverage (~77% / ~79%); (b) states `RF_CESAR` is first populated in 2005 with the cert-adoption ramp; (c) points the reader to the FAQ entry for the full year-by-year coverage table. Did NOT touch the corresponding `father_age` schema note ("~77-79%"), which was independently correct.
- **Verify**:
  ```bash
  $ python3 -c "
  import pyarrow.parquet as pq, pyarrow.compute as pc, pyarrow as pa
  for y in [2011, 2012, 2013]:
      pf = pq.ParquetFile(f'output/yearly_clean/natality_{y}_core.parquet')
      t = b = rfb = 0
      for batch in pf.iter_batches(batch_size=500_000, columns=['FAGECOMB','RF_CESAR']):
          fg, rf = batch.column(0), batch.column(1)
          t += len(fg)
          b += int(pc.sum(pc.cast(pc.equal(pc.utf8_trim_whitespace(fg), ''), pa.int64())).as_py() or 0)
          rfb += int(pc.sum(pc.cast(pc.equal(pc.utf8_trim_whitespace(rf), ''), pa.int64())).as_py() or 0)
      print(f'{y}: FAGECOMB nonblank={((t-b)/t)*100:.2f}%  RF_CESAR nonblank={((t-rfb)/t)*100:.2f}%')
  "
  2011: FAGECOMB nonblank=85.79%  RF_CESAR nonblank=85.79%
  2012: FAGECOMB nonblank=88.26%  RF_CESAR nonblank=88.26%
  2013: FAGECOMB nonblank=90.45%  RF_CESAR nonblank=90.45%
  ```

### Files touched in this patch

- `metadata/harmonized_schema.csv` — one-row update on `prior_cesarean_count`
- `docs/CODEBOOK.md` — one-row update on `prior_cesarean_count`
- `docs/COMPARABILITY.md` — `prior_cesarean` subsection rewrite + removal of `_count` bullet from within-era list
- `docs/FAQ.md` — one-line append in prior_cesarean Q&A + removed `_count` from 2014+-only list
- `docs/PROJECT_EXPLAINER.md` — moved `prior cesarean count` out of the 2014-2024 bullet into a new 2005–2024-with-ramp bullet
- `docs/ABOUT_SOURCE_DATA.md` — one-line update
- `docs/VALIDATION.md` — FAGECOMB/RF_CESAR bullet rewrite

No schema structural changes, no pipeline changes, no data changes, no SHA-256 changes. Parquets, embedded metadata, and PROVENANCE.md all unaffected.


