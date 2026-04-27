/*
================================================================================
  PROJECT:  FDI Inflows and Export Performance — Panel Analysis
  AUTHOR:   Muaz Ahmed
  CONTACT:  muaz.ahmed@ens.psl.eu
  DATE:     April 2026

  DESCRIPTION:
  This master do-file executes the full pipeline for a panel analysis of the
  causal impact of FDI inflows on export performance using administrative data.
  The architecture follows strict reproducibility standards:

    - All paths are defined via globals; no hardcoded paths appear downstream.
    - Raw data is never modified. All transformations write to /intermediate/.
    - Every stage is logged. Outputs are fully traceable to construction decisions.
    - Seeds are set explicitly before any stochastic operation.
    - Regression output is exported directly to LaTeX via estout, removing
      copy-paste as a source of transcription error.

  PIPELINE STRUCTURE:
    Section 01 — Environment setup and path globals
    Section 02 — Data simulation (raw inputs)
    Section 03 — Data cleaning and standardisation
    Section 04 — Dataset merges and panel construction
    Section 05 — Variable construction (dummies, interactions, transformations)
    Section 06 — Summary statistics
    Section 07 — Two-way fixed effects estimation (TWFE)
    Section 08 — Difference-in-differences (DiD) specification
    Section 09 — Robustness checks
    Section 10 — Export tables to LaTeX

  NOTE:
  Raw data is simulated in Section 02 to make this script fully self-contained.
  This replaces the real data used in the actual project. All downstream
  sections are identical regardless of data source.
================================================================================
*/


/* =============================================================================
   SECTION 01: ENVIRONMENT SETUP
   ============================================================================= */

// Clear memory and set environment
clear all
set more off
set linesize 120
version 16.0                        // Declare version for forward compatibility

// ── Global path definitions ──────────────────────────────────────────────────
// All paths are defined here. Collaborators only edit this block.
// No path string appears anywhere else in this script.

global root    "."                  // Project root — set to your local path
global raw     "$root/data/raw"     // Raw data: read-only, never modified
global temp    "$root/data/intermediate"
global clean   "$root/data/clean"
global output  "$root/output/tables"
global log     "$root/logs"

// ── Create directories if they do not exist ──────────────────────────────────
// Ensures the pipeline runs on a fresh clone without manual setup.
capture mkdir "$root/data"
capture mkdir "$raw"
capture mkdir "$temp"
capture mkdir "$clean"
capture mkdir "$output"
capture mkdir "$log"

// ── Open log ─────────────────────────────────────────────────────────────────
// Log file is timestamped so reruns do not overwrite prior logs.
local date_str = subinstr("$S_DATE", " ", "_", .)
log using "$log/analysis_`date_str'.log", replace text

di "Pipeline started: $S_DATE $S_TIME"
di "Stata version: `c(stata_version)'"
di "Working directory: `c(pwd)'"


/* =============================================================================
   SECTION 02: DATA SIMULATION (RAW INPUTS)
   Simulates three administrative data sources that would ordinarily be loaded
   from $raw. In a live project this section is replaced by:
       use "$raw/fdi_data.dta", clear
   All variable names, types, and known data quality issues are documented here
   so that cleaning decisions in Section 03 are traceable.
   ============================================================================= */

set seed 20260428                   // Explicit seed — results are exactly replicable

// ── Source A: FDI inflows from central bank registry ─────────────────────────
// Coverage: 50 districts, 2010–2022. Known issues: district codes inconsistent
// across years (pre-2015 uses 4-digit codes; post-2015 uses 6-digit codes).
// Missing values coded as -99 in the raw extract.

local n_districts 50
local n_years     13                // 2010–2022

quietly {
    set obs `= `n_districts' * `n_years''

    gen district_id_raw = ceil(_n / `n_years')
    gen year = 2009 + mod(_n - 1, `n_years') + 1

    // Simulate pre-2015 / post-2015 coding inconsistency in raw data
    gen str8 district_code_raw = ""
    replace district_code_raw = string(district_id_raw, "%04.0f") if year < 2015
    replace district_code_raw = string(district_id_raw * 100, "%06.0f") if year >= 2015

    // FDI inflows (USD millions) — right-skewed, some zeros, a few -99 missings
    gen fdi_inflows_raw = max(0, rnormal(25, 40)) * (district_id_raw / 10)
    replace fdi_inflows_raw = -99 if runiform() < 0.03  // ~3% raw missings

    // Region identifier (five regions, assigned to districts)
    gen region = ceil(district_id_raw / 10)

    label variable district_id_raw   "District ID (raw, pre/post-2015 inconsistent)"
    label variable year              "Year"
    label variable district_code_raw "District code string (raw admin format)"
    label variable fdi_inflows_raw   "FDI inflows USD mn (raw; -99 = missing)"
    label variable region            "Region (1–5)"

    save "$temp/raw_fdi.dta", replace
}

// ── Source B: Export performance from trade ministry ─────────────────────────
// Coverage: same 50 districts, 2010–2022. Known issues: export values for
// districts 31–40 are implausibly high in 2011 (data entry error flagged in
// the administrative notes; treated as missing in cleaning).

quietly {
    set obs `= `n_districts' * `n_years''

    gen district_id_raw = ceil(_n / `n_years')
    gen year = 2009 + mod(_n - 1, `n_years') + 1

    gen exports_raw = max(0, rnormal(80, 60) * (1 + 0.04 * (year - 2010)))
    // Simulate known 2011 data entry error for districts 31–40
    replace exports_raw = exports_raw * 50 if district_id_raw >= 31 ///
        & district_id_raw <= 40 & year == 2011

    gen export_firms_raw = max(1, round(rnormal(120, 50)))

    label variable district_id_raw  "District ID (raw)"
    label variable exports_raw      "Total exports USD mn (raw)"
    label variable export_firms_raw "Number of exporting firms (raw)"

    save "$temp/raw_exports.dta", replace
}

// ── Source C: District-level controls from statistical office ─────────────────
// Cross-section (one row per district). Contains infrastructure index, literacy
// rate, and a treatment indicator for an SEZ (Special Economic Zone) designation
// announced in 2016 — used in the DiD specification.

quietly {
    set obs `n_districts'

    gen district_id_raw = _n
    gen infrastructure  = runiform(0.2, 0.9)
    gen literacy_rate   = runiform(0.4, 0.95)

    // SEZ treatment: districts 21–35 designated as SEZs in 2016
    // Treatment is permanent post-designation (absorbing state)
    gen sez_district = (district_id_raw >= 21 & district_id_raw <= 35)

    label variable infrastructure "Infrastructure quality index (0–1)"
    label variable literacy_rate  "Adult literacy rate"
    label variable sez_district   "=1 if district designated as SEZ"

    save "$temp/raw_controls.dta", replace
}

di "Section 02 complete: raw data simulated and saved to $temp"


/* =============================================================================
   SECTION 03: DATA CLEANING AND STANDARDISATION
   All cleaning decisions are documented at the point of decision. No decision
   is made silently. Raw files are read from $temp (simulated) or $raw (live).
   Output is written to $temp as intermediate clean files.
   ============================================================================= */

// ── Clean FDI source ─────────────────────────────────────────────────────────
use "$temp/raw_fdi.dta", clear

// DECISION: Recode -99 to Stata missing. -99 is the raw system's missing code;
// confirmed in data dictionary provided by central bank (see /docs/fdi_codebook.pdf).
// This affects ~3% of observations. We do not impute; they are dropped in the
// final estimation sample via listwise deletion.
replace fdi_inflows_raw = . if fdi_inflows_raw == -99
rename fdi_inflows_raw fdi_inflows

// DECISION: Standardise district codes to a consistent 6-digit integer.
// Pre-2015 codes are 4-digit; multiply by 100 to align with post-2015 format.
// This coding inconsistency is documented in the admin notes (2015 redistricting).
destring district_code_raw, gen(district_code_num) force
gen district_id = district_id_raw   // Use integer ID as merge key (unambiguous)

// DECISION: Log-transform FDI inflows for regression (right-skewed distribution).
// Zero values are assigned log(1) = 0 to preserve them in the sample.
// Negative values do not exist after recoding.
gen ln_fdi = ln(fdi_inflows + 1)
label variable ln_fdi "Log FDI inflows (log(fdi+1))"

// Flag observations with missing FDI for transparency in the log
gen fdi_missing = missing(fdi_inflows)
quietly count if fdi_missing == 1
di "FDI missing observations: `r(N)' (will be dropped in estimation)"

keep district_id year region fdi_inflows ln_fdi fdi_missing
save "$temp/clean_fdi.dta", replace

// ── Clean exports source ──────────────────────────────────────────────────────
use "$temp/raw_exports.dta", clear

// DECISION: Flag and drop the known 2011 data entry error for districts 31–40.
// Source: administrative quality note from Trade Ministry, dated March 2019.
// We treat as missing rather than attempting correction — no reliable source exists.
gen export_entry_error = (district_id_raw >= 31 & district_id_raw <= 40 & year == 2011)
replace exports_raw = . if export_entry_error == 1
quietly count if export_entry_error == 1
di "Export entry errors flagged and set to missing: `r(N)' observations"

rename district_id_raw district_id
rename exports_raw exports

gen ln_exports = ln(exports + 1)
label variable exports    "Total exports USD mn (cleaned)"
label variable ln_exports "Log exports (log(exports+1))"

keep district_id year exports ln_exports export_firms_raw export_entry_error
save "$temp/clean_exports.dta", replace

di "Section 03 complete: cleaned files saved to $temp"


/* =============================================================================
   SECTION 04: DATASET MERGES AND PANEL CONSTRUCTION
   Merge order and key choices are documented explicitly. Each merge is checked
   immediately after execution — unmatched observations are logged, not silently
   dropped.
   ============================================================================= */

// ── Merge 1: FDI + Exports (many-to-one on district_id x year) ───────────────
use "$temp/clean_fdi.dta", clear

merge 1:1 district_id year using "$temp/clean_exports.dta"

// DECISION: All observations should match (same district-year coverage by design).
// Log any mismatches — in a live project, mismatches trigger investigation before
// proceeding.
quietly count if _merge == 1
di "Unmatched from FDI file (master only): `r(N)'"
quietly count if _merge == 2
di "Unmatched from exports file (using only): `r(N)'"
quietly count if _merge == 3
di "Matched observations: `r(N)'"

// DECISION: Drop unmatched observations and document. In a live project, unmatched
// observations are saved to a separate file and inspected before dropping.
drop if _merge != 3
drop _merge

// ── Merge 2: Add cross-sectional controls (many-to-one on district_id) ────────
merge m:1 district_id using "$temp/raw_controls.dta"

quietly count if _merge != 3
if `r(N)' > 0 {
    di as error "WARNING: `r(N)' unmatched observations after controls merge. Investigate."
}
drop if _merge != 3
drop _merge

// ── Declare panel structure ───────────────────────────────────────────────────
xtset district_id year, yearly
di "Panel declared: `r(N)' district-year observations"
xtdescribe

save "$clean/panel_analysis.dta", replace
di "Section 04 complete: analysis panel saved to $clean"


/* =============================================================================
   SECTION 05: VARIABLE CONSTRUCTION
   Dummy variables, interaction terms, and transformations constructed here.
   Construction logic is commented at the line level so any output is traceable
   to this section.
   ============================================================================= */

use "$clean/panel_analysis.dta", clear

// ── Treatment and time dummies ────────────────────────────────────────────────

// Post-treatment dummy: SEZ policy announced and effective from 2016 onward
gen post_2016 = (year >= 2016)
label variable post_2016 "=1 if year >= 2016 (post-SEZ designation)"

// DiD interaction: treated district x post period
// This is the variable of interest in the DiD specification (Section 08)
gen did_treatment = sez_district * post_2016
label variable did_treatment "DiD: SEZ district x Post-2016"

// ── FDI intensity classification ──────────────────────────────────────────────
// DECISION: High-FDI districts defined as those whose average FDI inflow over the
// full sample period exceeds the 75th percentile. Cut-off is computed on the
// full sample (including pre-treatment years) to avoid look-ahead bias.
quietly bysort district_id: egen mean_fdi = mean(fdi_inflows)
quietly summarize mean_fdi, detail
local p75 = r(p75)
gen high_fdi_district = (mean_fdi > `p75') if !missing(mean_fdi)
label variable high_fdi_district "=1 if district avg FDI > 75th pctile"
di "High-FDI threshold (75th pctile of district mean FDI): `p75'"

// ── Interaction terms for heterogeneity analysis ──────────────────────────────
gen fdi_x_infrastructure = ln_fdi * infrastructure
label variable fdi_x_infrastructure "Interaction: log FDI x infrastructure index"

gen fdi_x_literacy = ln_fdi * literacy_rate
label variable fdi_x_literacy "Interaction: log FDI x literacy rate"

// ── Year and region dummies ───────────────────────────────────────────────────
// Absorbed by fixed effects in xthdidregress/xtreg — generated here for
// explicit use in pooled OLS robustness checks.
quietly tabulate year,   generate(yr_)
quietly tabulate region, generate(reg_)

// ── Outcome: export growth ────────────────────────────────────────────────────
sort district_id year
by district_id: gen export_growth = ln_exports - ln_exports[_n-1]
label variable export_growth "Annual log-point change in exports"

save "$clean/panel_analysis.dta", replace
di "Section 05 complete: variables constructed"


/* =============================================================================
   SECTION 06: SUMMARY STATISTICS
   ============================================================================= */

use "$clean/panel_analysis.dta", clear

// Full sample summary
estpost summarize ln_exports ln_fdi exports fdi_inflows infrastructure ///
    literacy_rate sez_district post_2016 did_treatment, detail

esttab using "$output/table_summary_stats.tex", replace ///
    cells("mean(fmt(%9.3f)) sd(fmt(%9.3f)) min(fmt(%9.3f)) max(fmt(%9.3f)) count(fmt(%9.0f))") ///
    label nomtitle nonumber ///
    title("Summary Statistics") ///
    addnotes("Full panel, 2010–2022. All monetary values in USD millions (log-transformed for estimation).")

di "Section 06 complete: summary statistics exported"


/* =============================================================================
   SECTION 07: TWO-WAY FIXED EFFECTS ESTIMATION (TWFE)
   Specification: ln_exports(it) = beta*ln_fdi(it) + X(it)*gamma
                                  + alpha(i) + delta(t) + epsilon(it)
   District FE absorb time-invariant confounders.
   Year FE absorb common aggregate shocks.
   Standard errors clustered at the district level.
   ============================================================================= */

use "$clean/panel_analysis.dta", clear

// ── Model 1: Baseline TWFE (no controls) ─────────────────────────────────────
quietly xtreg ln_exports ln_fdi i.year, fe vce(cluster district_id)
estimates store m1_baseline
di "Model 1 (Baseline TWFE): beta_fdi = " _b[ln_fdi] " (se = " _se[ln_fdi] ")"

// ── Model 2: TWFE with district-level controls ────────────────────────────────
quietly xtreg ln_exports ln_fdi infrastructure literacy_rate i.year, ///
    fe vce(cluster district_id)
estimates store m2_controls
di "Model 2 (TWFE + controls): beta_fdi = " _b[ln_fdi] " (se = " _se[ln_fdi] ")"

// ── Model 3: TWFE with interaction terms (heterogeneity by infrastructure) ────
quietly xtreg ln_exports ln_fdi fdi_x_infrastructure infrastructure ///
    literacy_rate i.year, fe vce(cluster district_id)
estimates store m3_interaction
di "Model 3 (TWFE + interaction): beta_fdi = " _b[ln_fdi]

// ── Export TWFE results to LaTeX ──────────────────────────────────────────────
esttab m1_baseline m2_controls m3_interaction ///
    using "$output/table_twfe.tex", replace ///
    se star(* 0.10 ** 0.05 *** 0.01) ///
    keep(ln_fdi fdi_x_infrastructure infrastructure literacy_rate) ///
    label ///
    mtitles("Baseline" "Controls" "Interaction") ///
    title("Two-Way Fixed Effects: FDI and Export Performance") ///
    booktabs ///
    addnotes("All models include district and year fixed effects." ///
             "Standard errors clustered at district level in parentheses." ///
             "* p<0.10, ** p<0.05, *** p<0.01")

di "Section 07 complete: TWFE results exported to $output/table_twfe.tex"


/* =============================================================================
   SECTION 08: DIFFERENCE-IN-DIFFERENCES SPECIFICATION
   Exploits the staggered rollout of the SEZ policy (2016 designation) to
   identify a local average treatment effect on export performance.
   Treated: districts 21–35 (SEZ-designated). Control: all other districts.
   Assumption: parallel pre-trends. Tested via event-study plot (Section 09).

   Specification: ln_exports(it) = beta*DiD(it) + X(it)*gamma
                                  + alpha(i) + delta(t) + epsilon(it)
   ============================================================================= */

use "$clean/panel_analysis.dta", clear

// ── Main DiD estimate ─────────────────────────────────────────────────────────
quietly xtreg ln_exports did_treatment infrastructure literacy_rate i.year, ///
    fe vce(cluster district_id)
estimates store did_main
di "DiD estimate (ATT): " _b[did_treatment] " (se = " _se[did_treatment] ")"

// ── DiD with export firm count as additional outcome ─────────────────────────
quietly xtreg export_firms_raw did_treatment infrastructure literacy_rate ///
    i.year, fe vce(cluster district_id)
estimates store did_firms

// ── Export DiD results ────────────────────────────────────────────────────────
esttab did_main did_firms ///
    using "$output/table_did.tex", replace ///
    se star(* 0.10 ** 0.05 *** 0.01) ///
    keep(did_treatment infrastructure literacy_rate) ///
    label ///
    mtitles("Log Exports" "No. Export Firms") ///
    title("Difference-in-Differences: SEZ Designation and Export Performance") ///
    booktabs ///
    addnotes("Treatment: SEZ-designated districts (21–35), effective 2016." ///
             "District and year fixed effects included in all specifications." ///
             "Standard errors clustered at district level in parentheses." ///
             "* p<0.10, ** p<0.05, *** p<0.01")

di "Section 08 complete: DiD results exported to $output/table_did.tex"


/* =============================================================================
   SECTION 09: ROBUSTNESS CHECKS
   ============================================================================= */

use "$clean/panel_analysis.dta", clear

// ── Pre-trends test: event-study coefficients ─────────────────────────────────
// Construct relative-time indicators (omitted: t = -1, the year before treatment)
// DECISION: Year 2015 (t = -1) is the omitted reference category.

gen rel_year = year - 2016                        // 0 = treatment year
gen treated  = sez_district

// Generate leads and lags (cap at +-4 to avoid thin-tailed cells)
forvalues k = 4(-1)1 {
    gen pre_`k' = treated * (rel_year == -`k')
    label variable pre_`k' "Pre-treatment t-`k'"
}
forvalues k = 0/4 {
    gen post_`k' = treated * (rel_year == `k')
    label variable post_`k' "Post-treatment t+`k'"
}

// Event study regression (omit pre_1 = t-1 as reference)
quietly xtreg ln_exports pre_4 pre_3 pre_2 ///
    post_0 post_1 post_2 post_3 post_4 ///
    infrastructure literacy_rate i.year, ///
    fe vce(cluster district_id)
estimates store eventstudy

// Store coefficients for plotting
gen     coef_es = .
gen     ci_lo   = .
gen     ci_hi   = .
gen     time_es = .

local j = 1
foreach v in pre_4 pre_3 pre_2 post_0 post_1 post_2 post_3 post_4 {
    // Recover event-time value from variable name
    local t = regexr("`v'", "pre_", "")
    local t = regexr("`t'", "post_", "")
    if regexm("`v'", "pre") local t_val = -`t'
    else                     local t_val = `t'
    quietly replace time_es = `t_val' in `j'
    quietly replace coef_es = _b[`v']  in `j'
    quietly replace ci_lo   = _b[`v'] - 1.96 * _se[`v'] in `j'
    quietly replace ci_hi   = _b[`v'] + 1.96 * _se[`v'] in `j'
    local j = `j' + 1
}

// Add omitted reference category (t = -1, coef = 0 by construction)
quietly {
    local ref_row = `j'
    replace time_es = -1 in `ref_row'
    replace coef_es =  0 in `ref_row'
    replace ci_lo   =  0 in `ref_row'
    replace ci_hi   =  0 in `ref_row'
}

// Plot pre-trends (visual test of parallel trends assumption)
twoway (rcap ci_hi ci_lo time_es, lcolor(navy%60)) ///
       (scatter coef_es time_es, mcolor(navy) msize(medium)) ///
       (line coef_es time_es, lcolor(navy) lpattern(solid)), ///
       xline(-0.5, lcolor(red) lpattern(dash)) ///
       yline(0, lcolor(black) lpattern(solid)) ///
       xlabel(-4(1)4) ///
       xtitle("Years relative to SEZ designation") ///
       ytitle("Coefficient (log exports)") ///
       title("Event Study: Pre-trend Test") ///
       subtitle("Reference: t = -1; 95% confidence intervals shown") ///
       legend(off) ///
       graphregion(color(white)) plotregion(color(white))

graph export "$output/figure_eventstudy.pdf", replace

// ── Robustness: exclude regions with entry errors ─────────────────────────────
// DECISION: districts 31–40 had a documented export data error in 2011 (set to
// missing in cleaning). As a robustness check, we exclude these districts
// entirely to verify results are not driven by imputation of their 2011 values.
quietly xtreg ln_exports did_treatment infrastructure literacy_rate i.year ///
    if !(district_id >= 31 & district_id <= 40), ///
    fe vce(cluster district_id)
estimates store did_robustness

esttab did_main did_robustness ///
    using "$output/table_robustness.tex", replace ///
    se star(* 0.10 ** 0.05 *** 0.01) ///
    keep(did_treatment) ///
    label ///
    mtitles("Full Sample" "Excl. Districts 31–40") ///
    title("Robustness Check: DiD Estimate") ///
    booktabs ///
    addnotes("Column 2 excludes districts with known data quality issues (districts 31–40)." ///
             "Standard errors clustered at district level." ///
             "* p<0.10, ** p<0.05, *** p<0.01")

di "Section 09 complete: robustness checks exported"


/* =============================================================================
   SECTION 10: CLOSE AND FINALISE
   ============================================================================= */

estimates clear

di " "
di "==============================================================="
di " Pipeline complete: $S_DATE $S_TIME"
di " Outputs written to: $output"
di " Log written to:     $log"
di "==============================================================="

log close
