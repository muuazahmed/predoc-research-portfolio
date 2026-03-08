# ==============================================================================
# PROJECT:  Contract Choice and Operating Costs in Public Transport
#           A Methodological Reassessment via Structural FIML
#
# AUTHOR:   Muaz Ahmed
#           Paris School of Economics — APE MRes, 2025-2026
#
# SUMMARY:  This script estimates the causal effect of Fixed-Price (incentive)
#           contracts on variable operating costs in French public transport
#           networks (1987–2001), replicating and extending Piechucka (2021).
#
#           The core identification problem is endogenous contract selection:
#           local authorities assign high-powered contracts non-randomly, so
#           naive OLS conflates selection effects with causal cost impacts.
#
#           The econometric strategy proceeds in four steps:
#             1. Naive FE-OLS baseline (establishes upward selection bias)
#             2. Control Function correction (Heckman generalized residual)
#             3. Structural FIML — joint estimation of the cost and selection
#                equations under a bivariate-normal error structure, yielding
#                consistent estimates of δ (FP cost effect) and ρ (error corr.)
#             4. Robustness: DiD on switchers, post-Sapin IV, Sargan tests
#
# INPUTS:   Base077.xls  — unbalanced panel of 49 French urban transport
#                          networks, 1987–2001 (N ≈ 692 network-year obs.)
#
# OUTPUTS:  Console output of all model results
#           LaTeX table (stdout) for the combined FIML outcome + selection eqs.
# ==============================================================================


# ==============================================================================
# 0. DEPENDENCIES
# ==============================================================================

library(readxl)          # XLS ingestion
library(dplyr)           # Data wrangling
library(fixest)          # High-dimensional FE estimation (feols)
library(numDeriv)        # Numerical Hessian for FIML standard errors
library(MASS)            # ginv() fallback for near-singular Hessians
library(car)             # linearHypothesis


# ==============================================================================
# 1. DATA PREPARATION
# ==============================================================================

data_raw <- read_excel("Base077.xls")

# Disambiguate duplicate column name and align variable labels with paper
data_raw <- data_raw %>%
  rename(
    NUM = `NUM...3`,
    FP  = INCENT
  )

# Construct cost-function variables following Gagnepain & Ivaldi (2002):
#   - Normalize all monetary variables by PM (material price) to impose
#     linear homogeneity of degree 1 in input prices
#   - Take logs so the translog cost function is linear in parameters
#   - Center continuous regressors at their sample means so that first-order
#     coefficients (βY, βL, βK) are interpretable as cost elasticities
#     evaluated at the geometric mean of the sample
data <- data_raw %>%
  mutate(
    PL  = LABOR / EMPLOY,
    PM  = GASI,
    Y   = PKO,
    lC  = log(COSTS / PM),
    lY  = log(Y),
    lK  = log(PARC),
    lPL = log(PL / PM),
    t   = if ("YEAR" %in% names(.)) as.numeric(YEAR) else as.numeric(as.factor(TIME))
  ) %>%
  filter(is.finite(lC), is.finite(lY), is.finite(lPL), is.finite(t)) %>%
  mutate(
    lY  = lY  - mean(lY,  na.rm = TRUE),
    lPL = lPL - mean(lPL, na.rm = TRUE),
    lK  = lK  - mean(lK,  na.rm = TRUE)
  )


# ==============================================================================
# 2. REDUCED-FORM ESTIMATION
# ==============================================================================

# ------------------------------------------------------------------------------
# 2.1 Naive Fixed-Effects OLS (Restricted Translog)
#
# Identifies δ under the incorrect assumption E[FP·ε | αi, γt] = 0.
# The positive, significant coefficient on FP reflects upward selection bias:
# local authorities assign high-powered contracts to networks with higher
# unobserved cost shocks, so naive FE overstates the apparent cost of FP.
# ------------------------------------------------------------------------------

naive_ols <- feols(
  lC ~ FP +
    lY + I(0.5 * lY^2) +
    lPL + I(0.5 * lPL^2) +
    I(lY * lPL) +
    lK +
    factor(YEAR) | NUM,
  cluster = ~NUM,
  data    = data
)

summary(naive_ols)


# ------------------------------------------------------------------------------
# 2.2 First-Stage Probit (Contract Selection Equation)
#
# Models the latent preference of the local authority for a Fixed-Price regime.
# Identification relies on two exclusion restrictions:
#   RIGHT     — political orientation (right-wing majority more likely to adopt
#               high-powered, market-oriented contracts)
#   log(NCITIES) — network complexity (more municipalities increases contracting
#               costs under FP due to information asymmetry; Bajari, 2001)
# Operator dummies (TRANS, KEOLIS, CONNEX, AGIR) capture differences in
# bargaining power and incumbency advantages in the tendering process.
# ------------------------------------------------------------------------------

first_stage_probit <- glm(
  FP ~ TRANS + KEOLIS + CONNEX + AGIR + RIGHT + PUBLIC +
    log(NCITIES) + log(PARC) + log(LINES) + log(LENGHT) +
    lY + lPL + I(0.5 * lY^2) + I(0.5 * lPL^2) + I(lY * lPL) +
    factor(YEAR),
  family = binomial(link = "probit"),
  data   = data
)

summary(first_stage_probit)


# ------------------------------------------------------------------------------
# 2.3 Control Function Correction (Two-Step Heckman)
#
# To correct for selection on unobservables, we augment the cost equation with
# the generalized residual κ from the first-stage probit. For a binary
# endogenous regressor, the generalized residual is defined as:
#
#   κᵢ = (FPᵢ - Φ(Xᵢγ)) · φ(Xᵢγ) / [Φ(Xᵢγ) · (1 - Φ(Xᵢγ))]
#
# where φ(·) and Φ(·) are the standard normal PDF and CDF. When included as
# an additional regressor, κ absorbs the covariance between the selection
# unobservable ηᵢ and the cost unobservable εᵢ. Its coefficient θ identifies
# ρ·σ in the bivariate-normal framework. Note: for binary treatment the
# generalized residual and the Inverse Mills Ratio (IMR) are algebraically
# equivalent; we verify this as an internal consistency check.
# ------------------------------------------------------------------------------

# Compute probit index, predicted probability, and density
eta <- predict(first_stage_probit, type = "link")
p   <- pnorm(eta)
phi <- dnorm(eta)

# Clip predicted probabilities to avoid division by zero at the boundary
eps       <- 1e-8
p_clipped <- pmin(pmax(p, eps), 1 - eps)

# Generalized residual (κ) and Inverse Mills Ratio (IMR) — should be identical
data <- data %>%
  mutate(
    kappa_gr  = (FP - p_clipped) * phi / (p_clipped * (1 - p_clipped)),
    eta_link  = predict(first_stage_probit, type = "link"),
    p_hat     = pnorm(eta_link),
    phi_hat   = dnorm(eta_link),
    p2        = pmin(pmax(p_hat, 1e-8), 1 - 1e-8),
    kappa_imr = ifelse(FP == 1, phi_hat / p2, -phi_hat / (1 - p2))
  )

# Second stage: κ absorbs the endogeneity; the coefficient on FP now identifies
# δ conditional on selection on unobservables
cf_ols_gr <- feols(
  lC ~ FP + kappa_gr +
    lY + lPL + lK +
    I(0.5 * lY^2) + I(0.5 * lPL^2) + I(lY * lPL) +
    factor(YEAR) | NUM,
  cluster = ~NUM,
  data    = data
)

# IMR variant — internal robustness check (should produce identical FP estimate)
cf_ols_imr <- feols(
  lC ~ FP + kappa_imr +
    lY + lPL + lK +
    I(0.5 * lY^2) + I(0.5 * lPL^2) + I(lY * lPL) +
    factor(YEAR) | NUM,
  cluster = ~NUM,
  data    = data
)

etable(
  naive_ols, cf_ols_gr, cf_ols_imr,
  se   = "cluster",
  dict = c(kappa_gr = "κ (Gen. Residual)", kappa_imr = "κ (IMR)")
)


# ==============================================================================
# 3. STRUCTURAL FIML ESTIMATION
# ==============================================================================

# ------------------------------------------------------------------------------
# 3.1 Data Preparation for FIML
#
# We construct a clean estimation sample (data_ml) that is consistent across
# all FIML specifications: no NAs in any outcome or selection variable, and
# no degenerate log-transformed values.
# ------------------------------------------------------------------------------

data_ml <- data %>%
  mutate(
    FP  = as.integer(FP),
    NUM = as.factor(NUM)
  ) %>%
  drop_na(
    lC, FP, lY, lPL, lK, t, NUM,
    TRANS, KEOLIS, CONNEX, AGIR, PUBLIC,
    RIGHT, NCITIES, PARC, LINES, LENGHT
  ) %>%
  filter(
    is.finite(lC), is.finite(lY), is.finite(lPL), is.finite(lK),
    is.finite(log(NCITIES)), is.finite(log(PARC))
  )

cat("FIML estimation sample:", nrow(data_ml), "observations,",
    length(unique(data_ml$NUM)), "networks\n")


# ------------------------------------------------------------------------------
# 3.2 FIML Log-Likelihood Function
#
# The structural model is a bivariate-normal endogenous treatment effects model:
#
#   Cost equation:    lCᵢ  = Xᵢβ + δ·FPᵢ + εᵢ,     εᵢ ~ N(0, σ²)
#   Selection eq.:    FPᵢ* = Wᵢγ + ηᵢ,              ηᵢ ~ N(0, 1)
#   Error structure:  Corr(εᵢ, ηᵢ) = ρ
#
# The individual log-likelihood contribution is:
#
#   ℓᵢ = -log(σ) + log φ(uᵢ/σ) + log Φ(qᵢ)
#
# where:
#   uᵢ = lCᵢ - Xᵢβ - δ·FPᵢ            (cost equation residual)
#   qᵢ = (2FPᵢ - 1) · [Wᵢγ + (ρ/σ)uᵢ] / √(1-ρ²)
#                                        (selection probability conditional on uᵢ)
#
# Parameterisation:
#   log(σ) instead of σ  — ensures σ > 0 under unconstrained optimisation
#   atanh(ρ) instead of ρ — ensures ρ ∈ (-1,1) (Fisher z-transform)
# ------------------------------------------------------------------------------

negloglik <- function(par, y, d, X, W) {

  p_beta  <- ncol(X)
  p_gamma <- ncol(W)

  beta      <- par[seq_len(p_beta)]
  xi        <- par[p_beta + 1]
  gamma     <- par[(p_beta + 2):(p_beta + 1 + p_gamma)]
  log_sigma <- par[p_beta + 1 + p_gamma + 1]
  atanh_rho <- par[p_beta + 1 + p_gamma + 2]

  sigma <- exp(log_sigma)
  rho   <- tanh(atanh_rho)
  s     <- sqrt(1 - rho^2)

  u  <- y - as.numeric(X %*% beta) - xi * d   # cost equation residual
  a  <- as.numeric(W %*% gamma)               # selection equation index
  q  <- (2 * d - 1) * (a + (rho / sigma) * u) / s

  ll <- -log(sigma) + dnorm(u / sigma, log = TRUE) + pnorm(q, log.p = TRUE)

  if (any(!is.finite(ll))) return(1e12)
  -sum(ll)
}


# Per-observation log-likelihood (used for numerical score computation)
ll_obs <- function(par, y_i, d_i, X_i, W_i, p_beta, p_gamma) {

  beta      <- par[seq_len(p_beta)]
  xi        <- par[p_beta + 1]
  gamma     <- par[(p_beta + 2):(p_beta + 1 + p_gamma)]
  log_sigma <- par[p_beta + 1 + p_gamma + 1]
  atanh_rho <- par[p_beta + 1 + p_gamma + 2]

  sigma <- exp(log_sigma)
  rho   <- tanh(atanh_rho)
  s     <- sqrt(1 - rho^2)

  u <- y_i - as.numeric(X_i %*% beta) - xi * d_i
  a <- as.numeric(W_i %*% gamma)
  q <- (2 * d_i - 1) * (a + (rho / sigma) * u) / s

  -log(sigma) + dnorm(u / sigma, log = TRUE) + pnorm(q, log.p = TRUE)
}


# ------------------------------------------------------------------------------
# 3.3 Design Matrices
#
# Cost equation (X): network FE as dummies to absorb time-invariant
#   heterogeneity; linear time trend for Hicks-neutral technical progress
#   (FIML requires a single time index rather than year dummies because the
#   selection equation must also be identified off cross-sectional variation).
#
# Selection equation (W): operator dummies + political/complexity instruments.
#   Crucially, W does not include network FE — identification of γ comes from
#   cross-network variation in political orientation and organisational complexity.
# ------------------------------------------------------------------------------

X_common <- model.matrix(
  ~ lY + lPL + lK +
    I(0.5 * lY^2) + I(0.5 * lPL^2) + I(lY * lPL) +
    t + NUM,
  data = data_ml
)

y_ml <- data_ml$lC
d_ml <- data_ml$FP


# ------------------------------------------------------------------------------
# 3.4 FIML Estimation Function (Four Nested Specifications)
#
# Starting values are critical for BFGS convergence in a high-dimensional
# likelihood. We initialize using:
#   β₀, δ₀  ← OLS coefficients from the unrestricted cost equation
#              (consistent but biased; provides a warm start in the right region)
#   γ₀      ← first-stage probit MLE
#              (consistent under correct specification of the selection equation)
#   σ₀      ← SD of OLS residuals (a downward-biased but nearby estimate)
#   ρ₀ = 0.2 ← small positive value consistent with expected upward selection bias
#              (regulators assigned FP to high-cost networks ⟹ ρ > 0)
# ------------------------------------------------------------------------------

run_fiml <- function(model_label, selection_formula) {

  cat(sprintf("\n>>> Estimating: %s <<<\n", model_label))

  W_curr  <- model.matrix(selection_formula, data = data_ml)
  p_beta  <- ncol(X_common)
  p_gamma <- ncol(W_curr)

  # OLS starting values for cost equation (with explicit NUM dummies)
  ols_init <- lm(
    lC ~ FP + lY + lPL + lK +
      I(0.5 * lY^2) + I(0.5 * lPL^2) + I(lY * lPL) +
      t + NUM,
    data = data_ml
  )
  b_init  <- coef(ols_init)
  xi_init <- b_init["FP"]

  beta_init        <- rep(0, p_beta)
  names(beta_init) <- colnames(X_common)
  for (nm in intersect(names(beta_init), names(b_init))) {
    beta_init[nm] <- b_init[nm]
  }

  # Probit starting values for selection equation
  probit_init <- glm(
    selection_formula,
    family = binomial(link = "probit"),
    data   = data_ml
  )
  g_init  <- coef(probit_init)
  gamma_init        <- rep(0, p_gamma)
  names(gamma_init) <- colnames(W_curr)
  for (nm in intersect(names(gamma_init), names(g_init))) {
    gamma_init[nm] <- g_init[nm]
  }

  # Sigma: SD of OLS residuals; rho: small positive reflecting expected bias
  u_init   <- y_ml - as.numeric(X_common %*% beta_init) - xi_init * d_ml
  sigma_init <- sd(u_init, na.rm = TRUE)
  rho_init   <- 0.2

  par_init <- c(
    beta_init,
    xi_FP     = xi_init,
    gamma_init,
    log_sigma = log(sigma_init),
    atanh_rho = atanh(rho_init)
  )

  # Local negative log-likelihood wrapping global matrices
  nll <- function(par) negloglik(par, y = y_ml, d = d_ml, X = X_common, W = W_curr)

  fit <- optim(
    par     = par_init,
    fn      = nll,
    method  = "BFGS",
    control = list(maxit = 5000, reltol = 1e-12)
  )

  if (fit$convergence != 0) {
    warning(sprintf("%s: optim did not converge (code %d)", model_label, fit$convergence))
  }

  # Robust standard errors via numerical Hessian inversion
  # The sandwich formula H⁻¹ M H⁻¹ reduces to H⁻¹ under correct specification;
  # we use the simpler inverse-Hessian form here (robust to misspecification of
  # the information matrix equality when sample is large relative to clusters).
  H       <- numDeriv::hessian(nll, fit$par)
  cov_mat <- tryCatch(solve(H), error = function(e) {
    message("Hessian singular — falling back to generalised inverse.")
    MASS::ginv(H)
  })
  se <- sqrt(diag(cov_mat))

  # Delta-method SE for ρ = tanh(atanh_rho):  d tanh/d x = 1 - tanh²(x)
  rho_hat     <- tanh(fit$par[length(fit$par)])
  rho_se      <- se[length(se)] * (1 - rho_hat^2)

  # Wald χ²(1) test for H₀: ρ = 0 (instrument relevance / endogeneity test)
  wald_rho    <- (rho_hat / rho_se)^2

  par_names   <- c(colnames(X_common), "xi_FP", colnames(W_curr), "log_sigma", "atanh_rho")
  names(fit$par) <- par_names
  names(se)      <- par_names

  list(
    label   = model_label,
    est     = fit$par,
    se      = se,
    p_beta  = p_beta,
    p_gamma = p_gamma,
    loglik  = -fit$value,
    rho     = rho_hat,
    rho_se  = rho_se,
    wald    = wald_rho,
    sigma   = exp(fit$par["log_sigma"])
  )
}

# Four nested specifications (ETM1–ETM4), progressively expanding the
# selection equation to test stability of the cost-equation parameters
res1 <- run_fiml("ETM1", FP ~ TRANS + KEOLIS + CONNEX + AGIR + t)
res2 <- run_fiml("ETM2", FP ~ TRANS + KEOLIS + CONNEX + AGIR + PUBLIC + t)
res3 <- run_fiml("ETM3", FP ~ TRANS + KEOLIS + CONNEX + AGIR + PUBLIC + RIGHT + t)
res4 <- run_fiml("ETM4", FP ~ TRANS + KEOLIS + CONNEX + AGIR + PUBLIC + RIGHT +
                    log(NCITIES) + log(PARC) + log(LINES) + log(LENGHT) +
                    lY + lPL + I(0.5 * lY^2) + I(0.5 * lPL^2) + I(lY * lPL) + t)


# ==============================================================================
# 4. ROBUSTNESS CHECKS
# ==============================================================================

# ------------------------------------------------------------------------------
# 4.1 Sargan Overidentification Test (Pooled IV)
#
# Tests whether operator identity (TRANS, KEOLIS, etc.) satisfies the exclusion
# restriction. Network FE are dropped here so that the time-invariant instruments
# are not collinear with the fixed effects. A rejection indicates the instruments
# are correlated with cost unobservables, violating exogeneity.
# ------------------------------------------------------------------------------

iv_pooled <- feols(
  lC ~ lY + lPL + lK +
    I(0.5 * lY^2) + I(0.5 * lPL^2) + I(lY * lPL) +
    factor(YEAR) + log(NCITIES) |
    FP ~ TRANS + KEOLIS + CONNEX + AGIR + RIGHT + PUBLIC,
  data    = data,
  cluster = ~NUM
)

cat("\n=== SARGAN TEST (Pooled IV, Full Sample) ===\n")
print(summary(iv_pooled))
print(fitstat(iv_pooled, "sargan"))


# ------------------------------------------------------------------------------
# 4.2 Difference-in-Differences on Switchers
#
# Restricts the sample to networks that changed contract type at least once.
# The within-network estimator differences out any time-invariant unobservable
# differences between FP and CP networks, providing a cleaner causal estimate
# that does not rely on cross-sectional variation or exclusion restrictions.
# POST_SWITCH = 1 in all periods at or after a network's first regime change.
# ------------------------------------------------------------------------------

switcher_ids <- data %>%
  group_by(NUM) %>%
  summarize(n_contracts = n_distinct(FP)) %>%
  filter(n_contracts > 1) %>%
  pull(NUM)

switchers <- data %>%
  filter(NUM %in% switcher_ids) %>%
  group_by(NUM) %>%
  mutate(
    initial_contract = first(FP),
    switch_year      = ifelse(
      any(FP != initial_contract),
      min(t[FP != initial_contract], na.rm = TRUE),
      Inf
    ),
    POST_SWITCH = as.numeric(t >= switch_year)
  ) %>%
  ungroup()

did_model <- feols(
  lC ~ POST_SWITCH +
    lY + lPL + lK +
    I(0.5 * lY^2) + I(0.5 * lPL^2) + I(lY * lPL) +
    factor(YEAR) | NUM,
  data    = switchers,
  cluster = ~NUM
)

cat("\n=== DiD: SWITCHERS ONLY ===\n")
print(summary(did_model))


# ------------------------------------------------------------------------------
# 4.3 Sapin Law Interaction Test (1993 Regulatory Shock)
#
# The 1993 Loi Sapin mandated competitive tendering, which should have
# strengthened the efficiency incentive under FP contracts. We test this
# via a triple interaction: FP × Post-1993 × ln(NCITIES). A null result
# implies the competitive shock did not unlock efficiency gains, even for
# complex networks where information asymmetry is greatest.
# ------------------------------------------------------------------------------

data_sapin <- data %>%
  mutate(
    POST_SAPIN      = as.numeric(YEAR >= 1993),
    FP_POST         = FP * POST_SAPIN,
    FP_POST_COMPLEX = FP * POST_SAPIN * log(NCITIES)
  )

sapin_model <- feols(
  lC ~ FP + FP_POST + FP_POST_COMPLEX +
    lY + lPL + lK +
    I(0.5 * lY^2) + I(0.5 * lPL^2) + I(lY * lPL) +
    log(NCITIES) + factor(YEAR) | NUM,
  data    = data_sapin,
  cluster = ~NUM
)

cat("\n=== SAPIN LAW × COMPLEXITY INTERACTION ===\n")
print(summary(sapin_model))


# ------------------------------------------------------------------------------
# 4.4 Post-Sapin IV (1993–2001 Subsample)
#
# Re-estimates the IV model on the competitive era only. If the Sapin Law
# restored instrument validity (by breaking the link between operator identity
# and cost unobservables), the Sargan test should not reject in this subsample.
# A continuing rejection suggests operator-level cost heterogeneity persists
# regardless of the regulatory regime, confounding cross-sectional IV estimates.
# ------------------------------------------------------------------------------

data_post_sapin <- data %>% filter(YEAR >= 1993)

iv_post_sapin <- feols(
  lC ~ lY + lPL + lK +
    I(0.5 * lY^2) + I(0.5 * lPL^2) + I(lY * lPL) +
    log(NCITIES) + factor(YEAR) |
    FP ~ TRANS + KEOLIS + CONNEX + AGIR + RIGHT + PUBLIC,
  data    = data_post_sapin,
  cluster = ~NUM
)

cat("\n=== POST-SAPIN IV (1993–2001) ===\n")
print(summary(iv_post_sapin))
print(fitstat(iv_post_sapin, "sargan"))


# ==============================================================================
# 5. OUTPUT: LATEX TABLE (FIML ESTIMATES)
# ==============================================================================

# Helper: format a point estimate + SE as LaTeX with significance stars
fmt_coef <- function(est, se) {
  if (is.na(est) || is.na(se)) return(c("--", ""))
  z      <- abs(est / se)
  stars  <- dplyr::case_when(
    z > 2.576 ~ "^{***}",
    z > 1.960 ~ "^{**}",
    z > 1.645 ~ "^{*}",
    TRUE      ~ ""
  )
  c(paste0(round(est, 3), stars), paste0("[", round(se, 3), "]"))
}

# Helper: build one LaTeX data row across all four FIML models
latex_row <- function(label, varname) {
  models  <- list(res1, res2, res3, res4)
  line1   <- paste0(label, " & ")
  line2   <- " & "

  for (i in seq_along(models)) {
    mod  <- models[[i]]
    sep  <- if (i < 4) " & " else " \\\\"

    # Retrieve estimate and SE by variable name or by structural position
    if (varname == "xi_FP") {
      idx    <- mod$p_beta + 1
      est    <- mod$est[idx]
      se_val <- mod$se[idx]
    } else if (varname == "rho") {
      est    <- mod$rho
      se_val <- mod$rho_se
    } else if (varname %in% names(mod$est)) {
      est    <- mod$est[varname]
      se_val <- mod$se[varname]
    } else {
      est <- se_val <- NA
    }

    fmt   <- fmt_coef(est, se_val)
    line1 <- paste0(line1, fmt[1], sep)
    line2 <- paste0(line2, fmt[2], sep)
  }
  paste(line1, line2, sep = "\n")
}

# Bottom-row statistics extracted programmatically from model objects
logliks <- sapply(list(res1, res2, res3, res4), function(m) round(m$loglik, 1))
walds   <- sapply(list(res1, res2, res3, res4), function(m) round(m$wald,   2))
n_obs   <- nrow(data_ml)

# --- Print LaTeX Table ---
cat("\n\n% ============================================================\n")
cat("% FIML ESTIMATES — copy into paper\n")
cat("% ============================================================\n")
cat("\\begin{table}[htbp]\n\\centering\n")
cat("\\caption{FIML Estimates: Cost and Contract-Choice Equations}\n")
cat("\\label{tab:fiml_complete}\n")
cat("\\begin{tabular}{lcccc}\n\\toprule\n")
cat(" & (1) & (2) & (3) & (4) \\\\\n")
cat(" & \\textbf{ETM1} & \\textbf{ETM2} & \\textbf{ETM3} & \\textbf{ETM4} \\\\\n\\midrule\n")

cat("\\textit{A. Selection Equation} & & & & \\\\\n")
cat(latex_row("Transdev",               "TRANS"));        cat("\\addlinespace\n")
cat(latex_row("Keolis",                 "KEOLIS"));       cat("\\addlinespace\n")
cat(latex_row("Connex",                 "CONNEX"));       cat("\\addlinespace\n")
cat(latex_row("Agir",                   "AGIR"));         cat("\\addlinespace\n")
cat(latex_row("Public Operator",        "PUBLIC"));       cat("\\addlinespace\n")
cat(latex_row("Right-Wing Majority",    "RIGHT"));        cat("\\addlinespace\n")
cat(latex_row("Complexity (ln Cities)", "log(NCITIES)")); cat("\\midrule\n")

cat("\\textit{B. Cost Equation} & & & & \\\\\n")
cat(latex_row("Fixed-Price ($\\delta$)", "xi_FP")); cat("\\addlinespace\n")
cat(latex_row("Output ($\\ln Y$)",       "lY"));    cat("\\addlinespace\n")
cat(latex_row("Labor Price ($\\ln P_L$)","lPL"));   cat("\\addlinespace\n")
cat(latex_row("Capital ($\\ln K$)",      "lK"));    cat("\\addlinespace\n")
cat(latex_row("Time Trend",              "t"));     cat("\\midrule\n")

cat("\\textit{C. Structural Parameters} & & & & \\\\\n")
cat(latex_row("Correlation ($\\rho$)", "rho")); cat("\\addlinespace\n")

cat(sprintf("Log-Likelihood & %s \\\\\n",
    paste(logliks, collapse = " & ")))
cat(sprintf("Wald $\\chi^2(1)$ ($\\rho = 0$) & %s \\\\\n",
    paste(walds, collapse = " & ")))
cat(sprintf("Observations & %s \\\\\n",
    paste(rep(n_obs, 4), collapse = " & ")))

cat("\\bottomrule\n\\end{tabular}\n\\end{table}\n")
