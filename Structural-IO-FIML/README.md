# Structural IO & Contract Choice: FIML Estimation of Cost Functions

This directory contains the core empirical components of my research on regulatory contract choice and operating efficiency in public transport. The project replicates and extends **Piechucka (2021)**, utilizing a panel of 49 French transport networks (1987–2001) to investigate whether high-powered incentive contracts actually reduce technical operating costs.

### 📄 Primary Materials
* **[Code Sample](Ahmed_Muaz_FIML_Code_Sample.R):** A production-ready R script implementing structural FIML and multiple reduced-form benchmarks.


---

### 🔬 Research Overview
The central challenge in this setting is **endogenous selection bias**: local authorities non-randomly assign Fixed-Price (FP) contracts to networks facing higher unobserved cost shocks. Naive estimators conflate this selection effect with the true causal impact of the contract.

**Key Finding:** While naive FE-OLS suggests FP contracts increase costs by **5.3%**, my structural FIML and within-network (DiD) estimates reveal a **statistically insignificant effect (~3.9%)**. This suggests that efficiency gains from high-powered incentives are likely organizational (overhead reduction) rather than technological (variable cost efficiency).

---

### 💻 Econometric & Programming Features

#### 1. Structural FIML Estimation
I implement a bivariate-normal endogenous treatment effects model to jointly estimate the cost function and the regulator’s selection process.
* **Unconstrained Optimization:** Utilizes `optim` (BFGS) with the **Fisher z-transform** for $\rho$ and **log-transform** for $\sigma$ to ensure parameter constraints are satisfied during the search.
* **Robust Inference:** Standard errors are derived via **numerical Hessian inversion** (`numDeriv`) and the **Delta Method** to back out the standard errors for structural parameters ($\rho, \sigma$).

#### 2. Reduced-Form & Robustness Benchmarks
To validate the structural findings, the script includes:
* **Control Function (Heckman Two-Step):** Augmenting the cost function with generalized residuals to absorb selection on unobservables.
* **DiD on Switchers:** A within-network estimator that exploits regime changes to difference out time-invariant unobserved heterogeneity.
* **Overidentification Tests:** Sargan tests for instrument validity (Loi Sapin and operator identity) using the `fixest` framework.

#### 3. Data Engineering & Reproducibility
* **Defensive Programming:** Automated detection of "switcher" networks to avoid hardcoded IDs.
* **Pipeline Efficiency:** High-dimensional fixed effects handled via `feols` with multi-way clustering.
* **Professional Output:** Programmatic generation of LaTeX tables for direct inclusion in research papers.

---

### 🛠 Technical Stack
* **Language:** R
* **Key Packages:** `fixest` (HDFE), `numDeriv` (Optimization), `dplyr` (Wrangling), `car` (Hypothesis Testing), `readxl`.
* **Version Control:** Full Git history with descriptive commits for reproducibility.

---
**Muaz Ahmed** MRes Economics Student | Paris School of Economics  
[muaz.ahmed@ens.psl.eu](mailto:muaz.ahmed@ens.psl.eu)