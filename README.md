
# Fundamental Stochastic Dynamics & Portfolio Fragility  
### MSc Financial Data Science Dissertation — Agalya Ayyadurai

This repository contains the implementation, data pipeline, and empirical results for my dissertation research on **stochastic modelling of corporate fundamentals** and its implications for **equity return predictability** and **portfolio fragility**.

The central idea of this work is that **fundamental drivers such as revenue growth are not deterministic accounting quantities**, but evolve as **stochastic diffusion processes** influenced by competitive dynamics, innovation cycles, macroeconomic shocks, and sectoral evolution.

---

## Dissertation Structure & Contribution

The project is organised into **five major research components**, which build on one another:

### A. Stochastic Modelling of Fundamentals (Core Theory Chapter)

Goal: Characterise the statistical behaviour of **revenue growth** at the **sector level**.

Steps:
- Compute **log revenue growth** for each firm-year.
- Group firms into sectors: **Technology, Healthcare, Consumer, Utilities**.
- Estimate parameters of a **discrete AR(1) process**, interpreted as a discretised **Ornstein–Uhlenbeck (OU)** diffusion:

  $$ g_{i,t+1} = \alpha + \phi g_{i,t} + \varepsilon_{i,t} $$

Mapping to continuous-time OU parameters:

$$ \kappa = -\frac{\ln(\phi)}{\Delta t}, \quad
\mu = \frac{\alpha}{1 - \phi}, \quad
\sigma = \sqrt{\frac{2\kappa\sigma_\varepsilon^2}{1-\phi^2}} $$

Interpretation:
- **μ (long‑run growth level)**: sector equilibrium growth
- **κ (mean‑reversion speed)**: competitive reversion forces
- **σ (fundamental volatility)**: shock intensity and uncertainty

Research output:
- Compare which sectors exhibit **stable vs unstable fundamental dynamics**.
- Discuss implications for valuation, predictability and sector resilience.

### B. Predictive Modelling (Core Empirics)

Research question:
> Do sectors with stable and/or high fundamental growth exhibit **higher subsequent stock returns**?

Proposed model:
A **panel regression** linking next‑year returns to current fundamentals and sector diffusion characteristics:

$$ R_{i,t+1} = \beta_0 + \beta_1 Valuation_{i,t} + \beta_2 Profitability_{i,t} +
\beta_3 \sigma_s + \beta_4 \mu_s + \beta_5 \kappa_s + u_{i,t} $$

Where:
- dependent variable: **next‑year stock return**
- independent variables include:
  - firm fundamentals (e.g., profitability, leverage, cash generation)
  - sector stochastic parameters (μ, σ, κ)
  - optionally **past returns or volatility**

Model choice justification:
- Combines theory-driven structure with empirical validation.
- Avoids purely prediction‑driven black‑box modelling.

### C. Portfolio Construction

Use **expected returns** (from Model B) and **risk estimates** to construct sample portfolios such as:

- **Market-cap benchmark**
- **Minimum-variance portfolio**
- **Model-informed high‑expected‑return portfolio**

Methodology: **Markowitz mean‑variance optimisation**.

### D. Stability & Fragility Analysis

Using stochastic fundamentals + estimated return model:

1. **Simulate future revenue diffusion paths** using OU dynamics.
2. **Simulate return and portfolio value trajectories**.
3. Define **an escape threshold** (e.g., portfolio drawdown below −30%).
4. Estimate **escape probability** = probability portfolio falls below threshold.
5. Compare portfolio resilience across strategies.

This connects to literature on **portfolio fragility**, **escape rates**, and **dynamical systems in finance**.

### E. Backtesting & Robustness

- Train on historical period (e.g., up to 2020/2021)
- Validate on 2022–2023
- (Optional) Test on more recent 2024–2025 observations

Evaluation considerations:
- structural breaks, sector rotation
- non-stationarity in fundamentals
- limitations of annual frequency data

---

## Data Source & Engineering

Data Source: **SEC Financial Statement Data Sets**  
🔗 https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets

Data Pipeline:
```
ZIP → Bronze (raw tables: num, pre, bs, is, cf)
     → Silver (cleaned + standardised identifiers, fiscal date alignment)
     → Gold (panel dataset: firm, sector, fundamentals, log revenue growth)
```

Coverage:
- 8,000+ US public firms
- Approx. 15 years of annual data (10‑K focus)
- Sector mapping via SIC → manually consolidated sector buckets

---

## Methods Summary (Keywords for Research Indexing)

- Stochastic Processes for Fundamentals
- Ornstein–Uhlenbeck Diffusion
- Discrete‑time AR(1) Estimation
- Panel Regression for Expected Returns
- Markowitz Efficient Frontier
- Portfolio Fragility & Escape Probability
- SEC Financial Statement Data Ingestion
- Empirical Asset Pricing and Fundamental Risk

---

## Repository Structure (Research Code)

```
/data/bronze            Raw SEC extracts
/data/silver            Cleaned/standardised fundamentals
/data/gold              Final panel set for modeling
/notebooks              Exploratory + model estimation notes
/src/data_extract       SEC ETL pipeline
/src/data_model         AR(1) + OU parameter estimation
/src/portfolio          (planned) portfolio & escape simulations
```

---

## Author

**Agalya Ayyadurai**  
MSc Financial Data Science — University of Surrey  
🔗 LinkedIn: https://www.linkedin.com/in/agalya-ayyadurai-286517172/

---

## ⚠ Disclaimer
This project is for academic research only and does not constitute investment advice.

