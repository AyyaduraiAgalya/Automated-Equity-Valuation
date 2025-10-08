# Automated Equity Valuation

This repository is part of my MSc Financial Data Science dissertation project.  
It aims to **automate the process of pulling company financial statements, forecasting key financial drivers, and using those forecasts as assumptions for equity valuation models** — addressing one of the key pain points in traditional equity research: the manual, assumption-heavy nature of financial modeling.

---

## Project Overview

In equity research, analysts often rely on manual assumptions when forecasting revenues, margins, and other key drivers.
This project builds a **data-driven valuation engine** that combines:

1. **Automated Data Retrieval** — Pulls structured financial statements from public sources (e.g., SEC filings).  
2. **Driver Forecasting Models** — Uses statistical and machine learning models to project future revenues, margins, and cash flows.  
3. **Automated Valuation Framework** — Integrates forecasts into valuation models (e.g., DCF) for intrinsic value estimation.  
4. **Interactive Dashboard** — A web-based interface (built with Dash) for users to explore results and interact with the model dynamically.  

---

## System Architecture (MVP-1)

```
Data Source (SEC)
        ↓
Data Ingestion & Transformation (Neon PostgreSQL)
        ↓
Driver Forecasting (ML Models)
        ↓
Valuation Engine (DCF & Multiples)
        ↓
Interactive Dash Dashboard
        ↓
Deployment (Render)
```

---

## Objectives

- Automate financial data ingestion for public US companies.
- Build modular forecasting pipelines for key drivers such as **revenue**, **operating income**, and **free cash flow**.
- Generate intrinsic value estimates using both statistical and discounted cash flow models.
- Visualise the results through an interactive, cloud-deployed dashboard.

---

## Stack

| Category | Tools / Libraries |
|-----------|------------------|
| Data | `sec-api`, `pandas`, `requests`, `SQLAlchemy` |
| Modelling | `scikit-learn`, `statsmodels`, `prophet` |
| Dashboard | `Dash`, `Plotly` |
| Database | `PostgreSQL (Neon / AWS RDS)` |
| Deployment | `Render`, `AWS EC2` |
| Versioning | `Git`, `GitHub` |

---

## MVP Progress

| Milestone | Description | Status |
|------------|--------------|--------|
| MVP 1 | End-to-end system with annual data, baseline ML forecast, Render + Neon deployment | In progress |
| MVP 2 | Upgrade to AWS RDS + EC2 deployment, add ARIMA/Prophet forecasting | Planned |
| MVP 3 | Integrate quarterly reports, advanced model comparison, and REST API endpoints | Future |

---

## Research Context

This work contributes to the **automation of fundamental valuation** workflows in equity research and investment analysis.  
By empirically testing forecasting accuracy and valuation reliability, it also explores the intersection of **data science**, **financial modelling**, and **decision automation**.

---

## Repository Structure

```
automated-equity-valuation/
│
├── src/                # Core logic (data ingestion, modeling, valuation)
├── dashboard/          # Dash web app
├── notebooks/          # Jupyter Notebooks for initial coding
├── data/               # Local data cache (optional)
├── docs/               # Documentation and diagrams
├── tests/              # Unit tests
├── requirements.txt
└── README.md
```

---

## Author

**Agalya Ayyadurai**  
MSc Financial Data Science, University of Surrey  
LinkedIn: [https://www.linkedin.com/in/agalya-ayyadurai-286517172/]  

---

## Disclaimer

This project is for **academic and educational purposes only**.  
The valuation outputs are not intended as investment advice or recommendations.

---

## Next Steps

- MVP 1 development (data ingestion → forecasting → valuation → dashboard → Render deploy)
- Incorporate cloud databases (Neon → AWS RDS)
- Explore advanced ML models (Prophet, LSTM)
- Automate report generation for equity summaries
