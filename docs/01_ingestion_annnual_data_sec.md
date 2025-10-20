# Data Ingestion from SEC – Developer Guide

**Module name:** `data_ingestion_from_sec`  

**Purpose:** Retrieve annual (10-K) financial statement data for US-listed companies using the SEC EDGAR API and store structured results for modelling and valuation workflows.  

**Version:** MVP-1  

**Author:** Agalya Ayyadurai

**Last updated:** 2025-10-08  

---

## Overview

This module automates retrieval of structured financial data from the **U.S. SEC EDGAR API**.  
It supports:
- Pulling company filings (10-K annual reports)
- Extracting key XBRL financial metrics
- Cleaning, validating, and storing the data in tidy DataFrames  
- Saving outputs locally (CSV) for downstream analysis or database ingestion

Future versions will support:
- Quarterly data (10-Q)
- Direct database writes (Neon / AWS RDS)
- Cloud-based deployment

---

## Environment Setup

### 1. Create a Virtual Environment

```bash
python -m venv venv
source venv/bin/activate        # on macOS/Linux
venv\Scripts\activate         # on Windows
```

### 2. Install Core Dependencies

```bash
pip install requests pandas python-dotenv jupyter
```

(Optional but recommended: `ipykernel` for linking notebooks)

```bash
pip install ipykernel
python -m ipykernel install --user --name=venv
```

---

## Managing Secrets with `.env`

### 1. Create `.env` file (never commit this)

In the project root:
```bash
touch .env
```

Add your SEC API key:
```
SEC_API_KEY=your_api_key_here
```

Make sure `.gitignore` contains:
```
.env
```

### 2. Load secrets in your code (IDE or scripts)

```python
import os
from dotenv import load_dotenv

load_dotenv()
SEC_API_KEY = os.getenv("SEC_API_KEY")
```

### 3. Load secrets in Jupyter Notebook

> Jupyter doesn’t automatically load the `.env` file unless explicitly instructed.  
> You can safely use the same pattern inside your notebook.

```python
import os
from dotenv import load_dotenv

load_dotenv()  # Loads variables from .env into environment
SEC_API_KEY = os.getenv("SEC_API_KEY")

print("API key loaded:", bool(SEC_API_KEY))  # Should print True (not the key itself)
```

**Tip:**  
Never display or print sensitive values. Just confirm they’re loaded with a boolean check.

---

## Ensuring the Notebook Uses the Same Kernel as Your Virtual Environment

If you created your venv using `python -m venv venv`, ensure your Jupyter notebook uses that interpreter.

### Steps

1. Activate your virtual environment:
   ```bash
   source venv/bin/activate     # or venv\Scripts\activate
   ```

2. Install the kernel package:
   ```bash
   pip install ipykernel
   ```

3. Register your venv as a Jupyter kernel:
   ```bash
   python -m ipykernel install --user --name=venv --display-name "venv <Project>"
   ```

4. Restart VS Code or Jupyter Lab →  
   Click **Kernel > Change Kernel > “venv (Project)”**

You’re now guaranteed the notebook and project share the same environment.

---

## SEC Data Access (Summary)

**API Endpoint:**  
- `https://data.sec.gov/api/xbrl/company_facts/CIK{CIK}.json`

**Key points:**
- Include a descriptive `User-Agent` header:  
  `"Your Name your_email@example.com"`  
  (SEC requires this or requests may be throttled)
- Respect rate limits (no more than ~10 requests per second)
- Use your `.env` for API keys if required (some endpoints don’t require keys)
- Normalize units (e.g., USD vs thousands)

---

## Output Structure

Default folder layout for saved files:

```
data/
  raw/YYYYMMDD/                 # raw JSON responses
  processed/YYYYMMDD/           # structured CSV outputs
```

Sample output files:
- `processed/YYYYMMDD/AAPL_income_annual.csv`
- `processed/YYYYMMDD/AAPL_balance_annual.csv`
- `processed/YYYYMMDD/AAPL_cashflow_annual.csv`

Each file includes standardized column names, fiscal year, and metadata (CIK, accession, asof_utc, source).

---

## QA Checklist (Quick)

| Check Type | Description | Status |
|-------------|--------------|--------|
| Null values | Verify critical fields are populated (revenue, net income, assets, etc.) | ☐ |
| Duplicates  | Ensure unique `(cik, fy)` per statement | ☐ |
| Reconciliation | `assets ≈ liabilities + equity` within ±1% | ☐ |
| Units consistency | Verify all reported in USD | ☐ |
| File integrity | Each CSV non-empty and timestamped | ☐ |

---

## Next Steps

- [ ] Modularize into functions/classes with docstrings (`data_ingestion/client.py`, `data_ingestion/utils.py`)  
- [ ] Add `pytest` unit tests for at least 2 known filers  
- [ ] Integrate Neon or RDS database connection  
- [ ] Create logging utilities for ingestion runs  
- [ ] Add Dash-based visual QA dashboard  

---

## References

- **SEC EDGAR API Documentation:**  
  https://www.sec.gov/edgar/sec-api-documentation  

- **Python Dotenv Docs:**  
  https://pypi.org/project/python-dotenv/  

- **Jupyter Kernel Setup:**  
  https://ipython.readthedocs.io/en/stable/install/kernel_install.html  

---

**End of document**