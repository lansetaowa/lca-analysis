# lca-analysis — Employer List Output Workflow

This repository analyzes the U.S. Department of Labor (DOL) **LCA disclosure dataset** and produces an **employer + point-of-contact (POC) list** to support job outreach.

## Key files

- `data/` — place the downloaded DOL files (in XLSX format) here (excluded via `.gitignore`)
- `xlsx_to_parquet.py` — converts the raw XLSX file into a Parquet file for fast loading
- `output_config.py` — **all configurable parameters** (paths, keywords, target SOC titles, target states, output columns) to customize output company list to suit your needs
- `output_company_list.py` — reads Parquet, filters rows, and exports the final employer list to Excel
- `company_list_output/` — output folder (exported Excel from last step)

---
## Environment
- Python 3.11

## Setup
```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate

pip install -r requirements.txt
```

## How to generate the employer list output

### 1) Download the DOL data into `data/`

Download the LCA disclosure dataset from the official DOL page and save it under `data/`.

- DOL page: https://www.dol.gov/agencies/eta/foreign-labor/performance

Example file names used in this repo:
- `data/LCA_Disclosure_Data_FY2025_Q3.xlsx`
- (generated) `data/LCA_Disclosure_Data_FY2025_Q3.parquet`

> Note: This repo's `.gitignore` ignores large data files under `data/`.

---

### 2) Convert XLSX → Parquet

Parquet loads much faster than Excel when the dataset is large.

```bash
python xlsx_to_parquet.py
```

By default, `xlsx_to_parquet.py` reads:
- `data/LCA_Disclosure_Data_FY2025_Q3.xlsx`
and writes:
- `data/LCA_Disclosure_Data_FY2025_Q3.parquet`

---

### 3) Modify parameters in `output_config.py`

Open `output_config.py` and edit as needed:

- **Paths**
  - `PARQUET_PATH`: where the Parquet file is
  - `OUTPUT_PATH`: where the Excel output should be written

- **Filters**
  - `SOC_TARGETS`: SOC_TITLE values to match (case-insensitive exact match)
  - `JOB_TITLE_KEYWORDS`: keywords to match in JOB_TITLE (case-insensitive substring match)
  - `TARGET_STATES`: keep rows where `WORKSITE_STATE` or `EMPLOYER_STATE` is in the set
  - `FILTER_CHANGE_EMPLOYER_ONLY`: if `True`, keep only rows where `CHANGE_EMPLOYER > 0`, applying to scenarios of H1b transfer/H1b recapture etc.

- **Output schema**
  - `OUTPUT_COLUMNS`: the base columns included in the exported table
    - `WORKSITE_STATE` is aggregated into `WORKSITE_STATES` per employer

---

### 4) Run `output_company_list.py`

```bash
python output_company_list.py
```

This produces an Excel file at the configured output path, e.g.:

- `company_list_output/output_company_list.xlsx`

