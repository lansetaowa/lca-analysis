"""output_config.py

Centralized configuration for `output_company_list.py`.

Edit this file to change:
- Input/output paths
- Filter targets (SOC titles / job-title keywords / target states)
- Output columns
"""

from __future__ import annotations

from pathlib import Path

# If True, keep only rows where CHANGE_EMPLOYER > 0
FILTER_CHANGE_EMPLOYER_ONLY = True

# --------------------
# Paths
# --------------------
DATA_DIR = Path("data")
PARQUET_PATH = DATA_DIR / "LCA_Disclosure_Data_FY2025_Q3.parquet"

if FILTER_CHANGE_EMPLOYER_ONLY:
    OUTPUT_PATH = Path("company_list_output") / "output_company_list_transfer.xlsx"
else:
    OUTPUT_PATH = Path("company_list_output") / "output_company_list.xlsx"

# --------------------
# Filters
# --------------------

# SOC_TITLE matches (case-insensitive exact match after strip)
SOC_TARGETS = [
    "Data Scientists",
    "Business Intelligence Analysts",
]

# JOB_TITLE keyword matches (case-insensitive substring match)
JOB_TITLE_KEYWORDS = [
    "Data Analyst",
    "Data Scientist",
    "Data Science",
    "Business Analyst",
    "Data Analytics",
    "Advanced Analytics",
]

# Keep rows where either WORKSITE_STATE or EMPLOYER_STATE is in this set
TARGET_STATES = {"NJ", "NY", "CT"}

# --------------------
# Output schema
# --------------------
# Base columns used for de-duplicated employer contact rows.
# Note: WORKSITE_STATE is aggregated into WORKSITE_STATES (comma-separated).
OUTPUT_COLUMNS = ['RECEIVED_DATE','JOB_TITLE','SOC_TITLE','TOTAL_WORKER_POSITIONS','WAGE_RATE_OF_PAY_FROM','WAGE_UNIT_OF_PAY',
            'EMPLOYER_NAME', 'EMPLOYER_ADDRESS1',
            'EMPLOYER_POC_LAST_NAME', 'EMPLOYER_POC_FIRST_NAME', 'EMPLOYER_POC_JOB_TITLE','EMPLOYER_POC_EMAIL',
            'EMPLOYER_POC_ADDRESS1', 'EMPLOYER_POC_ADDRESS2',
            'EMPLOYER_POC_CITY', 'EMPLOYER_POC_STATE', 'EMPLOYER_POC_POSTAL_CODE',
            'EMPLOYER_STATE']
