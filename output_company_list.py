import pandas as pd
import numpy as np
import re
from pathlib import Path

# import utils
from lca_utils import *

# data path
PARQUET_PATH = Path("data/LCA_Disclosure_Data_FY2025_Q3.parquet")
OUTPUT_PATH = Path("company_list_output/output_company_list.xlsx")

# 1. read in whole dataset
def read_parquet(path):
    df = pd.read_parquet(path, engine="fastparquet")
    return df

# 2. filter dataset for Data positions
def filter_data_positions(df):
    # 1) SOC_TITLE match
    soc_targets = {s.casefold() for s in ["Data Scientists", "Business Intelligence Analysts"]}
    mask_soc = df["SOC_TITLE"].astype(str).str.strip().str.casefold().isin(soc_targets)

    # 2) JOB_TITLE match
    keywords = [
        "Data Analyst",
        "Data Scientist",
        "Data Science",
        "Business Analyst",
        "Data Analytics",
        "Advanced Analytics",
    ]
    pattern = "|".join(re.escape(k) for k in keywords)
    mask_job = df["JOB_TITLE"].astype(str).str.contains(pattern, case=False, na=False)

    # 3) combine both match conditions
    data_subset = df[mask_soc | mask_job].copy()

    return data_subset

# 3. filter dataset with only CHANGE_EMPLOYER category
def filter_change_emp(df):
    df['CHANGE_EMPLOYER'] = df['CHANGE_EMPLOYER'].astype(int)
    data_chg_emp = df[df['CHANGE_EMPLOYER'] > 0].copy()

    return data_chg_emp

# 4. filter dataset with my local emp or work states
def filter_states(df):
    STATES = {'NJ', 'NY', 'CT'}
    work_state = df['WORKSITE_STATE'].astype(str).str.strip().str.upper()
    emp_state = df['EMPLOYER_STATE'].astype(str).str.strip().str.upper()
    mask = work_state.isin(STATES) | emp_state.isin(STATES)  # ← 用 Series.isin()
    return df[mask].copy()

# 5. select target columns and remove duplicates
def output_company_list(df):
    cols = ['EMPLOYER_NAME', 'EMPLOYER_ADDRESS1',
            'EMPLOYER_POC_LAST_NAME', 'EMPLOYER_POC_FIRST_NAME', 'EMPLOYER_POC_JOB_TITLE',
            'EMPLOYER_POC_ADDRESS1', 'EMPLOYER_POC_ADDRESS2',
            'EMPLOYER_POC_CITY', 'EMPLOYER_POC_STATE', 'EMPLOYER_POC_POSTAL_CODE',
            'EMPLOYER_STATE']
    # 1. app count per employer
    counts = (
        df.groupby('EMPLOYER_NAME')
        .size()
        .reset_index(name='APPLICATION_COUNT')
    )
    # 2. worksite concat per employer, "NJ, NY, CT"
    worksite_states = (
        df[['EMPLOYER_NAME', 'WORKSITE_STATE']]
        .dropna()
        .assign(WORKSITE_STATE=lambda x: x['WORKSITE_STATE'].astype(str).str.strip().str.upper())
        .query("WORKSITE_STATE != ''")
        .drop_duplicates()
        .groupby('EMPLOYER_NAME')['WORKSITE_STATE']
        .apply(lambda s: ", ".join(sorted(s.unique())))
        .reset_index(name='WORKSITE_STATES')  # 新列名
    )

    # 3. 去重后的雇主联系信息（不含 WORKSITE_STATE，因为那是多值）
    dedup = (
        df[cols]
        .drop_duplicates()
        .copy()
    )

    # 4. merge APPLICATION_COUNT 和 WORKSITE_STATES
    dedup = (
        dedup
        .merge(counts, on='EMPLOYER_NAME', how='left')
        .merge(worksite_states, on='EMPLOYER_NAME', how='left')
        .sort_values(by=['EMPLOYER_NAME'])
    )

    dedup.to_excel(OUTPUT_PATH, index=False)

def main():
    df = read_parquet(PARQUET_PATH)
    df = filter_data_positions(df)
    df = filter_change_emp(df)
    df = filter_states(df)
    output_company_list(df)

if __name__ == '__main__':
    main()
