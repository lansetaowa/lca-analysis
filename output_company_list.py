# import utils
from lca_utils import *
from output_config import (
    PARQUET_PATH,
    OUTPUT_PATH,
    SOC_TARGETS,
    JOB_TITLE_KEYWORDS,
    TARGET_STATES,
    FILTER_CHANGE_EMPLOYER_ONLY,
    OUTPUT_COLUMNS,
)

# 1. read in whole dataset
def read_parquet(path):
    df = pd.read_parquet(path, engine="fastparquet")
    return df

# 2. filter dataset for Data positions
def filter_data_positions(df):
    # 1) SOC_TITLE match
    soc_targets = {s.casefold() for s in SOC_TARGETS}
    mask_soc = df["SOC_TITLE"].astype(str).str.strip().str.casefold().isin(soc_targets)

    # 2) JOB_TITLE match
    pattern = "|".join(re.escape(k) for k in JOB_TITLE_KEYWORDS)
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
    work_state = df['WORKSITE_STATE'].astype(str).str.strip().str.upper()
    emp_state = df['EMPLOYER_STATE'].astype(str).str.strip().str.upper()
    mask = work_state.isin(TARGET_STATES) | emp_state.isin(TARGET_STATES)
    return df[mask].copy()

# 5. select target columns and remove duplicates
def output_company_list(df):
    dedup = (
        df[OUTPUT_COLUMNS]
        .drop_duplicates()
        .copy()
    )

    dedup.to_excel(OUTPUT_PATH, index=False)

def main():
    df = read_parquet(PARQUET_PATH)
    df = filter_data_positions(df)

    if FILTER_CHANGE_EMPLOYER_ONLY:
        df = filter_change_emp(df)

    df = filter_states(df)
    output_company_list(df)

if __name__ == '__main__':
    main()
