import pandas as pd
from pathlib import Path

MAIN_PATH = Path("data/LCA_Disclosure_Data_FY2025_Q4.xlsx")
PARQUET_PATH = Path("data/LCA_Disclosure_Data_FY2025_Q4.parquet")

# 读入完整文件
main = pd.read_excel(MAIN_PATH, dtype=str, engine="openpyxl")

# 列名标准化
main.columns = [c.strip().upper() for c in main.columns]

# 存为parquet文件
for c in ["RECEIVED_DATE", "DECISION_DATE", "ORIGINAL_CERT_DATE", "BEGIN_DATE", "END_DATE"]:
    if c in main.columns:
        main[c] = pd.to_datetime(main[c], errors="coerce")

main.to_parquet(PARQUET_PATH, engine="fastparquet", compression="snappy", index=False)
print(f"Saved Parquet -> {PARQUET_PATH}")