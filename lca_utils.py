
"""
lca_utils.py
------------
Reusable helpers for analyzing DOL LCA disclosure data in notebooks.

This module focuses on:
- Column normalization and wage annualization.
- Cached loading (Excel -> Parquet).
- Common descriptive statistics and plotting (matplotlib only).
- SOC-level summaries and role-based wage analysis (Data Analyst / Data Scientist / Business Analyst).

All plotting functions use matplotlib and avoid specifying explicit colors.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Iterable, Tuple, Optional

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from textwrap import fill
import squarify

# ----------------------------
# Basics & cleaning helpers
# ----------------------------

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with uppercased, stripped column names.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.

    Returns
    -------
    pd.DataFrame
        A copy of `df` whose column names are transformed to
        `str(c).strip().upper()` for each original column `c`.
    """
    out = df.copy()
    out.columns = [str(c).strip().upper() for c in out.columns]
    return out


def to_float(value) -> float:
    """Parse numeric-like strings to float, with robust cleaning.

    The function removes currency symbols (e.g. '$'), commas, and whitespace.
    If a range like ``"60-70"`` is detected, the left value is used.
    Values that cannot be parsed are returned as ``NaN``.

    Parameters
    ----------
    value : Any
        Raw value potentially representing a number.

    Returns
    -------
    float
        Parsed float or ``np.nan`` if not parseable.
    """
    if pd.isna(value):
        return np.nan
    s = str(value)
    s = re.sub(r"[,$\s]", "", s)
    s = s.split("-")[0]
    try:
        return float(s)
    except Exception:
        return np.nan


def normalize_unit(u: Optional[str]) -> Optional[str]:
    """Normalize wage unit strings to a canonical set.

    Supported canonical units are: ``HOUR``, ``WEEK``, ``BI-WEEKLY``, ``MONTH``, ``YEAR``.
    Any variant of biweekly such as ``BIWEEKLY`` or ``BI-WEEKLY`` is mapped to ``BI-WEEKLY``.
    Other values return ``None``.

    Parameters
    ----------
    u : Optional[str]
        Raw unit string.

    Returns
    -------
    Optional[str]
        One of {``HOUR``, ``WEEK``, ``BI-WEEKLY``, ``MONTH``, ``YEAR``} or ``None``.
    """
    if u is None or (isinstance(u, float) and np.isnan(u)):
        return None
    t = str(u).strip().upper().replace("_", "-")
    if t in {"BIWEEKLY", "BI-WEEKLY"}:
        return "BI-WEEKLY"
    if t in {"HOUR", "WEEK", "MONTH", "YEAR"}:
        return t
    return None


UNIT_TO_ANNUAL = {
    "HOUR": 2080,     # 40 hours * 52 weeks
    "WEEK": 52,
    "BI-WEEKLY": 26,
    "MONTH": 12,
    "YEAR": 1,
}


def annualize_value(v: float, unit_norm: Optional[str]) -> float:
    """Convert wage value to annual wage using the normalized unit.

    Parameters
    ----------
    v : float
        The wage number to convert.
    unit_norm : Optional[str]
        Canonical unit (see :data:`UNIT_TO_ANNUAL`).

    Returns
    -------
    float
        Annualized wage or ``np.nan`` if not possible.
    """
    if pd.isna(v) or unit_norm is None:
        return np.nan
    factor = UNIT_TO_ANNUAL.get(unit_norm)
    return v * factor if factor else np.nan


def add_offered_wage_annual(
    df: pd.DataFrame,
    from_col: str = "WAGE_RATE_OF_PAY_FROM",
    unit_col: str = "WAGE_UNIT_OF_PAY",
    out_col: str = "OFFERED_WAGE_ANNUAL_USD",
    auto_annual_from_threshold: float | None = 60000.0,
) -> pd.DataFrame:
    """Add an annualized offered wage column to the DataFrame using **FROM only**.

    This function now computes annualized wage **solely from** ``from_col``
    (``WAGE_RATE_OF_PAY_FROM`` by default). 

    Auto-correction rule
    --------------------
    If ``auto_annual_from_threshold`` is not ``None`` (default: ``60000``),
    any row with ``from_col`` parsed numeric value **greater than the threshold**
    will have its pay unit **overridden to ``"YEAR"``** for annualization.
    This reduces outliers caused by common data-entry mistakes where annual
    amounts were paired with hourly/weekly/bi-weekly units.

    Parameters
    ----------
    df : pd.DataFrame
        Source data (columns will be standardized to uppercase).
    from_col : str, default ``"WAGE_RATE_OF_PAY_FROM"``
        Column containing the base wage used for annualization.
    unit_col : str, default ``"WAGE_UNIT_OF_PAY"``
        Column with the unit of pay (e.g., Hour/Week/Month/Year).
    out_col : str, default ``"OFFERED_WAGE_ANNUAL_USD"``
        Name of the resulting annualized wage column.
    auto_annual_from_threshold : float or None, default ``60000.0``
        Threshold for the auto-correction rule. If ``None``, the rule is disabled.

    Returns
    -------
    pd.DataFrame
        A copy of the standardized input with the new annualized wage column added.
    """
    dfx = standardize_columns(df)
    fr = dfx.get(from_col.upper())
    unit = dfx.get(unit_col.upper())

    # Parse FROM and normalize unit
    from_num = fr.map(to_float) if fr is not None else pd.Series(np.nan, index=dfx.index)
    unit_norm = unit.map(normalize_unit) if unit is not None else pd.Series([None]*len(dfx), index=dfx.index)

    # --- Auto-correct obvious unit errors based on FROM threshold ---
    if auto_annual_from_threshold is not None:
        mask_override = from_num > float(auto_annual_from_threshold)
        unit_norm = unit_norm.where(~mask_override, "YEAR")

    # Annualize FROM only
    dfx[out_col.upper()] = [annualize_value(v, u) for v, u in zip(from_num, unit_norm)]
    return dfx

def filter_wage_outliers(
    df: pd.DataFrame,
    from_col: str = "WAGE_RATE_OF_PAY_FROM",
    unit_col: str = "WAGE_UNIT_OF_PAY",
    thresholds: dict | None = None,
    keep_original_columns: bool = True,
) -> pd.DataFrame:
    """
    Remove obvious unit/value outliers BEFORE annualization.

    Rules (after unit normalization):
      - HOUR      & FROM > 1,000     -> drop
      - WEEK      & FROM > 10,000    -> drop
      - BI-WEEKLY & FROM > 50,000    -> drop
      - MONTH     & FROM > 250,000   -> drop

    Parameters
    ----------
    df : pd.DataFrame
        Input table.
    from_col : str, default "WAGE_RATE_OF_PAY_FROM"
        Column containing the numeric base wage used for checks.
    unit_col : str, default "WAGE_UNIT_OF_PAY"
        Column with the unit of pay.
    thresholds : dict or None
        Optional overrides for unit cutoffs. Keys must be canonical units
        after normalization: {"HOUR","WEEK","BI-WEEKLY","MONTH"}.
        Defaults to {"HOUR":1000, "WEEK":10000, "BI-WEEKLY":50000, "MONTH":250000}.
    keep_original_columns : bool, default True
        If True, return a filtered view of the original `df` (preserving original
        column names/casing). If False, return the standardized copy.

    Returns
    -------
    pd.DataFrame
        Filtered DataFrame with outlier rows removed.
    """
    dfx = standardize_columns(df)

    thr = {
        "HOUR": 1000.0,
        "WEEK": 10000.0,
        "BI-WEEKLY": 50000.0,
        "MONTH": 250000.0,
    }
    if thresholds:
        thr.update({str(k).upper(): float(v) for k, v in thresholds.items()})

    # Parse FROM and normalize unit
    from_num = dfx.get(from_col.upper(), pd.Series(np.nan, index=dfx.index)).map(to_float)
    unit_norm = dfx.get(unit_col.upper(), pd.Series(None, index=dfx.index)).map(normalize_unit)

    # Build invalid mask
    invalid = pd.Series(False, index=dfx.index)
    for unit_name, cutoff in thr.items():
        invalid |= (unit_norm == unit_name) & (from_num > cutoff)

    # Return filtered frame
    if keep_original_columns:
        # Map mask back to original index order
        invalid = invalid.reindex(df.index, fill_value=False)
        return df.loc[~invalid].copy()
    else:
        return dfx.loc[~invalid].copy()


# ----------------------------
# Cached loading
# ----------------------------

def load_lca_cached(xlsx_path: str | Path,
                    parquet_path: str | Path | None = None,
                    engine: str = "pyarrow") -> pd.DataFrame:
    """Load an Excel file once and cache it as Parquet for fast re-loads.

    If the Parquet file exists and is newer than the Excel, this function reads
    the Parquet. Otherwise, it reads the Excel (with ``dtype=str``), standardizes
    columns, and writes Parquet (with ``snappy`` compression).

    Parameters
    ----------
    xlsx_path : str or Path
        Path to the Excel file.
    parquet_path : str or Path, optional
        Optional override for the Parquet path. Defaults to ``xlsx_path`` with
        ``.parquet`` extension.
    engine : {"pyarrow", "fastparquet"}, default "pyarrow"
        Parquet engine used for reading/writing.

    Returns
    -------
    pd.DataFrame
        The loaded and standardized DataFrame.
    """
    xlsx_path = Path(xlsx_path)
    if parquet_path is None:
        parquet_path = xlsx_path.with_suffix(".parquet")
    parquet_path = Path(parquet_path)

    if parquet_path.exists() and parquet_path.stat().st_mtime >= xlsx_path.stat().st_mtime:
        try:
            return pd.read_parquet(parquet_path, engine=engine)
        except Exception:
            # Fallback to whichever engine is available.
            return pd.read_parquet(parquet_path)

    main = pd.read_excel(xlsx_path, dtype=str, engine="openpyxl")
    main = standardize_columns(main)
    try:
        main.to_parquet(parquet_path, engine=engine, compression="snappy", index=False)
    except Exception:
        # Attempt alternate engine if the requested one fails.
        eng2 = "fastparquet" if engine == "pyarrow" else "pyarrow"
        main.to_parquet(parquet_path, engine=eng2, compression="snappy", index=False)
    return main


# ----------------------------
# Generic stats & tables
# ----------------------------

def deciles(series: pd.Series, step: float = 0.10, fmt: bool = False) -> pd.DataFrame:
    """Compute a quantile table at regular steps (e.g., 0%,10%,...,100%).

    Parameters
    ----------
    series : pd.Series
        Numeric-like series.
    step : float, default 0.10
        Step between quantiles. ``0.10`` yields deciles.
    fmt : bool, default False
        If ``True``, include a preformatted string column (no rounding control).

    Returns
    -------
    pd.DataFrame
        A table with columns:
        - ``percentile`` (int): 0..100
        - ``value`` (float): quantile at each percentile
        - ``value_fmt`` (str, optional): formatted value (if ``fmt=True``).
    """
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        cols = ["percentile", "value", "value_fmt"] if fmt else ["percentile", "value"]
        return pd.DataFrame(columns=cols)
    qs = np.arange(0, 1.0001, step)
    qv = s.quantile(qs)
    out = pd.DataFrame({"percentile": (qs*100).astype(int), "value": qv.values})
    if fmt:
        out["value_fmt"] = out["value"].map(lambda x: f"{x:,.0f}")
    return out


# ----------------------------
# Plot helpers (matplotlib)
# ----------------------------

def plot_line_monthly_cases(df: pd.DataFrame,
                            case_col: str = "CASE_NUMBER",
                            decision_col: str = "DECISION_DATE",
                            received_col: str = "RECEIVED_DATE") -> pd.DataFrame:
    """Plot unique case counts per month and return the underlying table.

    The function uses ``DECISION_DATE`` when present; otherwise falls back to
    ``RECEIVED_DATE``. Dates are coerced to datetime; rows with missing dates
    are dropped.

    Parameters
    ----------
    df : pd.DataFrame
        Source table.
    case_col : str, default "CASE_NUMBER"
        Column identifying unique cases.
    decision_col : str, default "DECISION_DATE"
        Preferred date column for monthly aggregation.
    received_col : str, default "RECEIVED_DATE"
        Fallback date column when ``decision_col`` is not suitable.

    Returns
    -------
    pd.DataFrame
        One-column DataFrame indexed by YYYY-MM string with the unique case
        count per month (column name ``CASE_COUNT``).
    """
    dfx = standardize_columns(df)
    date_col = decision_col.upper() if decision_col.upper() in dfx.columns else None
    if date_col is None or dfx[date_col].isna().all():
        date_col = received_col.upper()
    dfx[date_col] = pd.to_datetime(dfx[date_col], errors="coerce")
    dfx = dfx.dropna(subset=[date_col])
    dfx["DECISION_MONTH"] = dfx[date_col].dt.to_period("M").astype(str)

    grp = dfx.groupby("DECISION_MONTH")[case_col.upper()].nunique().rename("CASE_COUNT").sort_index()
    # Plot
    x = np.arange(len(grp))
    plt.figure(figsize=(10, 5))
    plt.plot(x, grp.values, marker="o", linewidth=2)
    plt.xticks(x, grp.index, rotation=45, ha="right")
    plt.ylabel("Unique CASE_NUMBER")
    plt.title("Monthly CASE count")
    plt.grid(True, axis="y", alpha=0.3)
    for xi, yi in zip(x, grp.values):
        plt.text(xi, yi, f"{int(yi):,}", va="bottom", ha="center", fontsize=9)
    plt.tight_layout()
    plt.show()
    return grp.to_frame()


def plot_donut_from_counts(counts: pd.Series, title: str = "") -> pd.DataFrame:
    """Plot a donut chart from a counts series and return count + percentage table.

    Parameters
    ----------
    counts : pd.Series
        Counts per category.
    title : str, default ""
        Plot title.

    Returns
    -------
    pd.DataFrame
        Two columns:
        - ``count`` (int)
        - ``pct`` (float percentage with two decimals)
    """
    s = counts.dropna().sort_values(ascending=False)
    total = s.sum() if s.sum() else 1
    labels = [f"{k} ({int(v):,})" for k, v in s.items()]
    sizes = s.values

    fig, ax = plt.subplots(figsize=(4,4))
    ax.pie(
        sizes,
        labels=labels,
        autopct=lambda pct: f"{pct:.1f}%",
        startangle=90,
        wedgeprops=dict(width=0.35),
    )
    ax.set_title(title)
    ax.axis("equal")
    plt.tight_layout()
    plt.show()

    pct = (s / total * 100).round(2)
    return pd.DataFrame({"count": s.astype(int), "pct": pct})


def plot_bar_counts_vertical_simple(series: pd.Series, top_n: int = 20, title: Optional[str] = None) -> pd.DataFrame:
    """Plot a simple vertical bar chart for a categorical series (Top-N + Other).

    The function computes value counts, keeps the top-N categories, optionally
    aggregates the rest into ``"Other"``, and plots a vertical bar chart. The
    figure shows both counts and shares on the bars.

    Parameters
    ----------
    series : pd.Series
        Categorical-like series.
    top_n : int, default 20
        Number of top categories to keep; any remaining are aggregated.
    title : str, optional
        Plot title. If not provided, a default title is used.

    Returns
    -------
    pd.DataFrame
        Table with columns:
        - ``count`` (int)
        - ``pct`` (float, percentage rounded to 1 decimal place)
    """
    s = series.dropna().astype(str).str.strip()
    s = s[s != ""]
    counts = s.value_counts()
    total = counts.sum()
    if top_n is not None and top_n > 0 and len(counts) > top_n:
        top_counts = counts.iloc[:top_n].copy()
        other = counts.iloc[top_n:].sum()
        if other > 0:
            top_counts.loc["Other"] = other
    else:
        top_counts = counts

    pct = (top_counts / total * 100).round(1)
    result = pd.DataFrame({"count": top_counts.astype(int), "pct": pct})

    cats = result.index.tolist()
    vals = result["count"].to_numpy()
    pcts = result["pct"].to_numpy()
    n = len(vals)
    fig_w = min(max(8, 0.6 * n + 2), 24)
    fig, ax = plt.subplots(figsize=(fig_w, 6))
    x = np.arange(n)
    bars = ax.bar(x, vals)
    ax.set_xticks(x, labels=cats, rotation=45, ha="right")
    ax.set_ylabel("Count")
    ax.set_title(title or f"Top {n}")
    ax.grid(axis="y", alpha=0.3)
    ymax = max(vals) if n else 1
    ax.set_ylim(0, ymax * 1.15)
    for xi, rect, v, p in zip(x, bars, vals, pcts):
        ax.text(rect.get_x() + rect.get_width()/2.0, rect.get_height()*1.01, f"{v:,} ({p:.1f}%)",
                ha="center", va="bottom", fontsize=9)
    plt.tight_layout()
    plt.show()
    return result


def plot_box_horizontal_from_groups(group_arrays: Dict[str, np.ndarray], title: str = "") -> None:
    """Plot a horizontal boxplot for multiple named groups.

    Parameters
    ----------
    group_arrays : dict[str, np.ndarray]
        Mapping of display label -> numeric array of values.
    title : str, default ""
        Figure title.

    Returns
    -------
    None
        Shows the figure and returns ``None``.
    """
    labels = list(group_arrays.keys())
    data = [np.asarray(group_arrays[k], dtype=float) for k in labels]
    plt.figure(figsize=(9, 5))
    plt.boxplot(data, vert=False, labels=labels, showfliers=False)
    plt.xlabel("Annualized offered wage (USD)")
    plt.title(title or "Wage distribution")
    plt.grid(axis="x", alpha=0.3)
    plt.tight_layout()
    plt.show()


def plot_treemap_from_series(series: pd.Series, top_n: int = 30, title: str | None = None, wrap: int = 16):
    """
    Treemap（矩形树图）：对类别型 Series 计数后按面积呈现。
    - 仅输入 Series（如 df['SOC_TITLE']）
    - 自动 dropna/strip/去空串
    - 默认显示 top_n，其他合并为 'Other'
    - 矩形标签显示 名称 + 计数 + 百分比
    返回：汇总 DataFrame（count, pct）
    """
    # 清理
    s = series.dropna().astype(str).str.strip()
    s = s[s != ""]
    if s.empty:
        raise ValueError("Series 为空（清理后无数据）。")

    # 统计 + top_n
    counts = s.value_counts()
    total = int(counts.sum())
    if top_n and top_n > 0 and len(counts) > top_n:
        top = counts.iloc[:top_n].copy()
        other = counts.iloc[top_n:].sum()
        if other > 0:
            top.loc["Other"] = other
    else:
        top = counts

    df = pd.DataFrame({"count": top.astype(int)})
    df["pct"] = (df["count"] / total * 100).round(1)

    # 标签（长名称自动换行）
    labels = [
        f"{fill(str(name), width=wrap)}\n{cnt:,} ({pct:.1f}%)"
        for name, cnt, pct in zip(df.index, df["count"], df["pct"])
    ]

    # 画图（单图，无指定配色）
    fig, ax = plt.subplots(figsize=(14, 6))
    squarify.plot(sizes=df["count"].tolist(), label=labels, pad=True, ax=ax)
    ax.axis("off")
    ax.set_title(title or f"Treemap — Top {len(df)}", pad=12)
    plt.tight_layout()
    plt.show()

    return df


# ----------------------------
# Specific analyses
# ----------------------------

def nonzero_share(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    """Compute non-zero counts and shares for a set of columns.

    The denominator is the total number of rows. Columns are coerced to numeric
    (invalid values become 0); a row is considered positive if the numeric value
    is strictly greater than 0.

    Parameters
    ----------
    df : pd.DataFrame
        Source table.
    columns : Iterable[str]
        Column names to evaluate.

    Returns
    -------
    pd.DataFrame
        Sorted table with columns:
        - ``variable`` (str)
        - ``nonzero_count`` (int)
        - ``total_rows`` (int)
        - ``pct_nonzero`` (float, percentage with one decimal)
    """
    dfx = standardize_columns(df)
    total = len(dfx)
    rows = []
    for c in columns:
        if c.upper() not in dfx.columns:
            continue
        s = pd.to_numeric(dfx[c.upper()], errors="coerce").fillna(0)
        cnt = int((s > 0).sum())
        pct = round(cnt / total * 100.0, 1) if total else np.nan
        rows.append((c.upper(), cnt, total, pct))
    res = pd.DataFrame(rows, columns=["variable", "nonzero_count", "total_rows", "pct_nonzero"])
    return res.sort_values("pct_nonzero", ascending=False).reset_index(drop=True)


def plot_nonzero_share_vertical(res: pd.DataFrame, title: str = "Share of Non-Zero Flags") -> None:
    """Plot a vertical bar chart from the ``nonzero_share`` result table.

    Parameters
    ----------
    res : pd.DataFrame
        Output of :func:`nonzero_share`.
    title : str, default "Share of Non-Zero Flags"
        Plot title.

    Returns
    -------
    None
        Shows the figure and returns ``None``.
    """
    labels = res["variable"].tolist()
    vals_pct = res["pct_nonzero"].to_numpy()
    vals_cnt = res["nonzero_count"].to_numpy()
    n = len(vals_pct)
    fig_w = min(max(8, 0.7 * n + 2), 18)
    fig, ax = plt.subplots(figsize=(fig_w, 6))
    x = np.arange(n)
    bars = ax.bar(x, vals_pct)
    ax.set_xticks(x, labels=labels, rotation=35, ha="right")
    ax.set_ylabel("% non-zero (of all rows)")
    ax.set_title(title)
    ymax = vals_pct.max() if n else 100.0
    ax.set_ylim(0, min(100.0, ymax * 1.15 + 5))
    ax.grid(axis="y", alpha=0.3)
    for xi, rect, p, cnt in zip(x, bars, vals_pct, vals_cnt):
        ax.text(rect.get_x() + rect.get_width()/2.0, rect.get_height() * 1.01, f"{cnt:,} ({p:.1f}%)",
                ha="center", va="bottom", fontsize=9)
    plt.tight_layout()
    plt.show()


def soc_median_tables(df: pd.DataFrame,
                      wage_col: str = "OFFERED_WAGE_ANNUAL_USD",
                      soc_col: str = "SOC_TITLE",
                      min_count: int = 50) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Compute top/bottom SOC titles by median annual wage after a size filter.

    Parameters
    ----------
    df : pd.DataFrame
        Source table.
    wage_col : str, default "OFFERED_WAGE_ANNUAL_USD"
        Column containing numeric annual wages.
    soc_col : str, default "SOC_TITLE"
        Column containing SOC titles (string names).
    min_count : int, default 50
        Minimum number of rows per SOC required to include in the ranking.

    Returns
    -------
    (pd.DataFrame, pd.DataFrame)
        A pair ``(top10, bottom10)`` tables, each with columns:
        - ``SOC_TITLE`` (str)
        - ``n`` (int, group size)
        - ``median_annual`` (float, group median annual wage)
    """
    dfx = standardize_columns(df)
    assert soc_col.upper() in dfx.columns, "SOC column not found"
    w = pd.to_numeric(dfx[wage_col.upper()], errors="coerce")
    mask = dfx[soc_col.upper()].notna() & (dfx[soc_col.upper()].astype(str).str.strip()!="") & w.notna()
    tmp = pd.DataFrame({"SOC_TITLE": dfx.loc[mask, soc_col.upper()].astype(str).str.strip(),
                        "WAGE_ANNUAL": w.loc[mask].astype(float)})
    agg = tmp.groupby("SOC_TITLE", as_index=False).agg(n=("WAGE_ANNUAL","size"),
                                                       median_annual=("WAGE_ANNUAL","median"))
    filt = agg[agg["n"] >= min_count].copy()
    top10 = filt.sort_values("median_annual", ascending=False).head(10).copy()
    bot10 = filt.sort_values("median_annual", ascending=True).head(10).copy()
    return top10, bot10


# ----------------------------
# Role filters & analyses
# ----------------------------

def masks_for_roles(df: pd.DataFrame) -> Dict[str, pd.Series]:
    """Return boolean masks for three common analytics roles.

    Rules (case-insensitive):
    - **Data Analyst**: ``JOB_TITLE`` contains any of
      ``"data analyst"``, ``"data analytics"``, ``"advanced analytics"``.
    - **Data Scientist**: ``JOB_TITLE`` contains ``"data science"`` or
      ``"data scientist"``, OR ``SOC_TITLE`` starts with ``"data scientist"``
      (singular or plural).
    - **Business Analyst**: ``SOC_TITLE`` starts with ``"business intelligence analyst"``
      (singular or plural) OR ``JOB_TITLE`` contains ``"business analyst"``.

    Parameters
    ----------
    df : pd.DataFrame
        Source table (columns are standardized to uppercase internally).

    Returns
    -------
    dict[str, pd.Series]
        Mapping role name -> boolean mask over the rows.
    """
    dfx = standardize_columns(df)
    job = dfx.get("JOB_TITLE", pd.Series("", index=dfx.index)).astype(str)
    soc = dfx.get("SOC_TITLE", pd.Series("", index=dfx.index)).astype(str)

    da = job.str.contains(r"(data analyst|data analytics|advanced analytics)", case=False, na=False)
    ds = job.str.contains(r"(data science|data scientist)", case=False, na=False) | \
         soc.str.contains(r"^data scientist", case=False, na=False)
    ba = soc.str.contains(r"^business intelligence analyst", case=False, na=False) | \
         job.str.contains(r"business analyst", case=False, na=False)

    return {"Data Analyst": da, "Data Scientist": ds, "Business Analyst": ba}


def role_wage_arrays(df: pd.DataFrame,
                     wage_col: str = "OFFERED_WAGE_ANNUAL_USD") -> Dict[str, np.ndarray]:
    """Return wage arrays by role suitable for plotting boxplots.

    Non-positive or missing wages are excluded.

    Parameters
    ----------
    df : pd.DataFrame
        Source table.
    wage_col : str, default "OFFERED_WAGE_ANNUAL_USD"
        Column with numeric annual wages.

    Returns
    -------
    dict[str, np.ndarray]
        Mapping role name -> numeric array of wages.
    """
    dfx = standardize_columns(df)
    w = pd.to_numeric(dfx.get(wage_col.upper(), pd.Series(np.nan, index=dfx.index)), errors="coerce")
    masks = masks_for_roles(dfx)
    out: Dict[str, np.ndarray] = {}
    for name, m in masks.items():
        arr = w[m].dropna()
        arr = arr[arr > 0]
        out[name] = arr.values
    return out


def role_deciles_table(df: pd.DataFrame,
                       wage_col: str = "OFFERED_WAGE_ANNUAL_USD",
                       step: float = 0.10) -> pd.DataFrame:
    """Compute a wide deciles table (columns = roles, index = 0..100 percentiles).

    Parameters
    ----------
    df : pd.DataFrame
        Source table.
    wage_col : str, default "OFFERED_WAGE_ANNUAL_USD"
        Column with numeric annual wages.
    step : float, default 0.10
        Quantile step; use 0.05 for ventiles, etc.

    Returns
    -------
    pd.DataFrame
        Wide table with index ``percentile`` (0..100) and one column per role.
    """
    arrays = role_wage_arrays(df, wage_col=wage_col)
    qs = np.arange(0, 1.0001, step)
    idx = (qs*100).astype(int)
    cols = {}
    for name, arr in arrays.items():
        if len(arr) == 0:
            cols[name] = pd.Series([np.nan]*len(idx), index=idx)
        else:
            s = pd.Series(arr)
            qv = s.quantile(qs).values
            cols[name] = pd.Series(qv, index=idx)
    tbl = pd.DataFrame(cols)
    tbl.index.name = "percentile"
    return tbl
