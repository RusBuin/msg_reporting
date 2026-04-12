# iljin_mapper.py
# ILJIN Slovakia – ESG metric extractor.
# Source: iljin_source.xlsx (LlamaParse output, page-named sheets)
# Years: 2022–2024.
# Note: GHG values in the source are in million metric tons – converted to tons on extraction.

import re
import pandas as pd

COMPANY = "ILJIN"
_SUPERSCRIPTS = "¹²³⁴⁵⁶⁷⁸⁹⁰"

def _clean(x) -> str:
    if x is None:
        return ""
    s = str(x)
    for ch in _SUPERSCRIPTS:
        s = s.replace(ch, "")
    s = s.replace("✓", " ").replace("–", " ").replace("—", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def parse_num(x):
    """Parses numbers with either decimal comma (e.g., 3558,047) or thousand commas."""
    if x is None:
        return None
    s = _clean(x)
    if s == "" or s.lower() in {"na","n/a","-"}:
        return None
    s = s.replace(" ", "")
    # percent in string
    if s.endswith("%"):
        s = s[:-1]
    # decimal comma like 3558,047 or 4,19
    if re.fullmatch(r"-?\d+,\d+", s) and s.count(".")==0:
        s = s.replace(",", ".")
    # thousands: 1,234.56 or 1,234
    elif s.count(",") > 0 and s.count(".") > 0:
        s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return None

def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    d = df.copy()
    d["_abs"] = d["Value"].abs()
    d["_isnull"] = d["Value"].isna().astype(int)
    d = d.sort_values(["Company","Year","MetricCode","_isnull","_abs"],
                      ascending=[True,True,True,True,False])
    d = d.drop_duplicates(subset=["Company","Year","MetricCode"], keep="first")
    return d.drop(columns=["_abs","_isnull"])

def extract_iljin_core(excel_path: str, dedup: bool = True) -> pd.DataFrame:
    out = []
    xls = pd.ExcelFile(excel_path)

    # Energy_usage (wide)
    if "Energy_usage" in xls.sheet_names:
        df = pd.read_excel(excel_path, sheet_name="Energy_usage")
        for _, r in df.iterrows():
            cat = _clean(r.get("Category")).lower()
            if "energy usage" in cat and "electricity" in cat:
                for y in [2022, 2023, 2024]:
                    col = y if y in df.columns else (str(y) if str(y) in df.columns else None)
                    if col is None:
                        continue
                    val = parse_num(r.get(col))
                    if val is None:
                        continue
                    out.append([COMPANY, y, "ENERGY_TOTAL", val, "MWh", r.get("Category"), "Energy_usage"])

    # Water usage
    if "Page 69 - B" in xls.sheet_names:
        df = pd.read_excel(excel_path, sheet_name="Page 69 - B")
        for _, r in df.iterrows():
            cat = _clean(r.get("Category")).lower()
            if "water usage" in cat:
                for y in [2022, 2023, 2024]:
                    col = y if y in df.columns else (str(y) if str(y) in df.columns else None)
                    if col is None:
                        continue
                    val = parse_num(r.get(col))
                    if val is None:
                        continue
                    out.append([COMPANY, y, "WATER_TOTAL", val, "m3", r.get("Category"), "Page 69 - B"])

    # CO2 emissions (million metric tons -> tons)
    if "Page 49 - B" in xls.sheet_names:
        df = pd.read_excel(excel_path, sheet_name="Page 49 - B")
        metric_col = df.columns[0]
        year_col = 2024 if 2024 in df.columns else df.columns[1]
        for _, r in df.iterrows():
            m = _clean(r.get(metric_col))
            mt = m.lower()
            val_million = parse_num(r.get(year_col))
            if val_million is None:
                continue
            val_tons = val_million * 1_000_000  # million metric tons -> tons
            if "direct co2 emissions" in mt and "scope 1" in mt:
                out.append([COMPANY, 2024, "GHG_SCOPE1", val_tons, "t CO2e", m, "Page 49 - B"])
            elif "indirect co2 emissions" in mt and "scope 2" in mt:
                out.append([COMPANY, 2024, "GHG_SCOPE2", val_tons, "t CO2e", m, "Page 49 - B"])
            elif "total own co2 emissions" in mt and "scopes 1 and 2" in mt:
                out.append([COMPANY, 2024, "GHG_TOTAL", val_tons, "t CO2e", m, "Page 49 - B"])

    # Employees (Page 69 - F)
    if "Page 69 - F" in xls.sheet_names:
        df = pd.read_excel(excel_path, sheet_name="Page 69 - F")
        # Category column uses merged cells -> forward fill to propagate group labels
        if "Category" in df.columns:
            df["Category"] = df["Category"].ffill()
        for _, r in df.iterrows():
            cat = _clean(r.get("Category")).lower()
            sub = _clean(r.get("Unnamed: 1")).lower()
            if cat == "employee type" and sub == "total":
                for y in [2022, 2023, 2024]:
                    col = y if y in df.columns else (str(y) if str(y) in df.columns else None)
                    if col is None:
                        continue
                    val = parse_num(r.get(col))
                    if val is None:
                        continue
                    out.append([COMPANY, y, "EMPLOYEES_TOTAL", val, "Number", "Employee Type - Total", "Page 69 - F"])
            if cat == "gender" and sub == "male":
                for y in [2022, 2023, 2024]:
                    col = y if y in df.columns else (str(y) if str(y) in df.columns else None)
                    if col is None:
                        continue
                    val = parse_num(r.get(col))
                    if val is None:
                        continue
                    out.append([COMPANY, y, "EMPLOYEES_MALE", val, "Number", "Gender - Male", "Page 69 - F"])
            if cat == "gender" and sub == "female":
                for y in [2022, 2023, 2024]:
                    col = y if y in df.columns else (str(y) if str(y) in df.columns else None)
                    if col is None:
                        continue
                    val = parse_num(r.get(col))
                    if val is None:
                        continue
                    out.append([COMPANY, y, "EMPLOYEES_FEMALE", val, "Number", "Gender - Female", "Page 69 - F"])

    # Optional: Sickness ratio (percent)
    if "Page 69 - G" in xls.sheet_names:
        df = pd.read_excel(excel_path, sheet_name="Page 69 - G")
        for _, r in df.iterrows():
            cat = _clean(r.get("Category")).lower()
            if "sickness ratio" in cat:
                for y in [2023, 2024]:
                    col = y if y in df.columns else (str(y) if str(y) in df.columns else None)
                    if col is None:
                        continue
                    val = parse_num(r.get(col))
                    if val is None:
                        continue
                    out.append([COMPANY, y, "SICKNESS_RATIO", val, "%", r.get("Category"), "Page 69 - G"])

    # Optional: Fluctuation ratio (percent)
    if "Page 69 - H" in xls.sheet_names:
        df = pd.read_excel(excel_path, sheet_name="Page 69 - H")
        for _, r in df.iterrows():
            cat = _clean(r.get("Category")).lower()
            if "fluctuation ratio" in cat:
                for y in [2022, 2023, 2024]:
                    col = y if y in df.columns else (str(y) if str(y) in df.columns else None)
                    if col is None:
                        continue
                    val = parse_num(r.get(col))
                    if val is None:
                        continue
                    out.append([COMPANY, y, "FLUCTUATION_RATIO", val, "%", r.get("Category"), "Page 69 - H"])

    # Optional: Drawdown (number)
    if "Page 69 - I" in xls.sheet_names:
        df = pd.read_excel(excel_path, sheet_name="Page 69 - I")
        # Category is merged cell -> forward fill
        if "Category" in df.columns:
            df["Category"] = df["Category"].ffill()
        for _, r in df.iterrows():
            cat = _clean(r.get("Category")).lower()
            sub = _clean(r.get("Unnamed: 1")).lower()
            if "drawdown" in cat:
                code = "DRAWDOWN_MALE" if sub == "male" else ("DRAWDOWN_FEMALE" if sub == "female" else None)
                if not code:
                    continue
                for y in [2022, 2023, 2024]:
                    col = y if y in df.columns else (str(y) if str(y) in df.columns else None)
                    if col is None:
                        continue
                    val = parse_num(r.get(col))
                    if val is None:
                        continue
                    out.append([COMPANY, y, code, val, "Number", f"Drawdown - {sub.title()}", "Page 69 - I"])

    res = pd.DataFrame(out, columns=["Company","Year","MetricCode","Value","UnitRaw","MetricRaw","SourceSheet"])
    if dedup:
        res = deduplicate(res)
    return res
