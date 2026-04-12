# sungwoo_mapper.py
# SUNGWOO HITECH – ESG metric extractor.
# Source: sungwoo_source.xlsx (LlamaParse output, page-named sheets)
# Years: 2022–2024.
# Note: H&S table (Page 10 - B) is transposed – years are rows, indicators are columns.

import re
import pandas as pd

COMPANY = "SUNGWOO"
YEARS = [2022, 2023, 2024]
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
    if x is None:
        return None
    s = _clean(x)
    if s == "" or s.lower() in {"na","n/a","-"}:
        return None
    s = s.replace(" ", "")
    # percentages like 70%
    if s.endswith("%"):
        try:
            return float(s[:-1].replace(",", "."))
        except Exception:
            return None
    # handle "1 463" thousands with spaces
    s = s.replace("\u00a0", "").replace(" ", "")
    # decimal comma
    if s.count(",") == 1 and s.count(".") == 0 and re.fullmatch(r"-?\d+,\d+", s):
        s = s.replace(",", ".")
    # remove thousand separators like 1,234.56
    if s.count(",") > 0 and s.count(".") > 0:
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

def _emit_result_row(excel_path: str, sheet: str, metric_code: str, unit: str):
    df = pd.read_excel(excel_path, sheet_name=sheet)
    label_col = df.columns[0]
    for _, r in df.iterrows():
        label = _clean(r.get(label_col)).upper()
        if label == "RESULT":
            for y in YEARS:
                col = y if y in df.columns else (str(y) if str(y) in df.columns else None)
                if col is None:
                    continue
                val = parse_num(r.get(col))
                if val is None:
                    continue
                yield [COMPANY, y, metric_code, val, unit, f"{sheet} RESULT", sheet]
            break

def extract_sungwoo_core(excel_path: str, dedup: bool = True) -> pd.DataFrame:
    out = []
    xls = pd.ExcelFile(excel_path)

    # Energy & Water
    if "Energy_consumption(MWh)" in xls.sheet_names:
        out += list(_emit_result_row(excel_path, "Energy_consumption(MWh)", "ENERGY_TOTAL", "MWh"))
    if "Total_water_consumption(m3)" in xls.sheet_names:
        out += list(_emit_result_row(excel_path, "Total_water_consumption(m3)", "WATER_TOTAL", "m3"))

    # Waste
    if "General_waste_production(t)" in xls.sheet_names:
        out += list(_emit_result_row(excel_path, "General_waste_production(t)", "WASTE_TOTAL", "t"))
    if "Separated waste production(t)" in xls.sheet_names:
        out += list(_emit_result_row(excel_path, "Separated waste production(t)", "WASTE_RECYCLED", "t"))
    if "Dangerous waste production(t)" in xls.sheet_names:
        out += list(_emit_result_row(excel_path, "Dangerous waste production(t)", "WASTE_HAZARDOUS", "t"))

    # Employees (counts) - Page 8 - D
    if "Page 8 - D" in xls.sheet_names:
        df = pd.read_excel(excel_path, sheet_name="Page 8 - D")
        metric_col = df.columns[0]
        year_cols = [c for c in df.columns[1:] if str(c).isdigit() and int(c) in YEARS]
        for _, r in df.iterrows():
            m = _clean(r.get(metric_col)).upper()
            if m not in {"EMPLOYEE QTY","MALE","FEMALE"}:
                continue
            code = {"EMPLOYEE QTY":"EMPLOYEES_TOTAL", "MALE":"EMPLOYEES_MALE", "FEMALE":"EMPLOYEES_FEMALE"}[m]
            for y in year_cols:
                val = parse_num(r.get(y))
                if val is None:
                    continue
                out.append([COMPANY, int(y), code, val, "Number", m, "Page 8 - D"])

    # GHG emissions - Page 7 - A
    if "Page 7 - A" in xls.sheet_names:
        df = pd.read_excel(excel_path, sheet_name="Page 7 - A")
        label_col = df.columns[0]  # YEAR/label
        year_cols = [c for c in df.columns[1:] if str(c).isdigit() and int(c) in YEARS]
        s1, s2 = {}, {}
        for _, r in df.iterrows():
            label = _clean(r.get(label_col)).upper()
            if label not in {"TOTAL","SCOPE 1","SCOPE 2","SCOPE 3"}:
                continue
            for y in year_cols:
                val = parse_num(r.get(y))
                if val is None:
                    continue
                yi = int(y)
                if label == "SCOPE 1":
                    out.append([COMPANY, yi, "GHG_SCOPE1", val, "t CO2e", "SCOPE 1", "Page 7 - A"])
                    s1[yi] = val
                elif label == "SCOPE 2":
                    out.append([COMPANY, yi, "GHG_SCOPE2", val, "t CO2e", "SCOPE 2", "Page 7 - A"])
                    s2[yi] = val
                elif label == "SCOPE 3":
                    out.append([COMPANY, yi, "GHG_SCOPE3", val, "t CO2e", "SCOPE 3", "Page 7 - A"])
                elif label == "TOTAL":
                    out.append([COMPANY, yi, "GHG_TOTAL_ALL", val, "t CO2e", "TOTAL (as reported)", "Page 7 - A"])

        # Derived comparable total
        for yi in YEARS:
            if yi in s1 and yi in s2:
                out.append([COMPANY, yi, "GHG_TOTAL", float(s1[yi] + s2[yi]), "t CO2e",
                            "Derived: GHG_SCOPE1 + GHG_SCOPE2", "Page 7 - A"])

    # H&S - Page 10 - B (transposed: years as rows, indicators as columns)
    if "Page 10 - B" in xls.sheet_names:
        df = pd.read_excel(excel_path, sheet_name="Page 10 - B")
        # first column contains year values
        year_col = df.columns[0]
        for _, r in df.iterrows():
            yi = parse_num(r.get(year_col))
            if yi is None:
                continue
            yi = int(yi)
            if yi not in YEARS:
                continue
            # nested helper: emit one metric from a column in this row
            def emit_metric(col_name, code, unit):
                if col_name in df.columns:
                    val = parse_num(r.get(col_name))
                    if val is not None:
                        out.append([COMPANY, yi, code, val, unit, col_name, "Page 10 - B"])
            emit_metric("Work injuries with absence [thead]", "HNS_WORK_INJURIES_WITH_ABSENCE", "Number")
            emit_metric("Missed hours due to injury [thead] ", "HNS_MISSED_HOURS_DUE_TO_INJURY", "Hours")
            emit_metric("Occupational disease [thead] ", "HNS_OCCUPATIONAL_DISEASE", "Number")
            emit_metric("Conducted training", "HNS_TRAINING_CONDUCTED", "Number")

    res = pd.DataFrame(out, columns=["Company","Year","MetricCode","Value","UnitRaw","MetricRaw","SourceSheet"])
    if dedup:
        res = deduplicate(res)
    return res
