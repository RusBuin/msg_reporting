# hmc_mapper_core_v2.py
# HMC (Hyundai Motor Company) - CORE metric extractor from hmc.xlsx (v2 aligned to canonical naming)
#
# Output columns:
# Company, Year, MetricCode, Value, UnitRaw, MetricRaw, SourceSheet
#
# Canonical metrics extracted:
# - ENERGY_TOTAL (MWh)               : Page 115 - A -> "Energy Consumption Total"
# - GHG_SCOPE1 (tCO2-eq)             : Page 115 - B -> "Scope 1"
# - GHG_SCOPE2 (tCO2-eq)             : Page 115 - B -> "Scope 2"
# - GHG_SCOPE3 (tCO2-eq)             : Page 115 - B -> "Scope 3"  (note: HMC doesn't split upstream/downstream here)
# - GHG_TOTAL (tCO2-eq)              : Page 115 - B -> "Sum of Scope 1 and 2" (preferred)
#   (fallback if missing: derived Scope1+Scope2)
# - GHG_TOTAL_ALL (tCO2-eq)          : derived Scope1+Scope2+Scope3 (HMC table doesn't have explicit "Total GHG emissions")
# - WATER_TOTAL (Ton)                : Page 116 - B -> "Water consumption" (exclude intensity)
# - WASTE_TOTAL (Ton)                : Page 116 - D -> "Total" (exclude intensity)
# - WASTE_RECYCLED (Ton)             : Page 116 - D -> "Amount of waste recycling"
# - EMPLOYEES_TOTAL / FEMALE / MALE  : Page 119 - A (employee section, not executives)
# - HNS_TRIR                          : Page 124 - A -> "Employee TRIR"
#
# Policy:
# - Extract years 2022-2024 only.
# - Exclude intensity/specific metrics (per vehicle).
# - Keep UnitRaw from sheet.

import re
import pandas as pd

COMPANY = "HMC"
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

def norm(x) -> str:
    return _clean(x).lower()

def parse_num(x):
    if x is None:
        return None
    s = _clean(x)
    if s == "" or s.lower() in {"na","n/a","-"}:
        return None
    s = re.sub(r"\)", "", s)  # remove footnote ')'
    s = s.replace(" ", "").replace("\u00a0", "")
    # thousand separators
    if s.count(",") > 0 and s.count(".") == 0:
        s = s.replace(",", "")
    elif s.count(",") > 0 and s.count(".") > 0:
        s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return None

def _year_cols(df: pd.DataFrame):
    cols = {}
    for c in df.columns:
        m = re.search(r"(20\d{2})", _clean(c))
        if m:
            y = int(m.group(1))
            if y in YEARS:
                cols[y] = c
    return cols

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

def extract_hmc_core(excel_path: str, dedup: bool = True) -> pd.DataFrame:
    out = []
    xls = pd.ExcelFile(excel_path)

    def emit_exact(sheet, match_fn, metric_code, unit_override=None):
        df = pd.read_excel(excel_path, sheet_name=sheet)
        years = _year_cols(df)
        for _, r in df.iterrows():
            cls = norm(r.get("Classification"))
            if not cls:
                continue
            if not match_fn(cls):
                continue
            unit = _clean(r.get("Unit"))
            if unit_override:
                unit = unit_override
            for y, col in years.items():
                val = parse_num(r.get(col))
                if val is None:
                    continue
                out.append([COMPANY, y, metric_code, val, unit, _clean(r.get("Classification")), sheet])

    # ENERGY_TOTAL
    if "Page 115 - A" in xls.sheet_names:
        emit_exact("Page 115 - A",
                   lambda cls: cls == "energy consumption total",
                   "ENERGY_TOTAL")

    # GHG
    s1, s2, s3 = {}, {}, {}
    if "Page 115 - B" in xls.sheet_names:
        df = pd.read_excel(excel_path, sheet_name="Page 115 - B")
        years = _year_cols(df)
        for _, r in df.iterrows():
            cls = norm(r.get("Classification"))
            unit = _clean(r.get("Unit"))
            if not cls:
                continue
            # exclude intensity row
            if "intensity" in cls or "per" in unit.lower():
                continue
            if cls == "scope 1":
                for y, col in years.items():
                    val = parse_num(r.get(col))
                    if val is None: continue
                    out.append([COMPANY, y, "GHG_SCOPE1", val, unit, "Scope 1", "Page 115 - B"])
                    s1[y] = val
            elif cls == "scope 2":
                for y, col in years.items():
                    val = parse_num(r.get(col))
                    if val is None: continue
                    out.append([COMPANY, y, "GHG_SCOPE2", val, unit, "Scope 2", "Page 115 - B"])
                    s2[y] = val
            elif cls == "scope 3":
                for y, col in years.items():
                    val = parse_num(r.get(col))
                    if val is None: continue
                    out.append([COMPANY, y, "GHG_SCOPE3", val, unit, "Scope 3", "Page 115 - B"])
                    s3[y] = val
            elif "sum of scope 1 and 2" in cls:
                for y, col in years.items():
                    val = parse_num(r.get(col))
                    if val is None: continue
                    out.append([COMPANY, y, "GHG_TOTAL", val, unit, "Sum of Scope 1 and 2", "Page 115 - B"])

        # fallback derive GHG_TOTAL if missing
        for y in years.keys():
            if y not in YEARS:
                continue
            if not any(row[1]==y and row[2]=="GHG_TOTAL" for row in out):
                if y in s1 and y in s2:
                    out.append([COMPANY, y, "GHG_TOTAL", float(s1[y]+s2[y]), "tCO2-eq",
                                "Derived: GHG_SCOPE1 + GHG_SCOPE2", "Page 115 - B"])

        # GHG_TOTAL_ALL derived = scope1+2+3
        for y in years.keys():
            if y in s1 and y in s2 and y in s3:
                out.append([COMPANY, y, "GHG_TOTAL_ALL", float(s1[y]+s2[y]+s3[y]), "tCO2-eq",
                            "Derived: Scope1 + Scope2 + Scope3", "Page 115 - B"])

    # WATER_TOTAL (exclude intensity, recycled ratio)
    if "Page 116 - B" in xls.sheet_names:
        emit_exact("Page 116 - B",
                   lambda cls: cls == "water consumption",
                   "WATER_TOTAL")

    # WASTE_TOTAL / WASTE_RECYCLED (exclude intensity, rate)
    if "Page 116 - D" in xls.sheet_names:
        emit_exact("Page 116 - D",
                   lambda cls: cls == "total",
                   "WASTE_TOTAL")
        emit_exact("Page 116 - D",
                   lambda cls: cls == "amount of waste recycling",
                   "WASTE_RECYCLED")

    # Employees: pick employee section total (>10000) + female/male employees
    if "Page 119 - A" in xls.sheet_names:
        df = pd.read_excel(excel_path, sheet_name="Page 119 - A")
        years = _year_cols(df)
        for _, r in df.iterrows():
            cls = norm(r.get("Classification"))
            unit = _clean(r.get("Unit"))
            if unit.lower() != "person":
                continue
            if cls == "total":
                for y, col in years.items():
                    val = parse_num(r.get(col))
                    if val is None:
                        continue
                    if val >= 10000:  # employees section
                        out.append([COMPANY, y, "EMPLOYEES_TOTAL", val, "Person", "Total employees", "Page 119 - A"])
            elif cls == "female employees":
                for y, col in years.items():
                    val = parse_num(r.get(col))
                    if val is not None:
                        out.append([COMPANY, y, "EMPLOYEES_FEMALE", val, "Person", "Female employees", "Page 119 - A"])
            elif cls == "male employees":
                for y, col in years.items():
                    val = parse_num(r.get(col))
                    if val is not None:
                        out.append([COMPANY, y, "EMPLOYEES_MALE", val, "Person", "Male employees", "Page 119 - A"])

    # TRIR (Employee TRIR)
    if "Page 124 - A" in xls.sheet_names:
        emit_exact("Page 124 - A",
                   lambda cls: cls == "employee trir",
                   "HNS_TRIR")

    res = pd.DataFrame(out, columns=["Company","Year","MetricCode","Value","UnitRaw","MetricRaw","SourceSheet"])
    if dedup:
        res = deduplicate(res)
    return res
