# skoda_mapper.py
# ŠKODA Auto – ESG metric extractor.
# Source: skoda_source.xlsx (LlamaParse output, page-named sheets)
# Year: 2024 only (single-year snapshot in the source report).
# Note: “Total GHG emissions” in the report includes Scope 3 – not used here;
#       GHG_TOTAL is derived as Scope 1 + Scope 2 (market-based).

import re
import pandas as pd

COMPANY = "SKODA"
YEAR_DEFAULT = 2024
_SUPERSCRIPTS = "¹²³⁴⁵⁶⁷⁸⁹⁰"

def norm_text(x) -> str:
    if x is None:
        return ""
    s = str(x)
    s = s.replace("<br/>", " ").replace("<br />", " ")
    for ch in _SUPERSCRIPTS:
        s = s.replace(ch, "")
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

def parse_num(x):
    if x is None:
        return None
    s = str(x)
    for ch in _SUPERSCRIPTS:
        s = s.replace(ch, "")
    s = s.replace("–", "").replace("—", "").strip()
    if s == "" or s.lower() in {"na","n/a","-"}:
        return None
    if s.endswith("%"):
        try:
            return float(s[:-1].replace(",", "").strip())
        except Exception:
            return None
    s2 = s.replace(",", "").strip()
    try:
        return float(s2)
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

def extract_skoda_core(excel_path: str, dedup: bool = True) -> pd.DataFrame:
    out = []
    xls = pd.ExcelFile(excel_path)

    def scan_2col(sheet, unit, metric_col=0, value_col=1):
        df = pd.read_excel(excel_path, sheet_name=sheet)
        if df.shape[1] < 2:
            return
        c0 = df.columns[metric_col]
        c1 = df.columns[value_col]
        for _, r in df.iterrows():
            m = r.get(c0)
            v = r.get(c1)
            mt = norm_text(m)
            if not mt:
                continue
            val = parse_num(v)
            if val is None:
                continue

            # ENERGY
            if "total energy consumption related to own operations" in mt:
                out.append([COMPANY, YEAR_DEFAULT, "ENERGY_TOTAL", val, unit, str(m), sheet])
            elif "total energy consumption from renewable sources" in mt:
                out.append([COMPANY, YEAR_DEFAULT, "ENERGY_RENEWABLE", val, unit, str(m), sheet])

            # WATER (strict)
            elif "total water consumption within the production" in mt:
                out.append([COMPANY, YEAR_DEFAULT, "WATER_TOTAL", val, unit, str(m), sheet])

            # WASTE (strict)
            elif "the total amount of waste generated" in mt:
                out.append([COMPANY, YEAR_DEFAULT, "WASTE_TOTAL", val, unit, str(m), sheet])

            # GHG
            elif mt.startswith("scope 1 emissions"):
                out.append([COMPANY, YEAR_DEFAULT, "GHG_SCOPE1", val, unit, str(m), sheet])
            elif mt.startswith("scope 2 emissions") and "market" in mt:
                out.append([COMPANY, YEAR_DEFAULT, "GHG_SCOPE2", val, unit, str(m), sheet])
            elif mt.startswith("scope 3") and "upstream" in mt:
                out.append([COMPANY, YEAR_DEFAULT, "GHG_SCOPE3_UPSTREAM", val, unit, str(m), sheet])
            elif mt.startswith("scope 3") and "downstream" in mt:
                out.append([COMPANY, YEAR_DEFAULT, "GHG_SCOPE3_DOWNSTREAM", val, unit, str(m), sheet])

            # Total GHG emissions as reported (often includes Scope 3) -> store separately
            elif mt.startswith("total ghg emissions") and ("intensity" not in mt) and ("specific" not in mt) and ("/" not in mt):
                out.append([COMPANY, YEAR_DEFAULT, "GHG_TOTAL_ALL", val, unit, str(m), sheet])

            # PEOPLE (if present in this sheet)
            elif mt == "number of employees":
                out.append([COMPANY, YEAR_DEFAULT, "EMPLOYEES_TOTAL", val, "Number", str(m), sheet])
            elif mt == "female employees":
                out.append([COMPANY, YEAR_DEFAULT, "EMPLOYEES_FEMALE", val, "Number", str(m), sheet])
            elif mt == "male employees":
                out.append([COMPANY, YEAR_DEFAULT, "EMPLOYEES_MALE", val, "Number", str(m), sheet])

    # Page 62 blocks
    if "Page 62 - A" in xls.sheet_names:
        scan_2col("Page 62 - A", "MWh")
    if "Page 62 - E" in xls.sheet_names:
        scan_2col("Page 62 - E", "m3")
    if "Page 62 - F" in xls.sheet_names:
        scan_2col("Page 62 - F", "t")
    if "Page 62 - C" in xls.sheet_names:
        scan_2col("Page 62 - C", "t CO2e")

    # Workforce table: Page 63 - A (first two columns name/value)
    if "Page 63 - A" in xls.sheet_names:
        df = pd.read_excel(excel_path, sheet_name="Page 63 - A")
        if df.shape[1] >= 2:
            c0, c1 = df.columns[0], df.columns[1]
            for _, r in df.iterrows():
                m = r.get(c0)
                v = r.get(c1)
                mt = norm_text(m)
                if not mt:
                    continue
                val = parse_num(v)
                if val is None:
                    continue
                if mt == "number of employees":
                    out.append([COMPANY, YEAR_DEFAULT, "EMPLOYEES_TOTAL", val, "Number", str(m), "Page 63 - A"])
                elif mt == "female employees":
                    out.append([COMPANY, YEAR_DEFAULT, "EMPLOYEES_FEMALE", val, "Number", str(m), "Page 63 - A"])
                elif mt == "male employees":
                    out.append([COMPANY, YEAR_DEFAULT, "EMPLOYEES_MALE", val, "Number", str(m), "Page 63 - A"])

    res = pd.DataFrame(out, columns=["Company","Year","MetricCode","Value","UnitRaw","MetricRaw","SourceSheet"])

    # Derived GHG_TOTAL = scope1 + scope2
    if not res.empty and (res["MetricCode"]=="GHG_SCOPE1").any() and (res["MetricCode"]=="GHG_SCOPE2").any():
        s1 = float(res.loc[res["MetricCode"]=="GHG_SCOPE1","Value"].iloc[0])
        s2 = float(res.loc[res["MetricCode"]=="GHG_SCOPE2","Value"].iloc[0])
        res = pd.concat([res, pd.DataFrame([[COMPANY, YEAR_DEFAULT, "GHG_TOTAL", s1+s2, "t CO2e",
                                             "Derived: GHG_SCOPE1 + GHG_SCOPE2 (market-based)", "Page 62 - C"]],
                                           columns=res.columns)], ignore_index=True)

    if dedup:
        res = deduplicate(res)

    return res
