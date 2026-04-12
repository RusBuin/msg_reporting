# audi_mapper.py
# AUDI AG – rules/keywords-based extractor for CORE ESG metrics.
#
# Extraction rules applied:
# - ENERGY_TOTAL : absolute total only, "specific" rows excluded
# - GHG_TOTAL    : Scope 1 + 2 combined, "specific" rows excluded
# - WASTE_TOTAL  : strictly "Total amount of waste" row only
# - WATER_TOTAL, WASTE_RECYCLED, WASTE_DISPOSAL, GHG_SCOPE1 also extracted
#
# Note: WASTE_TOTAL for 2022/2023 may be absent if the source Excel does not
# contain a "Total amount of waste" row for those years.

import re
import pandas as pd

COMPANY = "AUDI"
_SUPERSCRIPTS = "¹²³⁴⁵⁶⁷⁸⁹⁰"

def norm_text(x) -> str:
    if x is None:
        return ""
    s = str(x)
    s = s.replace("<br/>", " ").replace("<br />", " ")
    s = s.replace("✓", " ")
    for ch in _SUPERSCRIPTS:
        s = s.replace(ch, "")
    s = s.replace("&", " and ")
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

def parse_num(x):
    if x is None:
        return None
    s = str(x)
    for ch in _SUPERSCRIPTS:
        s = s.replace(ch, "")
    s = s.replace("✓", "").replace("–", "").replace("—", "").strip()
    s = re.sub(r"\s+", "", s)
    if s == "" or s.lower() in {"na", "n/a", "-"}:
        return None
    try:
        if s.count(",") > 0 and s.count(".") == 0:
            s = s.replace(",", "")
        elif s.count(",") > 0 and s.count(".") > 0:
            s = s.replace(",", "")
        return float(s)
    except Exception:
        return None

RULES = [
    # ---- GHG ----
    {
        "code": "GHG_SCOPE1",
        "include_any": ["ghg", "greenhouse gas"],
        "include_all": ["scope", "1"],
        "exclude_any": ["specific", "scope 2", "scope 3", "scope 1 and 2", "scope 1+2"],
        "priority": 130,
    },
    {
        "code": "GHG_TOTAL",
        "include_any": ["total ghg emissions", "ghg emissions", "greenhouse gas emissions"],
        "include_all": ["scope", "1"],
        "also_require_any": ["and 2", "1 and 2", "1+2", "scope 2"],
        "exclude_any": ["specific", "scope 3"],
        "priority": 120,
    },

    # ---- Energy ----
    {
        "code": "ENERGY_RENEWABLE",
        "include_any": ["of which from renewable", "renewable energy sources"],
        # keep ONLY absolute values
        "exclude_any": ["mwh/veh", "per vehicle", "specific"],
        "priority": 110,
    },
    {
        "code": "ENERGY_TOTAL",
        "include_all": ["total", "energy", "consumption"],
        "exclude_any": ["specific", "mwh/veh", "per vehicle", "of which"],
        "priority": 105,
    },

    # ---- Water ----
    {
        "code": "WATER_TOTAL",
        "include_all": ["total", "water", "consumption"],
        "exclude_any": ["m3/veh", "per vehicle", "specific"],
        "priority": 100,
    },

    # ---- Waste ----
    # Strict: ONLY "Total amount of waste"
    {
        "code": "WASTE_TOTAL",
        "include_all": ["total", "amount", "of", "waste"],
        "exclude_any": ["production-specific", "specific", "kg/veh", "t/veh", "per vehicle"],
        "priority": 120,
    },
    {
        "code": "WASTE_RECYCLED",
        "include_any": ["total amount of recycled waste"],
        "exclude_any": ["kg/veh", "t/veh", "per vehicle", "specific"],
        "priority": 110,
    },
    {
        "code": "WASTE_DISPOSAL",
        "include_any": ["total disposable waste"],
        "exclude_any": ["kg/veh", "t/veh", "per vehicle", "specific"],
        "priority": 100,
    },
    # If you still want a fallback disposal line item (not total): keep but low priority and avoid mixing scopes
    # Disabled by default (commented): "Disposable waste" from Page 106-A often differs from production-specific totals.

    # ---- People ----
    {
        "code": "EMPLOYEES_FTE",
        "include_any": ["number of full-time employees", "full-time employees"],
        "exclude_any": [],
        "priority": 90,
    },
    {
        "code": "EMPLOYEES_FEMALE",
        "include_any": ["female employees"],
        "exclude_any": [],
        "priority": 85,
    },
    {
        "code": "EMPLOYEES_MALE",
        "include_any": ["male employees"],
        "exclude_any": [],
        "priority": 85,
    },

    # ---- H&S ----
    {
        "code": "HNS_TRIR",
        "include_any": ["trir", "rate of work-related accidents"],
        "exclude_any": [],
        "priority": 90,
    },

    # ---- Optional turnover ----
    {
        "code": "EMP_TURNOVER",
        "include_any": ["turnover rate", "turnover"],
        "exclude_any": ["revenue"],
        "priority": 20,
    },
]

def _rule_match(rule: dict, text: str) -> bool:
    inc_any = rule.get("include_any", [])
    if inc_any and not any(p in text for p in inc_any):
        return False
    inc_all = rule.get("include_all", [])
    if inc_all and not all(p in text for p in inc_all):
        return False
    ara = rule.get("also_require_any", [])
    if ara and not any(p in text for p in ara):
        return False
    exc = rule.get("exclude_any", [])
    if exc and any(p in text for p in exc):
        return False
    return True

def match_metric(metric_raw: str, unit_raw: str = "", sheet_name: str = ""):
    blob = " ".join([norm_text(metric_raw), norm_text(unit_raw), norm_text(sheet_name)]).strip()
    candidates = [r for r in RULES if _rule_match(r, blob)]
    if not candidates:
        return None
    candidates.sort(key=lambda x: x.get("priority", 0), reverse=True)
    return candidates[0]["code"]

def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    d = df.copy()
    d["_abs"] = d["Value"].abs()
    d["_isnull"] = d["Value"].isna().astype(int)
    d = d.sort_values(["Company", "Year", "MetricCode", "_isnull", "_abs"],
                      ascending=[True, True, True, True, False])
    d = d.drop_duplicates(subset=["Company", "Year", "MetricCode"], keep="first")
    return d.drop(columns=["_abs", "_isnull"])

def extract_audi_core(excel_path: str, dedup: bool = True) -> pd.DataFrame:
    out = []
    xls = pd.ExcelFile(excel_path)

    def emit_wide(df, metric_col, unit_col, sheet):
        year_cols = [c for c in df.columns if str(c).isdigit()]
        for _, r in df.iterrows():
            metric = r.get(metric_col)
            unit = r.get(unit_col) if unit_col is not None else ""
            code = match_metric(metric, unit, sheet)
            if not code:
                continue
            for y in year_cols:
                val = parse_num(r.get(y))
                if val is None:
                    continue
                out.append([COMPANY, int(y), code, val, str(unit), str(metric), sheet])

    # Scan relevant sheets
    for sheet in ["Page 77 - A","Page 80 - A","Page 106 - B","Page 107 - A","Page 113 - A","Page 114 - A","Page 118 - A"]:
        if sheet not in xls.sheet_names:
            continue
        df = pd.read_excel(excel_path, sheet_name=sheet)

        if sheet in {"Page 77 - A","Page 80 - A","Page 106 - B","Page 107 - A"}:
            metric_col = "Unnamed: 0" if "Unnamed: 0" in df.columns else df.columns[0]
        elif sheet == "Page 118 - A":
            metric_col = "Key figures, Audi Group" if "Key figures, Audi Group" in df.columns else df.columns[0]
        else:
            metric_col = df.columns[0]

        unit_col = "Unit" if "Unit" in df.columns else None
        emit_wide(df, metric_col, unit_col, sheet)

    # Headerless sheet (Page 106 - A): keep scanned only for other waste details if needed later
    # For now, we still scan it, but strict rules prevent "Disposable waste" from being mapped.
    if "Page 106 - A" in xls.sheet_names:
        df = pd.read_excel(excel_path, sheet_name="Page 106 - A", header=None)
        for _, r in df.iterrows():
            metric = r.get(0)
            unit = r.get(1)
            code = match_metric(metric, unit, "Page 106 - A")
            if not code:
                continue
            for y, col in [(2024, 2), (2023, 3), (2022, 4)]:
                val = parse_num(r.get(col))
                if val is None:
                    continue
                out.append([COMPANY, y, code, val, str(unit), str(metric), "Page 106 - A"])

    res = pd.DataFrame(out, columns=["Company", "Year", "MetricCode", "Value", "UnitRaw", "MetricRaw", "SourceSheet"])
    if dedup:
        res = deduplicate(res)
    return res
