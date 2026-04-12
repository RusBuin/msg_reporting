"""
run_all.py
----------
Combines per-company ESG core files into a single fact_esg_core.csv for Power BI.

Modes:
  combine (default) – reads *_core.xlsx files and merges them
  extract           – runs company mappers from LlamaParse source Excel files

Usage:
  python run_all.py
  python run_all.py --mode extract
  python run_all.py --mode extract --skip ILJIN,SKODA
  python run_all.py --out path/to/output.csv
"""

import argparse
import glob
import os
import sys
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ---------------------------------------------------------------------------
# Company registry (used in extract mode)
# ---------------------------------------------------------------------------
PROCESSED_DIR = os.path.join("data", "processed")
SOURCE_DIR    = os.path.join("data", "source")

COMPANIES = {
    "AUDI":    {"core": os.path.join(PROCESSED_DIR, "audi_core.xlsx"),    "source_default": os.path.join(SOURCE_DIR, "audi_source.xlsx")},
    "HMC":     {"core": os.path.join(PROCESSED_DIR, "hmc_core.xlsx"),     "source_default": os.path.join(SOURCE_DIR, "hmc_source.xlsx")},
    "ILJIN":   {"core": os.path.join(PROCESSED_DIR, "iljin_core.xlsx"),   "source_default": os.path.join(SOURCE_DIR, "iljin_source.xlsx")},
    "SKODA":   {"core": os.path.join(PROCESSED_DIR, "skoda_core.xlsx"),   "source_default": os.path.join(SOURCE_DIR, "skoda_source.xlsx")},
    "SUNGWOO": {"core": os.path.join(PROCESSED_DIR, "sungwoo_core.xlsx"), "source_default": os.path.join(SOURCE_DIR, "sungwoo_source.xlsx")},
}

DEFAULT_OUT = os.path.join("ESG_PowerBI", "drop", "fact_esg_core.csv")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _pillar(code: str) -> str:
    """Map a MetricCode to an ESG pillar letter (E / S / G)."""
    if code.startswith(("ENERGY_", "GHG_", "WATER_", "WASTE_")):
        return "E"
    if code.startswith(("EMPLOYEES_", "HNS_", "EMP_", "SICKNESS_",
                         "FLUCTUATION_", "DRAWDOWN_")):
        return "S"
    return "G"


def read_core_file(path: str) -> pd.DataFrame:
    """Read a single *_core.xlsx file into a normalised DataFrame."""
    df = pd.read_excel(path)
    df["Pillar"] = df["MetricCode"].apply(_pillar)
    return df


def dedup_fact(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate (Company, Year, MetricCode) rows, keeping the largest value."""
    if df.empty:
        return df
    d = df.copy()
    d["_abs"] = d["Value"].abs()
    d["_isnull"] = d["Value"].isna().astype(int)
    d = d.sort_values(
        ["Company", "Year", "MetricCode", "_isnull", "_abs"],
        ascending=[True, True, True, True, False],
    )
    d = d.drop_duplicates(subset=["Company", "Year", "MetricCode"], keep="first")
    return d.drop(columns=["_abs", "_isnull"])


# ---------------------------------------------------------------------------
# Mode 1: combine pre-processed *_core.xlsx files
# ---------------------------------------------------------------------------
def run_combine(skip: set, out_path: str) -> pd.DataFrame:
    """Auto-detect *_core.xlsx files in data/processed/ and merge them into one fact table."""
    core_files = sorted(glob.glob(os.path.join(PROCESSED_DIR, "*_core.xlsx")))
    if not core_files:
        print("  No *_core.xlsx files found in the current directory.")
        return pd.DataFrame()

    frames = []

    for f in core_files:
        company_name = os.path.splitext(os.path.basename(f))[0].replace("_core", "").upper()
        if company_name in skip:
            print(f"  [SKIP] {company_name}")
            continue
        print(f"  [READ] {company_name} <- {f}")
        try:
            df = read_core_file(f)
            frames.append(df)
            print(f"         {len(df)} rows")
        except Exception as e:
            print(f"  [ERROR] {f}: {e}")

    return _save(frames, out_path)


# ---------------------------------------------------------------------------
# Mode 2: extract from source Excel files (full mapper run)
# ---------------------------------------------------------------------------
def run_extract(source_paths: dict, skip: set, out_path: str) -> pd.DataFrame:
    """Run company mappers against original LlamaParse-structured Excel files."""
    from mappers.audi_mapper import extract_audi_core
    from mappers.hmc_mapper import extract_hmc_core
    from mappers.iljin_mapper import extract_iljin_core
    from mappers.skoda_mapper import extract_skoda_core
    from mappers.sungwoo_mapper import extract_sungwoo_core

    extractors = {
        "AUDI":    extract_audi_core,
        "HMC":     extract_hmc_core,
        "ILJIN":   extract_iljin_core,
        "SKODA":   extract_skoda_core,
        "SUNGWOO": extract_sungwoo_core,
    }

    frames = []

    for company, fn in extractors.items():
        if company in skip:
            print(f"  [SKIP] {company}")
            continue

        path = source_paths.get(company, COMPANIES[company]["source_default"])
        if not os.path.isfile(path):
            print(f"  [WARN] {company}: source file not found -> {path}  (skipping)")
            continue

        print(f"  [RUN]  {company} <- {path}")
        try:
            df = fn(path)
            df["Pillar"] = df["MetricCode"].apply(_pillar)

            # Save per-company core file
            core_out = COMPANIES[company]["core"]
            df.to_excel(core_out, index=False)
            print(f"         {len(df)} rows  |  saved -> {core_out}")

            frames.append(df)
        except Exception as e:
            print(f"  [ERROR] {company}: {e}")

    return _save(frames, out_path)


# ---------------------------------------------------------------------------
# Shared: dedup, sort, export
# ---------------------------------------------------------------------------
def _save(frames: list, out_path: str) -> pd.DataFrame:
    if not frames:
        print("\n  No data extracted. Check that input files exist.")
        return pd.DataFrame()

    fact = pd.concat(frames, ignore_index=True)
    fact = dedup_fact(fact)
    fact = fact.sort_values(["Company", "Year", "MetricCode"])
    fact = fact.reset_index(drop=True)

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    # UTF-8-SIG (BOM) ensures correct encoding when the file is opened in Excel
    fact.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n  Saved {len(fact)} rows -> {out_path}")
    return fact


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="ESG ETL pipeline: merge company ESG metrics into fact_esg_core.csv"
    )
    parser.add_argument(
        "--mode", choices=["combine", "extract"], default="combine",
        help="combine: auto-detect *_core.xlsx and merge (default) | "
             "extract: run mappers from LlamaParse source Excel files",
    )
    parser.add_argument("--out", default=DEFAULT_OUT,
                        help=f"Output CSV path (default: {DEFAULT_OUT})")
    parser.add_argument("--skip", default="",
                        help="Comma-separated list of companies to skip (e.g. ILJIN,SKODA)")

    # Source file overrides (used only in extract mode)
    parser.add_argument("--audi",    default=None, help="[extract] Path to AUDI source Excel")
    parser.add_argument("--hmc",     default=None, help="[extract] Path to HMC source Excel")
    parser.add_argument("--iljin",   default=None, help="[extract] Path to ILJIN source Excel")
    parser.add_argument("--skoda",   default=None, help="[extract] Path to SKODA source Excel")
    parser.add_argument("--sungwoo", default=None, help="[extract] Path to SUNGWOO source Excel")

    args = parser.parse_args()
    skip = {s.strip().upper() for s in args.skip.split(",") if s.strip()}

    print("=" * 60)
    print(f"ESG ETL Pipeline  [mode: {args.mode}]")
    print("=" * 60)

    if args.mode == "combine":
        run_combine(skip, args.out)
    else:
        source_paths = {}
        for company, val in [("AUDI", args.audi), ("HMC", args.hmc),
                              ("ILJIN", args.iljin), ("SKODA", args.skoda),
                              ("SUNGWOO", args.sungwoo)]:
            if val:
                source_paths[company] = val
        run_extract(source_paths, skip, args.out)


if __name__ == "__main__":
    main()
