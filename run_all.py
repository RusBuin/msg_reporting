"""
run_all.py
----------
Main ETL pipeline: runs all company mappers and combines results
into a single canonical fact table (fact_esg_core.csv).

Usage:
    python run_all.py [options]

Options:
    --audi     PATH   Path to audi Excel file    (default: audi_core.xlsx)
    --hmc      PATH   Path to HMC Excel file     (default: hmc_core.xlsx)
    --iljin    PATH   Path to ILJIN Excel file   (default: iljin_core.xlsx)
    --skoda    PATH   Path to SKODA Excel file   (default: skoda_core.xlsx)
    --sungwoo  PATH   Path to SUNGWOO Excel file (default: sungwoo_core.xlsx)
    --out      PATH   Output CSV path            (default: ESG_PowerBI/drop/fact_esg_core.csv)
    --skip     LIST   Comma-separated companies to skip (e.g. --skip ILJIN,SKODA)

Examples:
    # Run with default file names (all files in the current folder):
    python run_all.py

    # Run with custom paths:
    python run_all.py --audi data/audi.xlsx --hmc data/hmc.xlsx

    # Skip companies whose Excel files are not available:
    python run_all.py --skip ILJIN,SUNGWOO
"""

import argparse
import os
import sys
import pandas as pd

# ---------------------------------------------------------------------------
# Ensure the mappers/ package is importable regardless of working directory
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mappers.audi_mapper import extract_audi_core
from mappers.hmc_mapper import extract_hmc_core
from mappers.iljin_mapper import extract_iljin_core
from mappers.skoda_mapper import extract_skoda_core
from mappers.sungwoo_mapper import extract_sungwoo_core


# ---------------------------------------------------------------------------
# Company registry
# ---------------------------------------------------------------------------
COMPANIES = {
    "AUDI":    {"fn": extract_audi_core,    "default": "audi_core.xlsx"},
    "HMC":     {"fn": extract_hmc_core,     "default": "hmc_core.xlsx"},
    "ILJIN":   {"fn": extract_iljin_core,   "default": "iljin_core.xlsx"},
    "SKODA":   {"fn": extract_skoda_core,   "default": "skoda_core.xlsx"},
    "SUNGWOO": {"fn": extract_sungwoo_core, "default": "sungwoo_core.xlsx"},
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


def run_pipeline(paths: dict, skip: set, out_path: str) -> pd.DataFrame:
    frames = []

    for company, cfg in COMPANIES.items():
        if company in skip:
            print(f"  [SKIP] {company}")
            continue

        excel_path = paths.get(company, cfg["default"])

        if not os.path.isfile(excel_path):
            print(f"  [WARN] {company}: file not found -> {excel_path}  (skipping)")
            continue

        print(f"  [RUN]  {company} <- {excel_path}")
        try:
            df = cfg["fn"](excel_path)
            df["Pillar"] = df["MetricCode"].apply(_pillar)
            frames.append(df)
            print(f"         {len(df)} rows extracted")
        except Exception as e:
            print(f"  [ERROR] {company}: {e}")

    if not frames:
        print("\nNo data extracted. Check that Excel files exist.")
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    result = result.sort_values(["Company", "Year", "MetricCode"]).reset_index(drop=True)

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    result.to_csv(out_path, index=False, encoding="utf-8")
    print(f"\n✓ Saved {len(result)} rows -> {out_path}")
    return result


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="ESG ETL pipeline: extract metrics from Excel files and combine into fact_esg_core.csv"
    )
    parser.add_argument("--audi",    default=None, help="Path to AUDI Excel file")
    parser.add_argument("--hmc",     default=None, help="Path to HMC Excel file")
    parser.add_argument("--iljin",   default=None, help="Path to ILJIN Excel file")
    parser.add_argument("--skoda",   default=None, help="Path to SKODA Excel file")
    parser.add_argument("--sungwoo", default=None, help="Path to SUNGWOO Excel file")
    parser.add_argument("--out",     default=DEFAULT_OUT, help=f"Output CSV path (default: {DEFAULT_OUT})")
    parser.add_argument("--skip",    default="", help="Comma-separated list of companies to skip")
    args = parser.parse_args()

    # Build paths dict (only override defaults when explicitly provided)
    paths = {}
    for company, arg_val in [("AUDI", args.audi), ("HMC", args.hmc),
                              ("ILJIN", args.iljin), ("SKODA", args.skoda),
                              ("SUNGWOO", args.sungwoo)]:
        if arg_val:
            paths[company] = arg_val
        else:
            paths[company] = COMPANIES[company]["default"]

    skip = {s.strip().upper() for s in args.skip.split(",") if s.strip()}

    print("=" * 60)
    print("ESG ETL Pipeline")
    print("=" * 60)
    run_pipeline(paths, skip, args.out)


if __name__ == "__main__":
    main()
