"""
run_all.py
----------
ESG ETL pipeline with two modes:

  combine (default)
    Reads the pre-processed *_core.xlsx files (one per company) and
    merges them into a single fact_esg_core.csv ready for Power BI.
    Use this mode when the mapper step has already been completed.

  extract
    Runs the raw company mappers against the original structured
    Excel files (page-named sheets extracted from PDF reports) and
    then combines the results.  Requires the full source Excel files.

Usage examples
--------------
  # Combine already-processed core files (typical daily use):
  python run_all.py

  # Same as above, explicit:
  python run_all.py --mode combine

  # Run full extraction from source Excel files:
  python run_all.py --mode extract

  # Extract only specific companies:
  python run_all.py --mode extract --skip ILJIN,SKODA

  # Custom input / output paths:
  python run_all.py --mode combine --out results/fact_esg_core.csv
  python run_all.py --mode extract --audi data/audi_source.xlsx
"""

import argparse
import os
import sys
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ---------------------------------------------------------------------------
# Company registry
# ---------------------------------------------------------------------------
COMPANIES = {
    "AUDI":    {"core": "audi_core.xlsx",    "source_default": "audi_source.xlsx"},
    "HMC":     {"core": "hmc_core.xlsx",     "source_default": "hmc_source.xlsx"},
    "ILJIN":   {"core": "iljin_core.xlsx",   "source_default": "iljin_source.xlsx"},
    "SKODA":   {"core": "skoda_core.xlsx",   "source_default": "skoda_source.xlsx"},
    "SUNGWOO": {"core": "sungwoo_core.xlsx", "source_default": "sungwoo_source.xlsx"},
}

DEFAULT_OUT = os.path.join("ESG_PowerBI", "drop", "fact_esg_core.csv")


def _pillar(code: str) -> str:
    """Map a MetricCode to an ESG pillar letter (E / S / G)."""
    if code.startswith(("ENERGY_", "GHG_", "WATER_", "WASTE_")):
        return "E"
    if code.startswith(("EMPLOYEES_", "HNS_", "EMP_", "SICKNESS_",
                         "FLUCTUATION_", "DRAWDOWN_")):
        return "S"
    return "G"


# ---------------------------------------------------------------------------
# Mode 1: combine pre-processed *_core.xlsx files
# ---------------------------------------------------------------------------
def run_combine(skip: set, out_path: str) -> pd.DataFrame:
    """Read *_core.xlsx files and merge into one fact table."""
    frames = []

    for company, cfg in COMPANIES.items():
        if company in skip:
            print(f"  [SKIP] {company}")
            continue

        core_path = cfg["core"]
        if not os.path.isfile(core_path):
            print(f"  [WARN] {company}: {core_path} not found — skipping")
            continue

        print(f"  [READ] {company} <- {core_path}")
        try:
            df = pd.read_excel(core_path)
            df["Pillar"] = df["MetricCode"].apply(_pillar)
            frames.append(df)
            print(f"         {len(df)} rows")
        except Exception as e:
            print(f"  [ERROR] {company}: {e}")

    return _save(frames, out_path)


# ---------------------------------------------------------------------------
# Mode 2: extract from source Excel files (full mapper run)
# ---------------------------------------------------------------------------
def run_extract(source_paths: dict, skip: set, out_path: str) -> pd.DataFrame:
    """Run company mappers against original structured Excel files."""
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

            # Save per-company core file as well
            core_out = COMPANIES[company]["core"]
            df.to_excel(core_out, index=False)
            print(f"         {len(df)} rows  |  saved -> {core_out}")

            frames.append(df)
        except Exception as e:
            print(f"  [ERROR] {company}: {e}")

    return _save(frames, out_path)


# ---------------------------------------------------------------------------
# Shared helper
# ---------------------------------------------------------------------------
def _save(frames: list, out_path: str) -> pd.DataFrame:
    if not frames:
        print("\nNo data extracted. Check that input files exist.")
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    result = result.sort_values(["Company", "Year", "MetricCode"]).reset_index(drop=True)

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    result.to_csv(out_path, index=False, encoding="utf-8")
    print(f"\n  Saved {len(result)} rows -> {out_path}")
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="ESG ETL pipeline: merge company ESG metrics into fact_esg_core.csv"
    )
    parser.add_argument(
        "--mode", choices=["combine", "extract"], default="combine",
        help="combine: merge *_core.xlsx files (default) | extract: run mappers from source Excel"
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
