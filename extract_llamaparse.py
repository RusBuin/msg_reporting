"""
extract_llamaparse.py
---------------------
Extracts tables from selected pages of a PDF report using LlamaParse
and saves the result as a multi-sheet Excel workbook.

Each sheet is named "Page {N} - A" to match the naming convention
expected by the company mapper scripts.

Usage:
  export LLAMA_CLOUD_API_KEY=llx-...
  python extract_llamaparse.py --pdf pdfs/audi-report-2024.pdf \\
      --pages 77,80,106,107,113,114,118 --out audi_source.xlsx

  python extract_llamaparse.py --pdf pdfs/hmc-2025-sustainability-report-en.pdf \\
      --pages 115,116,119,124 --out hmc_source.xlsx

Only the pages that actually contain ESG tables are sent to the API –
this keeps processing within the free-tier page limit.
"""

import argparse
import asyncio
import os
import sys
from typing import Dict, List, Optional

import nest_asyncio
import pandas as pd

nest_asyncio.apply()


def _get_api_key(cli_key: Optional[str]) -> str:
    key = cli_key or os.environ.get("LLAMA_CLOUD_API_KEY", "")
    if not key:
        print(
            "Error: LlamaParse API key not provided.\n"
            "  Pass --api-key YOUR_KEY  or  export LLAMA_CLOUD_API_KEY=YOUR_KEY"
        )
        sys.exit(1)
    return key


async def _extract_pages_async(pdf_path: str, pages: List[int], api_key: str) -> List[Dict]:
    """Send selected pages to LlamaParse API; each page is processed as a separate async task."""
    from llama_parse import LlamaParse

    parser = LlamaParse(
        api_key=api_key,
        result_type="markdown",        # LlamaParse returns structured Markdown tables
        verbose=False,
    )

    async def fetch_page(page_num: int) -> dict:
        # LlamaParse accepts page_number as a 1-based integer
        docs = await parser.aload_data(pdf_path, extra_info={"page_number": page_num})
        md = "\n".join(d.text for d in docs) if docs else ""
        return {"page": page_num, "markdown": md}

    tasks = [fetch_page(p) for p in pages]
    results = await asyncio.gather(*tasks)
    return results


def _markdown_to_dataframe(md: str) -> pd.DataFrame:
    """Parse all Markdown tables in a string and concatenate them into one DataFrame."""
    lines = md.strip().splitlines()
    table_lines: List[str] = []
    frames: List[pd.DataFrame] = []

    for line in lines:
        if line.startswith("|"):
            table_lines.append(line)
        else:
            if table_lines:
                frames.append(_parse_md_table(table_lines))
                table_lines = []

    if table_lines:
        frames.append(_parse_md_table(table_lines))

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _parse_md_table(lines: List[str]) -> pd.DataFrame:
    """Parse a single Markdown table (list of | … | lines) into a DataFrame."""
    # Filter out separator rows (---|---|---)
    data_lines = [row for row in lines
                  if set(row.replace("|", "").replace("-", "").replace(" ", "")) != set()]
    if not data_lines:
        return pd.DataFrame()

    rows = [[cell.strip() for cell in row.strip().strip("|").split("|")]
            for row in data_lines]

    if len(rows) < 2:
        return pd.DataFrame()

    headers = rows[0]
    df = pd.DataFrame(rows[1:], columns=headers)
    return df


def extract_to_excel(pdf_path: str, pages: List[int], out_path: str, api_key: str) -> None:
    """Fetch pages via LlamaParse and write each page as a sheet in an Excel workbook."""
    print(f"Extracting {len(pages)} pages from {pdf_path} ...")
    results = asyncio.run(_extract_pages_async(pdf_path, pages, api_key))

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for item in results:
            page_num = item["page"]
            md = item["markdown"]
            df = _markdown_to_dataframe(md)
            sheet_name = f"Page {page_num} - A"
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"  Page {page_num:>3}  -> sheet '{sheet_name}'  ({len(df)} rows)")

    print(f"\nSaved -> {out_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Extract ESG tables from a PDF report using LlamaParse."
    )
    parser.add_argument("--pdf",     required=True, help="Path to the source PDF file")
    parser.add_argument("--pages",   required=True,
                        help="Comma-separated list of page numbers to extract (e.g. 77,80,106)")
    parser.add_argument("--out",     required=True, help="Output Excel (.xlsx) path")
    parser.add_argument("--api-key", default=None,
                        help="LlamaParse API key (or set LLAMA_CLOUD_API_KEY env var)")
    args = parser.parse_args()

    if not os.path.isfile(args.pdf):
        print(f"Error: PDF not found: {args.pdf}")
        sys.exit(1)

    try:
        pages = [int(p.strip()) for p in args.pages.split(",") if p.strip()]
    except ValueError:
        print("Error: --pages must be a comma-separated list of integers (e.g. 77,80,106)")
        sys.exit(1)

    api_key = _get_api_key(args.api_key)
    extract_to_excel(args.pdf, pages, args.out, api_key)


if __name__ == "__main__":
    main()
