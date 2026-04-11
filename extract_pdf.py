"""
extract_pdf.py
--------------
Extracts raw text from a PDF file and saves it to a .txt file.

Usage:
    python extract_pdf.py <input_pdf> [output_txt]

Examples:
    python extract_pdf.py pdfs/audi-report-2024.pdf
    python extract_pdf.py pdfs/audi-report-2024.pdf output/audi_extracted.txt
"""

import sys
import subprocess
import os
import argparse


def install_and_import(package, import_name):
    try:
        __import__(import_name)
    except ImportError:
        print(f"Installing {package}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package, "--quiet"])


install_and_import("pypdf", "pypdf")
from pypdf import PdfReader


def extract_text_from_pdf(file_path: str, out_path: str) -> None:
    reader = PdfReader(file_path)
    text = ""
    for page in reader.pages:
        extracted = page.extract_text()
        if extracted:
            text += extracted + "\n"

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(text)

    print(f"Extracted {len(text):,} characters -> {out_path}")


def main():
    parser = argparse.ArgumentParser(description="Extract text from a PDF file.")
    parser.add_argument("input_pdf", help="Path to the input PDF file")
    parser.add_argument(
        "output_txt",
        nargs="?",
        help="Path to the output .txt file (default: same name as PDF with .txt extension)",
    )
    args = parser.parse_args()

    input_pdf = args.input_pdf
    if not os.path.isfile(input_pdf):
        print(f"Error: file not found: {input_pdf}")
        sys.exit(1)

    if args.output_txt:
        out_path = args.output_txt
    else:
        base = os.path.splitext(input_pdf)[0]
        out_path = base + "_extracted.txt"

    try:
        extract_text_from_pdf(input_pdf, out_path)
    except Exception as e:
        print(f"Error reading PDF: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
