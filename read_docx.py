"""
read_docx.py
------------
Extracts plain text from a Word (.docx) file and saves it to a .txt file.

Usage:
    python read_docx.py <input_docx> [output_txt]

Examples:
    python read_docx.py ESG.docx
    python read_docx.py ESG.docx output/esg_text.txt
"""

import zipfile
import xml.etree.ElementTree as ET
import os
import sys
import argparse


def get_docx_text(path: str) -> str:
    """Extract all paragraph text from a .docx file."""
    try:
        with zipfile.ZipFile(path) as document:
            xml_content = document.read("word/document.xml")
        tree = ET.XML(xml_content)

        WORD_NAMESPACE = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
        PARA = WORD_NAMESPACE + "p"
        TEXT = WORD_NAMESPACE + "t"

        paragraphs = []
        for paragraph in tree.iter(PARA):
            texts = [node.text for node in paragraph.iter(TEXT) if node.text]
            if texts:
                paragraphs.append("".join(texts))

        return "\n".join(paragraphs)
    except Exception as e:
        return f"Error: {e}"


def main():
    parser = argparse.ArgumentParser(description="Extract text from a Word (.docx) file.")
    parser.add_argument("input_docx", help="Path to the input .docx file")
    parser.add_argument(
        "output_txt",
        nargs="?",
        help="Path to the output .txt file (default: same name with .txt extension)",
    )
    args = parser.parse_args()

    input_docx = args.input_docx
    if not os.path.isfile(input_docx):
        print(f"Error: file not found: {input_docx}")
        sys.exit(1)

    if args.output_txt:
        out_path = args.output_txt
    else:
        base = os.path.splitext(input_docx)[0]
        out_path = base + "_text.txt"

    text = get_docx_text(input_docx)

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(text)

    print(f"Extracted {len(text):,} characters -> {out_path}")


if __name__ == "__main__":
    main()
