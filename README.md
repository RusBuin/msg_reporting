# ESG Analysis & Power BI Report

Bachelor's thesis project: an ETL pipeline for collecting and analysing ESG metrics from automotive company sustainability reports, with a Power BI dashboard as the final output.

---

## Project Structure

```
esg-analysis-powerbi/
│
├── pdfs/                         # Source ESG reports (PDF)
├── data/
│   ├── source/                   # LlamaParse Excel outputs (page-named sheets)
│   └── processed/                # Mapper outputs (*_core.xlsx, one per company)
│
├── ESG_PowerBI/
│   └── drop/
│       └── fact_esg_core.csv     # Final consolidated dataset for Power BI
│
├── mappers/                      # Company-specific metric extractor modules
│   ├── __init__.py
│   ├── audi_mapper.py            # AUDI AG
│   ├── hmc_mapper.py             # Hyundai Motor Company
│   ├── iljin_mapper.py           # ILJIN Slovakia
│   ├── skoda_mapper.py           # ŠKODA Auto
│   └── sungwoo_mapper.py         # SUNGWOO HITECH
│
├── extract_llamaparse.py         # PDF table extraction via LlamaParse API
├── extract_pdf.py                # Basic PDF text extraction (pypdf)
├── read_docx.py                  # Text extraction from Word documents
├── run_all.py                    # Main ETL script
├── requirements.txt
└── README.md
```

---

## Companies & Data Sources

| Company | ESG Report | Years |
|---|---|---|
| AUDI AG | Sustainability Report 2024 | 2022–2024 |
| Hyundai Motor Company (HMC) | Sustainability Report 2025 | 2022–2024 |
| ILJIN Slovakia | CSR Report 2024 | 2022–2024 |
| ŠKODA Auto | ESG Report 2024 | 2024 |
| SUNGWOO HITECH | ESG Operation Report 2024 | 2022–2024 |

---

## Metrics (MetricCode)

| Code | Description | Unit |
|---|---|---|
| `ENERGY_TOTAL` | Total energy consumption | MWh |
| `ENERGY_RENEWABLE` | Energy from renewable sources | MWh |
| `GHG_SCOPE1` | Direct GHG emissions | t CO₂e |
| `GHG_SCOPE2` | Indirect GHG emissions (energy) | t CO₂e |
| `GHG_TOTAL` | Total GHG emissions (Scope 1 + 2) | t CO₂e |
| `WATER_TOTAL` | Total water consumption | m³ / Ton |
| `WASTE_TOTAL` | Total waste generated | t |
| `WASTE_RECYCLED` | Recycled waste | t |
| `EMPLOYEES_TOTAL` | Total number of employees | persons |
| `EMPLOYEES_FEMALE` | Female employees | persons |
| `EMPLOYEES_MALE` | Male employees | persons |
| `HNS_TRIR` | Total Recordable Incident Rate | — |

---

## Pipeline Overview

```
PDF reports  ──►  extract_llamaparse.py  ──►  data/source/*_source.xlsx
                                                       │
                                                 mappers/*.py
                                                       │
                                           data/processed/*_core.xlsx
                                                       │
                                                  run_all.py
                                                       │
                                        fact_esg_core.csv  ──►  Power BI
```

---

## Setup & Usage

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Extract tables from PDF (LlamaParse)

LlamaParse was used instead of standard PDF parsers because ESG reports contain
complex tables with merged cells and multi-level headers that geometric parsers
cannot handle correctly.

```bash
# Get a free API key at https://cloud.llamaindex.ai
export LLAMA_CLOUD_API_KEY=llx-...

python extract_llamaparse.py \
    --pdf pdfs/audi-report-2024.pdf \
    --pages 77,80,106,107,113,114,118 \
    --out data/source/audi_source.xlsx

python extract_llamaparse.py \
    --pdf pdfs/hmc-2025-sustainability-report-en.pdf \
    --pages 115,116,119,124 \
    --out data/source/hmc_source.xlsx
```

### 3. Run the ETL pipeline

```bash
# Combine pre-processed core files (default):
python run_all.py

# Run full extraction from source Excel files:
python run_all.py --mode extract

# Skip specific companies:
python run_all.py --mode extract --skip ILJIN,SKODA
```

Output is saved to `ESG_PowerBI/drop/fact_esg_core.csv`.

### 4. Connect to Power BI

1. Open Power BI Desktop → **Get Data → Text/CSV**
2. Select `ESG_PowerBI/drop/fact_esg_core.csv`
3. Build visuals using columns: `Company`, `Year`, `MetricCode`, `Value`, `Pillar`

---

## Output Dataset Columns

| Column | Description |
|---|---|
| `Company` | Company name |
| `Year` | Reporting year |
| `MetricCode` | Metric identifier |
| `Value` | Numeric value |
| `UnitRaw` | Unit as reported in the source |
| `MetricRaw` | Original metric label from Excel |
| `SourceSheet` | Excel sheet the data was taken from |
| `Pillar` | ESG pillar: E (Environment), S (Social), G (Governance) |

---

## Tech Stack

- **Python 3.10+** — pandas, openpyxl, pypdf, llama-parse
- **Microsoft Power BI Desktop** — dashboard and visualisations
