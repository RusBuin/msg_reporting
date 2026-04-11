# ESG Analysis & Power BI Report

Bachelor's thesis project: an ETL pipeline for collecting and analysing ESG metrics from automotive company sustainability reports, with a Power BI dashboard as the final output.

---

## Project Structure

```
esg-analysis-powerbi/
│
├── mappers/                    # Company-specific metric extractor modules
│   ├── __init__.py
│   ├── audi_mapper.py          # AUDI AG
│   ├── hmc_mapper.py           # Hyundai Motor Company
│   ├── iljin_mapper.py         # ILJIN Slovakia
│   ├── skoda_mapper.py         # ŠKODA Auto
│   └── sungwoo_mapper.py       # SUNGWOO HITECH
│
├── pdfs/                       # Source ESG reports in PDF format (not tracked in git)
│
├── ESG_PowerBI/
│   └── drop/
│       ├── fact_esg_core.csv   # Final dataset for Power BI
│       └── config.csv          # Dashboard configuration
│
├── extract_pdf.py              # CLI tool: extract text from PDF files
├── read_docx.py                # CLI tool: extract text from Word documents
├── run_all.py                  # Main ETL script (runs all mappers)
├── powerquery_fact_template.m  # Power Query M template for Power BI
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

## Setup & Usage

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Run the ETL pipeline

```bash
# Run all mappers (expects *_core.xlsx files in the project folder):
python run_all.py

# Specify custom file paths:
python run_all.py --audi path/to/audi.xlsx --hmc path/to/hmc.xlsx

# Skip companies whose Excel files are not available:
python run_all.py --skip ILJIN,SKODA
```

Output is saved to `ESG_PowerBI/drop/fact_esg_core.csv`.

### 3. Extract text from PDF

```bash
python extract_pdf.py pdfs/audi-report-2024.pdf
python extract_pdf.py pdfs/hmc-2025-sustainability-report-en.pdf output.txt
```

### 4. Connect to Power BI

1. Open Power BI Desktop → **Get Data → Blank Query → Advanced Editor**
2. Paste the contents of `powerquery_fact_template.m`
3. Update the `SourcePath` variable to point to your `fact_esg_core.csv`
4. Click **Close & Apply**
5. Build visuals using columns: `Company`, `Year`, `MetricCode`, `Value`, `Pillar`

After refreshing the dataset, just click **Refresh** in Power BI to update all visuals.

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

- **Python 3.10+** — pandas, openpyxl, pypdf
- **Microsoft Power BI Desktop** — dashboard and visualisations
- **Power Query (M)** — data connection and type casting
