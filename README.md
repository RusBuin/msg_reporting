# ESG Analysis & Power BI Report

Дипломна робота: ETL-пайплайн для збору та аналізу ESG-метрик автомобільних компаній з підготовкою даних для Power BI дашборду.

---

## Структура проекту

```
esg-analysis-powerbi/
│
├── mappers/                    # Модулі витягування даних по кожній компанії
│   ├── __init__.py
│   ├── audi_mapper.py          # AUDI AG
│   ├── hmc_mapper.py           # Hyundai Motor Company
│   ├── iljin_mapper.py         # ILJIN Slovakia
│   ├── skoda_mapper.py         # ŠKODA Auto
│   └── sungwoo_mapper.py       # SUNGWOO HITECH
│
├── pdfs/                       # Вихідні ESG-звіти у форматі PDF (не у git)
│
├── ESG_PowerBI/
│   └── drop/
│       ├── fact_esg_core.csv   # Фінальний датасет для Power BI
│       └── config.csv          # Конфігурація дашборду
│
├── extract_pdf.py              # Витягування тексту з PDF-файлів
├── read_docx.py                # Витягування тексту з Word-документів
├── run_all.py                  # Головний ETL-скрипт (запускає всі маппери)
├── powerquery_fact_template.m  # Power Query шаблон для Power BI
├── requirements.txt
└── README.md
```

---

## Компанії та джерела даних

| Компанія | ESG-звіт | Роки |
|---|---|---|
| AUDI AG | Sustainability Report 2024 | 2022–2024 |
| Hyundai Motor Company (HMC) | Sustainability Report 2025 | 2022–2024 |
| ILJIN Slovakia | CSR Report 2024 | 2022–2024 |
| ŠKODA Auto | ESG Report 2024 | 2024 |
| SUNGWOO HITECH | ESG Operation Report 2024 | 2022–2024 |

---

## Метрики (MetricCode)

| Код | Опис | Одиниця |
|---|---|---|
| `ENERGY_TOTAL` | Загальне споживання енергії | MWh |
| `ENERGY_RENEWABLE` | Споживання відновлюваної енергії | MWh |
| `GHG_SCOPE1` | Прямі викиди парникових газів | t CO₂e |
| `GHG_SCOPE2` | Непрямі викиди від споживання енергії | t CO₂e |
| `GHG_TOTAL` | Загальні викиди (Scope 1 + 2) | t CO₂e |
| `WATER_TOTAL` | Загальне споживання води | m³ / Ton |
| `WASTE_TOTAL` | Загальна кількість відходів | t |
| `WASTE_RECYCLED` | Відходи, що підлягають переробці | t |
| `EMPLOYEES_TOTAL` | Загальна кількість працівників | чол. |
| `EMPLOYEES_FEMALE` | Жінки серед працівників | чол. |
| `EMPLOYEES_MALE` | Чоловіки серед працівників | чол. |
| `HNS_TRIR` | Частота виробничих травм (TRIR) | — |

---

## Встановлення та запуск

### 1. Встановити залежності

```bash
pip install -r requirements.txt
```

### 2. Запустити ETL-пайплайн

```bash
# Запустити всі маппери (файли *_core.xlsx мають бути в папці проекту):
python run_all.py

# Або вказати шляхи до файлів вручну:
python run_all.py --audi path/to/audi.xlsx --hmc path/to/hmc.xlsx

# Пропустити компанії, для яких немає файлів:
python run_all.py --skip ILJIN,SKODA
```

Результат зберігається у `ESG_PowerBI/drop/fact_esg_core.csv`.

### 3. Витягування тексту з PDF

```bash
python extract_pdf.py pdfs/audi-report-2024.pdf
python extract_pdf.py pdfs/hmc-2025-sustainability-report-en.pdf output.txt
```

### 4. Підключення до Power BI

1. Відкрити Power BI Desktop → **Get Data → Blank Query → Advanced Editor**
2. Вставити вміст файлу `powerquery_fact_template.m`
3. Перевірити шлях до `fact_esg_core.csv` у рядку `SourcePath`
4. **Close & Apply**
5. Побудувати візуалізації за колонками: `Company`, `Year`, `MetricCode`, `Value`, `Pillar`

Після оновлення датасету достатньо натиснути **Refresh** у Power BI.

---

## Структура вихідного датасету

Файл `fact_esg_core.csv` містить наступні колонки:

| Колонка | Опис |
|---|---|
| `Company` | Назва компанії |
| `Year` | Рік звітності |
| `MetricCode` | Код метрики |
| `Value` | Числове значення |
| `UnitRaw` | Одиниця виміру з вихідного звіту |
| `MetricRaw` | Оригінальна назва метрики з Excel |
| `SourceSheet` | Лист Excel, звідки взято дані |
| `Pillar` | ESG-стовп: E (Environment), S (Social), G (Governance) |

---

## Технічний стек

- **Python 3.10+** — pandas, openpyxl, pypdf
- **Microsoft Power BI Desktop** — дашборд та візуалізації
- **Power Query (M)** — підключення та типізація даних
