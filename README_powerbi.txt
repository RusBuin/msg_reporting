
Power BI Template (manual one-time setup)
========================================

Goal
----
Power BI should always read the same file:
  C:\ESG_PowerBI\drop\fact_esg_core.csv

Workflow
--------
1) Run the Python transformer (Google Colab or local):
   python run_transform.py --input_excel <PATH_TO_ONE_COMPANY_EXCEL> --company AUDI --powerbi_drop_path "C:\ESG_PowerBI\drop\fact_esg_core.csv"

2) Open Power BI Desktop and create a report once:
   - Get Data -> Blank Query -> Advanced Editor
   - Paste code from powerquery_fact_template.m
   - Close & Apply
   - Build visuals using MetricCode, Year, Company, ValueConverted

3) Save the report as a template:
   - File -> Export -> Power BI template (.pbit) or save .pbix

After that:
-----------
Each time you run the transformer and overwrite fact_esg_core.csv in the same folder,
you only press Refresh in Power BI and visuals update automatically.
