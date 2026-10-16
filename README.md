# Electricity Bill Intelligence on Databricks

A proof of concept that reads 120 synthetic Spanish electricity bills (PDF) with two Databricks AI functions,
checks every answer with business rules and measures its own accuracy against an answer key.

Result on 120 bills x 10 fields: 1,199 of 1,200 fields correct (99.9%), 0 bills quarantined, 5 bills sent to
human review (4 low-confidence false alarms, 1 real CUPS error), 1 of 1 AI errors caught by the review rules.
The summary document explains how to read these numbers (synthetic data, tuned on the same bills, tiny error sample).

## Files

| File | What it does |
|---|---|
| `nb_utils.py` | Settings (catalog, schema, confidence threshold 0.80), the 10 fields with their descriptions, shared helpers. Every notebook starts with `%run ./nb_utils`. |
| `nb_setup_unpack.py` | Unpacks the dataset zip from the volume into `landing/T1 ... T6` (20 bills per template). |
| `nb_try_one_bill.py` | Runs the two AI functions on a single bill, to see the raw output. |
| `nb_01_bronze.py` | Bronze: `ai_parse_document` then `ai_extract` (version 2.1, confidence scores) on every new bill; answers stored untouched. Bills already read are skipped. |
| `nb_02_silver.py` | Silver: typed values, the six business rules, review flags, quarantine for bills the AI could not read. |
| `nb_03_gold.py` | Gold: compares every field with the answer key, builds the bill fact table, the review queue, the scorecard and the dimensions. |
| `dashboard_queries.sql` | The four queries behind the dashboard (bills, field accuracy, AI errors, review queue). |

## Run order

1. In a Databricks workspace where AI Functions are available, create a catalog, a schema and a volume named `landing`.
2. Upload the IDSEM dataset zip to the volume and edit `CATALOG` and `SCHEMA` at the top of `nb_utils.py`
   (and the paths at the top of `nb_setup_unpack.py`).
3. Run `nb_setup_unpack`, then `nb_01_bronze`, `nb_02_silver`, `nb_03_gold`.
4. Create a dashboard on top of the queries in `dashboard_queries.sql`.

Each file is a Databricks notebook in source format: import it with Workspace > Import and the cells are kept.
Every cell starts with a comment saying what it does, and the last cells of the Bronze, Silver and Gold notebooks
show the first rows of the tables they created.

## Main tables

`bronze_extractions` (raw AI answers), `bronze_ground_truth` (answer key), `silver_bills`, `silver_truth_fields`,
`fact_bill`, `fact_field_accuracy`, `gold_review_queue`, `dim_date`, plus a field-level Silver table, a quarantine
table, an AI scorecard and a template dimension.

## Data

Bills and answer key: IDSEM dataset of synthetic Spanish electricity bills (CC BY 4.0). No personal data is used.
Built on Databricks Free Edition, so it is a proof of concept and not a production setup.
