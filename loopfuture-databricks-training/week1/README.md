# Week 1 — Foundations (16–20 Nov 2026)

Hands-on notebooks following the Databricks Academy course
**Get Started with Databricks for Data Engineering**. They run on **Databricks Free Edition**.

Dataset: **ShopLoop**, an Amazon-like online marketplace (see `../docs/data_dictionary.md`).

| # | Notebook | Topic |
|---|----------|-------|
| 00 | `00_Setup` | Creates your schema, volume and the ShopLoop dataset (runs automatically) |
| 00a | `00a_Workspace_Tour_and_Git_Setup` | Workspace, compute, Catalog, SQL Editor, GitHub + Git folder |
| 01 | `01_Python_and_PySpark_Basics` | Python essentials, DataFrames, PySpark ↔ SQL |
| 02 | `02_Delta_Table_Basics` | Delta tables, history, time travel |
| 03 | `03_Incremental_Load_with_COPY_INTO` | Incremental ingestion with COPY INTO |
| 04 | `04_Medallion_Architecture_Pipeline` | Bronze → silver → gold |
| 05 | `05_Job_Task_Ingest_Orders` | Automating the pipeline with a Lakeflow Job |
| 06 | `06_Bonus_Declarative_Pipeline.sql` | Bonus: the same pipeline, declarative |

Run them in order. Each notebook ends with **Check yourself** and **exam-style practice** questions.
Commit & push to your own repository at the end of each notebook.
