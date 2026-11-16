# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Load Data Incrementally with COPY INTO
# MAGIC
# MAGIC Follows the demo *"Load Data Incrementally with COPY INTO"* from **Get Started with Databricks for Data Engineering**.
# MAGIC
# MAGIC Scenario: every night ShopLoop's order system drops a new CSV file with the day's orders into the volume. We want to load **only the new files** into a bronze table.
# MAGIC
# MAGIC `COPY INTO` remembers which files it already loaded → running it twice does **not** duplicate data (*idempotent*).

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Day 1: the first file arrives

# COMMAND ----------

print(write_orders_batch(batch_id=1))
print(list_landing_files("orders"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- An empty Delta table: COPY INTO will create the columns from the file
# MAGIC CREATE TABLE IF NOT EXISTS bronze_orders;
# MAGIC
# MAGIC COPY INTO bronze_orders
# MAGIC FROM (
# MAGIC   SELECT *,
# MAGIC          _metadata.file_name   AS source_file,   -- metadata columns: where did each row come from?
# MAGIC          current_timestamp()   AS ingested_at
# MAGIC   FROM '/Volumes/workspace/de_training/raw/landing/orders/'
# MAGIC )
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT source_file, COUNT(*) AS n_rows FROM bronze_orders GROUP BY source_file

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Run it again — nothing changes
# MAGIC Look at `num_inserted_rows` in the result: it should be **0**.

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO bronze_orders
# MAGIC FROM (
# MAGIC   SELECT *, _metadata.file_name AS source_file, current_timestamp() AS ingested_at
# MAGIC   FROM '/Volumes/workspace/de_training/raw/landing/orders/'
# MAGIC )
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Day 2: a new file arrives → only it gets loaded

# COMMAND ----------

print(write_orders_batch(batch_id=2))
print(list_landing_files("orders"))

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO bronze_orders
# MAGIC FROM (
# MAGIC   SELECT *, _metadata.file_name AS source_file, current_timestamp() AS ingested_at
# MAGIC   FROM '/Volumes/workspace/de_training/raw/landing/orders/'
# MAGIC )
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT source_file, COUNT(*) AS n_rows, MIN(ingested_at) AS loaded_at
# MAGIC FROM bronze_orders
# MAGIC GROUP BY source_file
# MAGIC ORDER BY source_file

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Bronze keeps everything — even bad data
# MAGIC Bronze is a faithful copy of the source. The dirty rows are still here; we fix them in **silver** (next notebook).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'missing customer' AS issue, COUNT(*) AS n FROM bronze_orders WHERE customer_id IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'negative quantity', COUNT(*) FROM bronze_orders WHERE quantity <= 0
# MAGIC UNION ALL
# MAGIC SELECT 'missing unit_price', COUNT(*) FROM bronze_orders WHERE unit_price IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'duplicated order lines', COUNT(*) - COUNT(DISTINCT order_id) FROM bronze_orders
# MAGIC UNION ALL
# MAGIC SELECT 'status variants', COUNT(DISTINCT status) FROM bronze_orders

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Check yourself
# MAGIC - Why did the second `COPY INTO` insert 0 rows?
# MAGIC - What would happen with a plain `INSERT INTO ... SELECT * FROM read_files(...)` run twice?
# MAGIC - `COPY INTO` vs **Auto Loader**: when would you choose each one? *(we go deeper on day 4)*

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Exam-style practice (exam sections 2.1–2.2)
# MAGIC Pick one answer for each question. We discuss them together.
# MAGIC
# MAGIC **Q1.** A daily job runs `COPY INTO` over a folder that already contains files loaded yesterday plus one new file. What is loaded?
# MAGIC
# MAGIC - A. All files again, creating duplicates
# MAGIC - B. Only the new file
# MAGIC - C. Nothing, COPY INTO runs only once
# MAGIC - D. Only files modified in the last hour
# MAGIC
# MAGIC **Q2.** Which option lets COPY INTO add new columns that appear in the source files?
# MAGIC
# MAGIC - A. FORMAT_OPTIONS ('header' = 'true')
# MAGIC - B. COPY_OPTIONS ('mergeSchema' = 'true')
# MAGIC - C. FILEFORMAT = DELTA
# MAGIC - D. OPTIONS ('overwrite' = 'true')
# MAGIC
# MAGIC **Q3.** Which column records the source file of each row during ingestion?
# MAGIC
# MAGIC - A. input_file()
# MAGIC - B. _metadata.file_name
# MAGIC - C. _rescued_data
# MAGIC - D. current_file
# MAGIC
