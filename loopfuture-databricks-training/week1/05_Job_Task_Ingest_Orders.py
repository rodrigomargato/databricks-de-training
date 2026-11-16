# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Job task: ingest a new day of orders
# MAGIC
# MAGIC Used by the demo *"Automate Your Pipeline with a Lakeflow Job"*. This notebook is **task 1** of the job:
# MAGIC 1. simulates the arrival of a new orders file
# MAGIC 2. loads it into `bronze_orders` with `COPY INTO`
# MAGIC
# MAGIC Task 2 of the job is `04_Medallion_Architecture_Pipeline`.
# MAGIC
# MAGIC ## Create the job (Jobs & Pipelines → Create → Job)
# MAGIC | Setting | Value |
# MAGIC |---|---|
# MAGIC | Job name | `de_training_daily_orders` |
# MAGIC | Task 1 name | `ingest_orders` → Notebook: **this notebook**, Compute: **Serverless** |
# MAGIC | Task 2 name | `build_medallion` → Notebook: `04_Medallion_Architecture_Pipeline`, **Depends on** `ingest_orders` |
# MAGIC | Parameter (task 1, optional) | `batch_id` — leave empty to use the next batch automatically |
# MAGIC | Schedule (optional) | Add trigger → Scheduled → every day |
# MAGIC
# MAGIC Then click **Run now** and watch the task graph. Run it 2–3 times and compare `gold_daily_revenue`.

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

dbutils.widgets.text("batch_id", "", "Batch id (empty = next)")
batch_param = dbutils.widgets.get("batch_id").strip()
batch_id = int(batch_param) if batch_param else next_batch_id()

file_path = write_orders_batch(batch_id=batch_id)
print(f"New file landed: {file_path}")

# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS bronze_orders")

result = spark.sql(f"""
COPY INTO bronze_orders
FROM (
  SELECT *, _metadata.file_name AS source_file, current_timestamp() AS ingested_at
  FROM '{LANDING_PATH}/orders/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true')
""")
display(result)

# COMMAND ----------

# Value visible in the job run output and usable by later tasks (task values)
n_rows = spark.table("bronze_orders").count()
dbutils.jobs.taskValues.set(key="bronze_rows", value=n_rows)
print(f"bronze_orders now has {n_rows} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Exam-style practice (exam sections 4.1–4.3)
# MAGIC Pick one answer for each question. We discuss them together.
# MAGIC
# MAGIC **Q1.** Task B must run only after Task A succeeds. What do you configure?
# MAGIC
# MAGIC - A. A retry policy on Task B
# MAGIC - B. A dependency: Task B depends on Task A
# MAGIC - C. Two separate jobs with the same schedule
# MAGIC - D. A file arrival trigger on Task A
# MAGIC
# MAGIC **Q2.** New files land at unpredictable times and should be processed as soon as they arrive. Which trigger fits best?
# MAGIC
# MAGIC - A. Scheduled (every day at 02:00)
# MAGIC - B. File arrival trigger
# MAGIC - C. Manual run
# MAGIC - D. Continuous SQL warehouse
# MAGIC
# MAGIC **Q3.** A job's second task failed because of a bug that is now fixed. What re-runs only the failed task?
# MAGIC
# MAGIC - A. Run now
# MAGIC - B. Repair run
# MAGIC - C. Delete and recreate the job
# MAGIC - D. Restart the SQL warehouse
# MAGIC
