# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Build a Medallion Architecture Pipeline
# MAGIC
# MAGIC Follows the demo *"Build a Medallion Architecture Pipeline"* from **Get Started with Databricks for Data Engineering**.
# MAGIC
# MAGIC ```
# MAGIC  files in volume ──► BRONZE (raw, as received) ──► SILVER (clean, typed, deduplicated) ──► GOLD (business aggregates)
# MAGIC ```
# MAGIC
# MAGIC **Prerequisite:** run `03_Incremental_Load_with_COPY_INTO` first (creates `bronze_orders`).
# MAGIC
# MAGIC This notebook runs top to bottom with no manual steps, so it can also be a **task in a Lakeflow Job** (notebook 05).

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# Make sure the one-time sources exist (in case notebook 02 was skipped)
if not list_landing_files("customers") or not list_landing_files("products"):
    write_static_sources()

# COMMAND ----------

# MAGIC %md
# MAGIC ## BRONZE · customers and products
# MAGIC Orders are already in `bronze_orders` (loaded incrementally by COPY INTO in notebook 03).
# MAGIC Customers and products arrive as one full file each, so a simple full load (CTAS) is enough.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bronze_customers AS
# MAGIC SELECT *, _metadata.file_name AS source_file, current_timestamp() AS ingested_at
# MAGIC FROM read_files('/Volumes/workspace/de_training/raw/landing/customers/', format => 'csv', header => true);
# MAGIC
# MAGIC CREATE OR REPLACE TABLE bronze_products AS
# MAGIC SELECT *, _metadata.file_name AS source_file, current_timestamp() AS ingested_at
# MAGIC FROM read_files('/Volumes/workspace/de_training/raw/landing/products/', format => 'csv', header => true);

# COMMAND ----------

# MAGIC %md
# MAGIC ## SILVER · clean and conform
# MAGIC Rules for `silver_orders`:
# MAGIC - drop rows **without customer**, with **quantity ≤ 0** or **without price**
# MAGIC - **standardize** `status` to lower case (`Delivered`, `DELIVERED` → `delivered`)
# MAGIC - **cast** types explicitly and compute `net_amount = quantity × unit_price × (1 − discount)`
# MAGIC - **deduplicate** by `order_id`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver_orders AS
# MAGIC SELECT order_id, customer_id, order_ts, order_date, product_id, quantity,
# MAGIC        unit_price, discount_pct, net_amount, status, channel
# MAGIC FROM (
# MAGIC   SELECT order_id,
# MAGIC          customer_id,
# MAGIC          CAST(order_ts AS TIMESTAMP)                    AS order_ts,
# MAGIC          CAST(order_ts AS DATE)                         AS order_date,
# MAGIC          product_id,
# MAGIC          CAST(quantity AS INT)                          AS quantity,
# MAGIC          CAST(unit_price AS DECIMAL(10,2))              AS unit_price,
# MAGIC          CAST(discount_pct AS INT)                      AS discount_pct,
# MAGIC          CAST(quantity * unit_price * (1 - discount_pct / 100.0) AS DECIMAL(12,2)) AS net_amount,
# MAGIC          lower(trim(status))                            AS status,
# MAGIC          channel,
# MAGIC          ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY ingested_at DESC) AS rn
# MAGIC   FROM bronze_orders
# MAGIC   WHERE customer_id IS NOT NULL
# MAGIC     AND quantity > 0
# MAGIC     AND unit_price IS NOT NULL
# MAGIC )
# MAGIC WHERE rn = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver_customers AS
# MAGIC SELECT customer_id,
# MAGIC        initcap(first_name)          AS first_name,
# MAGIC        initcap(last_name)           AS last_name,
# MAGIC        NULLIF(lower(email), '')     AS email,
# MAGIC        upper(country)               AS country,
# MAGIC        city,
# MAGIC        CAST(is_prime AS BOOLEAN)    AS is_prime,
# MAGIC        CAST(signup_date AS DATE)    AS signup_date
# MAGIC FROM bronze_customers;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE silver_products AS
# MAGIC SELECT product_id, product_name, category, brand,
# MAGIC        CAST(list_price AS DECIMAL(10,2)) AS list_price
# MAGIC FROM bronze_products;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data quality check: bronze vs silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT (SELECT COUNT(*) FROM bronze_orders)               AS bronze_rows,
# MAGIC        (SELECT COUNT(*) FROM silver_orders)               AS silver_rows,
# MAGIC        (SELECT COUNT(DISTINCT status) FROM silver_orders) AS status_values,
# MAGIC        (SELECT COUNT(*) FROM silver_orders
# MAGIC          WHERE quantity <= 0 OR customer_id IS NULL OR unit_price IS NULL) AS bad_rows_left

# COMMAND ----------

# The same check in PySpark, failing loudly if a rule is broken (useful when this runs as a job)
from pyspark.sql import functions as F

silver = spark.table("silver_orders")
bad = silver.filter((F.col("quantity") <= 0) | F.col("customer_id").isNull() | F.col("unit_price").isNull()).count()
dupes = silver.count() - silver.select("order_id").distinct().count()
assert bad == 0, f"{bad} invalid rows reached silver"
assert dupes == 0, f"{dupes} duplicated order_id in silver"
print("Silver quality checks passed ✅")

# COMMAND ----------

# MAGIC %md
# MAGIC ## GOLD · business-ready tables
# MAGIC Only **delivered** and **shipped** orders count as revenue (cancelled, returned and pending don't).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_daily_revenue AS
# MAGIC SELECT o.order_date,
# MAGIC        c.country,
# MAGIC        p.category,
# MAGIC        COUNT(DISTINCT o.order_id) AS n_orders,
# MAGIC        SUM(o.quantity)            AS units_sold,
# MAGIC        SUM(o.net_amount)          AS net_revenue
# MAGIC FROM silver_orders o
# MAGIC JOIN silver_customers c ON o.customer_id = c.customer_id
# MAGIC JOIN silver_products  p ON o.product_id  = p.product_id
# MAGIC WHERE o.status IN ('delivered', 'shipped')
# MAGIC GROUP BY o.order_date, c.country, p.category;
# MAGIC
# MAGIC SELECT * FROM gold_daily_revenue ORDER BY order_date, net_revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_top_products AS
# MAGIC SELECT p.product_name,
# MAGIC        p.category,
# MAGIC        SUM(o.quantity)   AS units_sold,
# MAGIC        SUM(o.net_amount) AS net_revenue,
# MAGIC        ROUND(100.0 * SUM(CASE WHEN o.status = 'returned' THEN o.quantity ELSE 0 END) / SUM(o.quantity), 1) AS return_rate_pct
# MAGIC FROM silver_orders o
# MAGIC JOIN silver_products p ON o.product_id = p.product_id
# MAGIC WHERE o.status IN ('delivered', 'shipped', 'returned')
# MAGIC GROUP BY p.product_name, p.category;
# MAGIC
# MAGIC SELECT * FROM gold_top_products ORDER BY net_revenue DESC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 Click the **+** above a result → **Visualization** → bar chart of `net_revenue` by `category`.
# MAGIC
# MAGIC ## ✅ Check yourself
# MAGIC - Why don't we clean data directly in bronze?
# MAGIC - Which layer would a Power BI report read from?
# MAGIC - `CREATE OR REPLACE TABLE` rebuilds everything on each run. What changes when data grows a lot? *(hint: incremental / MERGE / Declarative Pipelines)*

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Exam-style practice (exam sections 3.1, 3.4, 3.6)
# MAGIC Pick one answer for each question. We discuss them together.
# MAGIC
# MAGIC **Q1.** In which layer should duplicates be removed and data types standardized?
# MAGIC
# MAGIC - A. Bronze
# MAGIC - B. Silver
# MAGIC - C. Gold
# MAGIC - D. Landing volume
# MAGIC
# MAGIC **Q2.** A Power BI report needs daily revenue per country. Which layer should it read from?
# MAGIC
# MAGIC - A. Bronze
# MAGIC - B. Silver
# MAGIC - C. Gold
# MAGIC - D. The CSV files in the volume
# MAGIC
# MAGIC **Q3.** Which SQL pattern keeps only the latest row per `order_id`?
# MAGIC
# MAGIC - A. SELECT DISTINCT *
# MAGIC - B. GROUP BY order_id
# MAGIC - C. ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY ingested_at DESC) and keep rn = 1
# MAGIC - D. UNION ALL
# MAGIC
