# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Creating and Working with a Delta Table
# MAGIC
# MAGIC Follows the demo *"Creating and Working with a Delta Table"* from **Get Started with Databricks for Data Engineering**.
# MAGIC
# MAGIC You will:
# MAGIC 1. Land a CSV file in a **volume**
# MAGIC 2. Create a **Delta table** from it (CTAS with `read_files`)
# MAGIC 3. Change the table (`INSERT`, `UPDATE`, `DELETE`)
# MAGIC 4. Look at its **history** and use **time travel**

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Land the source file in the volume

# COMMAND ----------

path = write_customers()   # 120 ShopLoop customers
print(path)
print(list_landing_files("customers"))

# COMMAND ----------

# MAGIC %md
# MAGIC Open **Catalog** (left sidebar) → `workspace` → `de_training` → **Volumes** → `raw` to see the file you just created.
# MAGIC
# MAGIC Peek at the raw file with SQL — `read_files` reads files without creating a table:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM read_files('/Volumes/workspace/de_training/raw/landing/customers/', format => 'csv', header => true)
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Create a Delta table (CTAS)
# MAGIC In Databricks, **every table is Delta by default**: Parquet files + a transaction log (`_delta_log`).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE customers AS
# MAGIC SELECT customer_id,
# MAGIC        first_name,
# MAGIC        last_name,
# MAGIC        email,
# MAGIC        country,
# MAGIC        city,
# MAGIC        CAST(is_prime AS BOOLEAN) AS is_prime,
# MAGIC        CAST(signup_date AS DATE) AS signup_date
# MAGIC FROM read_files('/Volumes/workspace/de_training/raw/landing/customers/', format => 'csv', header => true);
# MAGIC
# MAGIC SELECT * FROM customers ORDER BY customer_id LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Where is it, what format, how many files?
# MAGIC DESCRIBE DETAIL customers

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Change the data (ACID operations)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO customers VALUES ('C9999', 'Lara', 'Antunes', 'lara@mail.com', 'PT', 'Lisboa', true, current_date());
# MAGIC
# MAGIC UPDATE customers SET is_prime = true WHERE customer_id = 'C0001';
# MAGIC
# MAGIC DELETE FROM customers WHERE customer_id = 'C0002';
# MAGIC
# MAGIC SELECT * FROM customers WHERE customer_id IN ('C0001', 'C0002', 'C9999');

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · History and time travel
# MAGIC Every change creates a new **version** of the table.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY customers

# COMMAND ----------

# MAGIC %sql
# MAGIC -- How did customer C0002 look before the DELETE?
# MAGIC SELECT * FROM customers VERSION AS OF 0 WHERE customer_id IN ('C0001', 'C0002')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Undo everything: back to version 0
# MAGIC RESTORE TABLE customers TO VERSION AS OF 0;
# MAGIC SELECT COUNT(*) AS n_customers FROM customers;

# COMMAND ----------

# The same table from PySpark
df = spark.table("customers")
display(df.groupBy("country", "is_prime").count().orderBy("country", "is_prime"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Check yourself
# MAGIC - What is the difference between a **volume** and a **table**?
# MAGIC - Which command shows who changed a table and when?
# MAGIC - How would you read the table as it was **yesterday**? *(hint: `TIMESTAMP AS OF`)*

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Exam-style practice (exam sections 1.1)
# MAGIC Pick one answer for each question. We discuss them together.
# MAGIC
# MAGIC **Q1.** Which component gives Delta Lake its ACID transactions and time travel?
# MAGIC
# MAGIC - A. The Parquet file format
# MAGIC - B. The transaction log (_delta_log)
# MAGIC - C. The Hive metastore
# MAGIC - D. The SQL warehouse
# MAGIC
# MAGIC **Q2.** A user deleted rows by mistake. Which command returns the table to its previous state?
# MAGIC
# MAGIC - A. VACUUM customers
# MAGIC - B. OPTIMIZE customers
# MAGIC - C. RESTORE TABLE customers TO VERSION AS OF <n>
# MAGIC - D. DESCRIBE DETAIL customers
# MAGIC
# MAGIC **Q3.** What is stored in a Unity Catalog **volume**?
# MAGIC
# MAGIC - A. Only Delta tables
# MAGIC - B. Non-tabular files such as CSV, JSON or images
# MAGIC - C. SQL views
# MAGIC - D. Cluster configurations
# MAGIC
