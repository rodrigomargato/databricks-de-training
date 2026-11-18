# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · SQL Basics — 20 topics
# MAGIC
# MAGIC Short, simple SQL practice. For each topic: **a short explanation → an example to run → one exercise** for you.
# MAGIC
# MAGIC All examples use **3 small tables** created in the first cell (no setup needed, they only exist in this session):
# MAGIC
# MAGIC | View | What it has |
# MAGIC |---|---|
# MAGIC | `customers` | 8 ShopLoop customers (country, Prime member, signup date) |
# MAGIC | `products` | 6 products (category, price) |
# MAGIC | `orders` | 15 order lines (who, what, how many, when, status) |
# MAGIC
# MAGIC **How to work:** run the example (`Shift + Enter`), read the result, then write your answer in the empty cell below the exercise.
# MAGIC
# MAGIC | # | Topic | # | Topic |
# MAGIC |---|---|---|---|
# MAGIC | 1 | SELECT * | 11 | DISTINCT |
# MAGIC | 2 | Choosing columns | 12 | Calculations & functions |
# MAGIC | 3 | Aliases (AS) | 13 | CASE WHEN |
# MAGIC | 4 | WHERE | 14 | COUNT, SUM, AVG, MIN, MAX |
# MAGIC | 5 | AND / OR / NOT | 15 | GROUP BY |
# MAGIC | 6 | IN and BETWEEN | 16 | HAVING |
# MAGIC | 7 | LIKE | 17 | INNER JOIN |
# MAGIC | 8 | NULL values | 18 | LEFT JOIN |
# MAGIC | 9 | ORDER BY | 19 | Dates |
# MAGIC | 10 | LIMIT | 20 | WITH (CTEs) |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Run this cell first: it creates the 3 practice tables (temporary views)
# MAGIC CREATE OR REPLACE TEMP VIEW customers AS
# MAGIC SELECT * FROM VALUES
# MAGIC   ('C01', 'Ana',    'Silva',   'PT', 'Lisboa',    true,  DATE'2024-01-15'),
# MAGIC   ('C02', 'Pedro',  'Costa',   'PT', 'Porto',     false, DATE'2024-03-02'),
# MAGIC   ('C03', 'Laura',  'García',  'ES', 'Madrid',    true,  DATE'2023-11-20'),
# MAGIC   ('C04', 'Lucas',  'Martin',  'FR', 'Paris',     false, DATE'2025-02-10'),
# MAGIC   ('C05', 'Emma',   'Müller',  'DE', 'Berlin',    true,  DATE'2024-07-30'),
# MAGIC   ('C06', 'Marco',  'Rossi',   'IT', 'Milan',     false, DATE'2025-06-01'),
# MAGIC   ('C07', 'Sofia',  'Santos',  'PT', 'Braga',     NULL,  DATE'2025-09-12'),
# MAGIC   ('C08', 'Noah',   'Smith',   'UK', 'London',    true,  DATE'2023-05-05')
# MAGIC AS t(customer_id, first_name, last_name, country, city, is_prime, signup_date);
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW products AS
# MAGIC SELECT * FROM VALUES
# MAGIC   ('P1', 'Kindle E-Reader',            'Electronics',    109.99),
# MAGIC   ('P2', 'Noise Cancelling Headphones','Electronics',    249.00),
# MAGIC   ('P3', 'Learning Spark',             'Books',           49.90),
# MAGIC   ('P4', 'Air Fryer 5L',               'Home & Kitchen',  89.90),
# MAGIC   ('P5', 'Yoga Mat',                   'Sports',          25.00),
# MAGIC   ('P6', 'Board Game Catan',           'Toys',            44.90)
# MAGIC AS t(product_id, product_name, category, price);
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW orders AS
# MAGIC SELECT * FROM VALUES
# MAGIC   ('O01', 'C01', 'P1', 1, TIMESTAMP'2026-11-01 09:15:00', 'delivered'),
# MAGIC   ('O02', 'C01', 'P3', 2, TIMESTAMP'2026-11-01 10:02:00', 'delivered'),
# MAGIC   ('O03', 'C02', 'P2', 1, TIMESTAMP'2026-11-01 11:30:00', 'cancelled'),
# MAGIC   ('O04', 'C03', 'P4', 1, TIMESTAMP'2026-11-02 08:45:00', 'shipped'),
# MAGIC   ('O05', 'C03', 'P5', 3, TIMESTAMP'2026-11-02 14:20:00', 'delivered'),
# MAGIC   ('O06', 'C04', 'P6', 1, TIMESTAMP'2026-11-02 18:05:00', 'returned'),
# MAGIC   ('O07', 'C05', 'P1', 2, TIMESTAMP'2026-11-03 09:00:00', 'delivered'),
# MAGIC   ('O08', 'C05', 'P2', 1, TIMESTAMP'2026-11-03 09:10:00', 'delivered'),
# MAGIC   ('O09', 'C06', 'P3', 1, TIMESTAMP'2026-11-03 12:40:00', 'pending'),
# MAGIC   ('O10', 'C08', 'P4', 2, TIMESTAMP'2026-11-04 16:55:00', 'shipped'),
# MAGIC   ('O11', 'C08', 'P6', 1, TIMESTAMP'2026-11-04 17:30:00', 'delivered'),
# MAGIC   ('O12', 'C01', 'P5', 1, TIMESTAMP'2026-11-05 07:50:00', 'delivered'),
# MAGIC   ('O13', 'C02', 'P1', 1, TIMESTAMP'2026-11-05 20:10:00', 'delivered'),
# MAGIC   ('O14', NULL,  'P2', 1, TIMESTAMP'2026-11-05 21:00:00', 'pending'),
# MAGIC   ('O15', 'C03', 'P3', 4, TIMESTAMP'2026-11-06 10:30:00', 'delivered')
# MAGIC AS t(order_id, customer_id, product_id, quantity, order_ts, status);
# MAGIC
# MAGIC SELECT 'Tables ready ✅' AS message;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · SELECT *
# MAGIC `SELECT *` returns **all columns** of a table. `FROM` says which table. Good for a first look, avoid it in real pipelines (you get columns you don't need).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers;

# COMMAND ----------

# MAGIC %md
# MAGIC **✏️ Exercise:** Show all columns of the `products` table.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- your answer here

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Choosing columns
# MAGIC List the columns you want, separated by commas, in the order you want them.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT first_name, last_name, country
# MAGIC FROM customers;

# COMMAND ----------

# MAGIC %md
# MAGIC **✏️ Exercise:** Show only `product_name` and `price` from `products`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- your answer here

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Aliases (AS)
# MAGIC `AS` gives a column (or table) a new name in the result. Useful for readable reports and for calculated columns.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT first_name AS name,
# MAGIC        country   AS country_code
# MAGIC FROM customers;

# COMMAND ----------

# MAGIC %md
# MAGIC **✏️ Exercise:** Show `product_name` as **product** and `price` as **price_eur**.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- your answer here

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · WHERE
# MAGIC `WHERE` keeps only the rows that match a condition. Operators: `=`, `<>` (different), `>`, `<`, `>=`, `<=`. Text goes in **single quotes**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT first_name, city
# MAGIC FROM customers
# MAGIC WHERE country = 'PT';

# COMMAND ----------

# MAGIC %md
# MAGIC **✏️ Exercise:** Show the products with a price **greater than 50**.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- your answer here

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · AND / OR / NOT
# MAGIC Combine conditions: `AND` = both must be true, `OR` = at least one, `NOT` = the opposite. Use parentheses when mixing AND and OR.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT first_name, country, is_prime
# MAGIC FROM customers
# MAGIC WHERE country = 'PT' AND is_prime = true;

# COMMAND ----------

# MAGIC %md
# MAGIC **✏️ Exercise:** Show orders that are `delivered` **or** `shipped` **and** have quantity greater than 1.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- your answer here

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · IN and BETWEEN
# MAGIC `IN (...)` = equal to any value of a list. `BETWEEN a AND b` = inside a range (**including** a and b).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT first_name, country
# MAGIC FROM customers
# MAGIC WHERE country IN ('PT', 'ES');

# COMMAND ----------

# MAGIC %md
# MAGIC **✏️ Exercise:** Show products with a price **between 40 and 100**.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- your answer here

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · LIKE
# MAGIC `LIKE` searches text patterns: `%` = any number of characters, `_` = exactly one character. `ILIKE` ignores upper/lower case.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT product_name
# MAGIC FROM products
# MAGIC WHERE product_name LIKE '%Game%';

# COMMAND ----------

# MAGIC %md
# MAGIC **✏️ Exercise:** Show the customers whose `last_name` **starts with S**.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- your answer here

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · NULL values
# MAGIC `NULL` = *unknown / missing*. It is **not** equal to anything, not even to NULL: use `IS NULL` / `IS NOT NULL`, never `= NULL`. `coalesce(a, b)` returns the first non-null value.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, customer_id
# MAGIC FROM orders
# MAGIC WHERE customer_id IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC **✏️ Exercise:** Show all customers, with `is_prime` replaced by **false** when it is NULL (column name `is_prime_clean`).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- your answer here

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · ORDER BY
# MAGIC `ORDER BY` sorts the result. `ASC` = ascending (default), `DESC` = descending. You can sort by several columns.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT product_name, price
# MAGIC FROM products
# MAGIC ORDER BY price DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC **✏️ Exercise:** Show customers sorted by `country` (A→Z) and then by `signup_date` (newest first).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- your answer here

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 · LIMIT
# MAGIC `LIMIT n` returns only the first *n* rows. Combined with `ORDER BY` it answers "top N" questions.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT product_name, price
# MAGIC FROM products
# MAGIC ORDER BY price DESC
# MAGIC LIMIT 3;

# COMMAND ----------

# MAGIC %md
# MAGIC **✏️ Exercise:** Show the **2 most recent** orders (`order_ts`).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- your answer here

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11 · DISTINCT
# MAGIC `DISTINCT` removes duplicate rows from the result: each combination appears once.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT country
# MAGIC FROM customers;

# COMMAND ----------

# MAGIC %md
# MAGIC **✏️ Exercise:** Show the list of different `status` values in `orders`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- your answer here

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12 · Calculations & functions
# MAGIC You can calculate in SELECT (`+ - * /`) and use functions: `round()`, `upper()`, `lower()`, `concat()`, `length()`…

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT upper(product_name)        AS product,
# MAGIC        price,
# MAGIC        round(price * 1.23, 2)     AS price_with_vat
# MAGIC FROM products;

# COMMAND ----------

# MAGIC %md
# MAGIC **✏️ Exercise:** Show `first_name` and `last_name` together in one column **full_name** (e.g. *Ana Silva*).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- your answer here

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13 · CASE WHEN
# MAGIC `CASE WHEN ... THEN ... ELSE ... END` creates a value based on conditions — the SQL version of *if / else*.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT product_name, price,
# MAGIC        CASE WHEN price >= 100 THEN 'premium'
# MAGIC             WHEN price >= 40  THEN 'standard'
# MAGIC             ELSE 'budget'
# MAGIC        END AS price_band
# MAGIC FROM products;

# COMMAND ----------

# MAGIC %md
# MAGIC **✏️ Exercise:** For each order, show `order_id`, `status` and a column **is_revenue** = 'yes' when status is delivered or shipped, otherwise 'no'.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- your answer here

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14 · COUNT, SUM, AVG, MIN, MAX
# MAGIC **Aggregate functions** turn many rows into one value. `COUNT(*)` counts rows, `COUNT(col)` counts non-null values.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)            AS n_products,
# MAGIC        round(AVG(price),2) AS avg_price,
# MAGIC        MIN(price)          AS cheapest,
# MAGIC        MAX(price)          AS most_expensive
# MAGIC FROM products;

# COMMAND ----------

# MAGIC %md
# MAGIC **✏️ Exercise:** How many orders are there, and how many **units** (sum of `quantity`) were ordered in total?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- your answer here

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15 · GROUP BY
# MAGIC `GROUP BY` calculates the aggregates **per group**. Rule: every column in SELECT is either in GROUP BY or inside an aggregate function.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT country, COUNT(*) AS n_customers
# MAGIC FROM customers
# MAGIC GROUP BY country
# MAGIC ORDER BY n_customers DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC **✏️ Exercise:** Count the number of orders **per status**.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- your answer here

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16 · HAVING
# MAGIC `WHERE` filters **rows before** grouping; `HAVING` filters **groups after** grouping (so it can use aggregates).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT country, COUNT(*) AS n_customers
# MAGIC FROM customers
# MAGIC GROUP BY country
# MAGIC HAVING COUNT(*) >= 2;

# COMMAND ----------

# MAGIC %md
# MAGIC **✏️ Exercise:** Show the customers (`customer_id`) with **more than 2 orders**.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- your answer here

# COMMAND ----------

# MAGIC %md
# MAGIC ## 17 · INNER JOIN
# MAGIC A **JOIN** combines tables using a common column. `INNER JOIN` keeps only rows that have a match on **both** sides. Use table aliases (`o`, `p`) to keep it short.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT o.order_id, p.product_name, o.quantity, p.price
# MAGIC FROM orders o
# MAGIC INNER JOIN products p ON o.product_id = p.product_id;

# COMMAND ----------

# MAGIC %md
# MAGIC **✏️ Exercise:** Show each order with the customer's `first_name` and `country`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- your answer here

# COMMAND ----------

# MAGIC %md
# MAGIC ## 18 · LEFT JOIN
# MAGIC `LEFT JOIN` keeps **all rows of the left table**, even without a match (the right columns come as NULL). Classic use: *find what is missing*.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c.first_name, o.order_id
# MAGIC FROM customers c
# MAGIC LEFT JOIN orders o ON c.customer_id = o.customer_id
# MAGIC ORDER BY c.first_name;

# COMMAND ----------

# MAGIC %md
# MAGIC **✏️ Exercise:** Which customers have **never ordered**? (hint: LEFT JOIN + `IS NULL`)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- your answer here

# COMMAND ----------

# MAGIC %md
# MAGIC ## 19 · Dates
# MAGIC Useful date functions: `to_date()`, `year()`, `month()`, `dayofweek()`, `date_trunc('MONTH', ts)`, `datediff(end, start)`, `current_date()`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id,
# MAGIC        to_date(order_ts)  AS order_date,
# MAGIC        hour(order_ts)     AS order_hour
# MAGIC FROM orders;

# COMMAND ----------

# MAGIC %md
# MAGIC **✏️ Exercise:** Count the number of orders **per day** (`to_date(order_ts)`), sorted by day.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- your answer here

# COMMAND ----------

# MAGIC %md
# MAGIC ## 20 · WITH (CTEs)
# MAGIC `WITH name AS (...)` creates a **temporary result with a name** that you use in the next query. It splits a complex query into readable steps — like variables in SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH order_values AS (
# MAGIC   SELECT o.order_id, o.customer_id, o.status, o.quantity * p.price AS amount
# MAGIC   FROM orders o JOIN products p ON o.product_id = p.product_id
# MAGIC )
# MAGIC SELECT status, round(SUM(amount), 2) AS total_amount
# MAGIC FROM order_values
# MAGIC GROUP BY status;

# COMMAND ----------

# MAGIC %md
# MAGIC **✏️ Exercise:** **Final challenge:** revenue (quantity × price) **per country**, only for delivered and shipped orders, highest first.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- your answer here

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Check yourself
# MAGIC - What is the difference between `WHERE` and `HAVING`?
# MAGIC - Why does `WHERE customer_id = NULL` return nothing?
# MAGIC - `INNER JOIN` vs `LEFT JOIN`: which one would you use to find customers without orders?
# MAGIC - In which order does the engine run `SELECT`, `FROM`, `WHERE`, `GROUP BY`, `ORDER BY`?

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Exam-style practice
# MAGIC **Q1.** Which query returns the number of orders per status, only for statuses with more than 2 orders?
# MAGIC
# MAGIC - A. `SELECT status, COUNT(*) FROM orders WHERE COUNT(*) > 2 GROUP BY status`
# MAGIC - B. `SELECT status, COUNT(*) FROM orders GROUP BY status HAVING COUNT(*) > 2`
# MAGIC - C. `SELECT status, COUNT(*) FROM orders HAVING status > 2`
# MAGIC - D. `SELECT DISTINCT status FROM orders LIMIT 2`
# MAGIC
# MAGIC **Q2.** A LEFT JOIN from `customers` to `orders` returns a customer with `order_id = NULL`. What does it mean?
# MAGIC
# MAGIC - A. The order was deleted
# MAGIC - B. The customer has no matching order
# MAGIC - C. The join condition is wrong
# MAGIC - D. The customer_id is NULL
# MAGIC
# MAGIC **Q3.** Which expression returns `'unknown'` when `city` is NULL?
# MAGIC
# MAGIC - A. `ifnull(city)`
# MAGIC - B. `city = NULL ? 'unknown'`
# MAGIC - C. `coalesce(city, 'unknown')`
# MAGIC - D. `CASE city WHEN NULL THEN 'unknown' END`
