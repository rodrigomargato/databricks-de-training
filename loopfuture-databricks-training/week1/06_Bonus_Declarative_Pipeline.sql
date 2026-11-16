-- =====================================================================================
-- 06 · BONUS — Build a Declarative Pipeline with Lakeflow Spark Declarative Pipelines
-- Follows the bonus demo of "Get Started with Databricks for Data Engineering".
--
-- The SAME medallion flow as notebooks 03 + 04, but DECLARATIVE:
-- you describe the tables, Databricks works out the order, the incremental logic
-- (only new files are processed) and the data quality checks.
--
-- How to use it (Jobs & Pipelines → Create → ETL pipeline):
--   1. Name: de_training_sdp     Default catalog: workspace     Default schema: de_training
--   2. Start with an empty file, paste this whole file, save
--   3. Click "Run pipeline" and open the pipeline graph
--   4. Run notebook 05 to land a new batch, then run the pipeline again:
--      only the new file is processed.
--
-- Prerequisite: at least one orders file in the volume (run notebook 03 or 05 first).
-- Free Edition allows ONE active pipeline per type: stop it when you are done.
-- =====================================================================================

-- BRONZE: incremental ingestion of new files (Auto Loader under the hood)
CREATE OR REFRESH STREAMING TABLE sdp_bronze_orders
COMMENT 'Raw orders, one row per CSV line, loaded incrementally'
AS SELECT *,
          _metadata.file_name AS source_file,
          current_timestamp() AS ingested_at
FROM STREAM read_files(
  '/Volumes/workspace/de_training/raw/landing/orders/',
  format => 'csv',
  header => true
);

-- Product catalog: a materialized view straight from the products file
CREATE OR REFRESH MATERIALIZED VIEW sdp_products
COMMENT 'ShopLoop product catalog'
AS SELECT product_id, product_name, category, brand, CAST(list_price AS DECIMAL(10,2)) AS list_price
FROM read_files('/Volumes/workspace/de_training/raw/landing/products/', format => 'csv', header => true);

-- SILVER: clean + data quality expectations
CREATE OR REFRESH STREAMING TABLE sdp_silver_orders (
  CONSTRAINT has_customer   EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_quantity EXPECT (quantity > 0)            ON VIOLATION DROP ROW,
  CONSTRAINT has_price      EXPECT (unit_price IS NOT NULL)  ON VIOLATION DROP ROW,
  CONSTRAINT known_status   EXPECT (status IN ('delivered', 'shipped', 'cancelled', 'returned', 'pending'))  -- warn only
)
COMMENT 'Typed and cleaned order lines'
AS SELECT order_id,
          customer_id,
          TRY_CAST(order_ts AS TIMESTAMP)                       AS order_ts,
          TRY_CAST(order_ts AS DATE)                            AS order_date,
          product_id,
          TRY_CAST(quantity AS INT)                             AS quantity,
          TRY_CAST(unit_price AS DECIMAL(10,2))                 AS unit_price,
          TRY_CAST(discount_pct AS INT)                         AS discount_pct,
          CAST(TRY_CAST(quantity AS INT) * TRY_CAST(unit_price AS DECIMAL(10,2))
               * (1 - TRY_CAST(discount_pct AS INT) / 100.0) AS DECIMAL(12,2)) AS net_amount,
          lower(trim(status))                                   AS status,
          channel,
          source_file
FROM STREAM(sdp_bronze_orders);

-- GOLD: always up to date, recomputed by the pipeline when silver changes
CREATE OR REFRESH MATERIALIZED VIEW sdp_gold_category_sales
COMMENT 'Net revenue per category (delivered and shipped orders only)'
AS SELECT p.category,
          COUNT(DISTINCT o.order_id) AS n_orders,
          SUM(o.quantity)            AS units_sold,
          SUM(o.net_amount)          AS net_revenue
FROM sdp_silver_orders o
JOIN sdp_products p ON o.product_id = p.product_id
WHERE o.status IN ('delivered', 'shipped')
GROUP BY p.category;

-- Discussion: the duplicated order line in each file is NOT removed here.
-- How would you deduplicate in a streaming table? (hint: AUTO CDC INTO, Week 2)
