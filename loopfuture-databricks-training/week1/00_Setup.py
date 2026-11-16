# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup — Databricks DE Training (Week 1)
# MAGIC
# MAGIC This notebook prepares **your own** training environment. Every other notebook starts with `%run ./00_Setup`, so you normally don't run it by hand.
# MAGIC
# MAGIC What it does:
# MAGIC 1. Creates a **schema** and a **volume** in Unity Catalog for the training
# MAGIC 2. Sets them as the default for this session (`USE CATALOG` / `USE SCHEMA`)
# MAGIC 3. Defines helper functions that **generate the training dataset** as files in the volume
# MAGIC
# MAGIC > Built for **Databricks Free Edition** (serverless only). No internet access or external storage is needed: all data is generated here.
# MAGIC
# MAGIC ## The dataset: **ShopLoop**, an online marketplace (Amazon-like)
# MAGIC
# MAGIC | Source | Format | Arrives | Description |
# MAGIC |---|---|---|---|
# MAGIC | `customers` | CSV | once | Customers, country, city, Prime-like membership |
# MAGIC | `products` | CSV | once | Catalog: product, category, brand, list price |
# MAGIC | `orders` | CSV | **one file per day** | Order lines: who bought what, when, how many, at what price |
# MAGIC | `reviews` | JSON (nested) | one file per day | Product reviews with a nested reviewer object and tags (used from day 4) |
# MAGIC
# MAGIC Like real sources, the daily files contain **problems on purpose** (missing customer, negative quantity, missing price, duplicated lines, inconsistent status values) that we clean in the silver layer.
# MAGIC
# MAGIC **Unity Catalog objects used in the training**
# MAGIC ```
# MAGIC workspace                      <- catalog (exists by default in Free Edition)
# MAGIC └── de_training                <- schema   (created here)
# MAGIC     ├── raw                    <- volume   (files: landing/customers, products, orders, reviews)
# MAGIC     ├── bronze_* tables
# MAGIC     ├── silver_* tables
# MAGIC     └── gold_*   tables
# MAGIC ```

# COMMAND ----------

# Names used by ALL notebooks. If you change them, also update the paths in the SQL cells
# (they use /Volumes/workspace/de_training/raw/...).
CATALOG = "workspace"
SCHEMA = "de_training"
VOLUME = "raw"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
LANDING_PATH = f"{VOLUME_PATH}/landing"

# COMMAND ----------

# Dataset generators (pure Python: writes CSV / JSON files straight into the volume).
# Everything uses fixed random seeds, so every trainee gets exactly the same data.
import csv
import json
import os
import random
from datetime import datetime, timedelta

COUNTRIES = {
    "PT": ["Lisboa", "Porto", "Braga", "Coimbra"],
    "ES": ["Madrid", "Barcelona", "Valencia"],
    "FR": ["Paris", "Lyon", "Marseille"],
    "DE": ["Berlin", "Munich", "Hamburg"],
    "IT": ["Rome", "Milan", "Turin"],
    "UK": ["London", "Manchester"],
}
FIRST_NAMES = ["Ana", "João", "Maria", "Pedro", "Sofia", "Miguel", "Inês", "Tiago", "Laura", "Lucas",
               "Emma", "Liam", "Mia", "Noah", "Clara", "Hugo", "Elena", "Marco", "Giulia", "Paul"]
LAST_NAMES = ["Silva", "Santos", "Costa", "García", "López", "Martin", "Bernard", "Müller", "Schmidt",
              "Rossi", "Bianchi", "Smith", "Jones", "Ferreira", "Oliveira"]

# (product_id, name, category, brand, list_price)
PRODUCTS = [
    ("P001", "Echo Smart Speaker", "Electronics", "Amazona", 49.99),
    ("P002", "Kindle E-Reader", "Electronics", "Amazona", 109.99),
    ("P003", "Noise Cancelling Headphones", "Electronics", "Sonix", 249.00),
    ("P004", "USB-C Charger 65W", "Electronics", "VoltPro", 29.90),
    ("P005", "4K Monitor 27 inch", "Electronics", "ViewMax", 329.00),
    ("P006", "Wireless Mouse", "Electronics", "Logitek", 24.99),
    ("P007", "Mechanical Keyboard", "Electronics", "Logitek", 89.00),
    ("P008", "Smartwatch Fit 3", "Electronics", "Sonix", 179.00),
    ("P009", "Data Engineering Design Patterns", "Books", "O'Rielly", 54.90),
    ("P010", "Learning Spark (2nd Ed.)", "Books", "O'Rielly", 49.90),
    ("P011", "The Pragmatic Programmer", "Books", "Addison", 42.00),
    ("P012", "Clean Code", "Books", "Prentice", 38.50),
    ("P013", "Espresso Machine", "Home & Kitchen", "Barista+", 199.00),
    ("P014", "Air Fryer 5L", "Home & Kitchen", "CrispyCo", 89.90),
    ("P015", "Non-stick Pan Set", "Home & Kitchen", "ChefLine", 59.90),
    ("P016", "Robot Vacuum", "Home & Kitchen", "CleanBot", 279.00),
    ("P017", "Yoga Mat", "Sports", "FlexFit", 25.00),
    ("P018", "Running Shoes", "Sports", "Strider", 119.00),
    ("P019", "Adjustable Dumbbells", "Sports", "IronCore", 149.00),
    ("P020", "Cycling Helmet", "Sports", "Velo", 69.00),
    ("P021", "LEGO City Set", "Toys", "Bricks", 59.99),
    ("P022", "Board Game Catan", "Toys", "Kosmos", 44.90),
    ("P023", "RC Car", "Toys", "SpeedKids", 39.90),
    ("P024", "Face Moisturizer", "Beauty", "Glowa", 19.90),
    ("P025", "Electric Toothbrush", "Beauty", "Oral+", 69.90),
    ("P026", "Hair Dryer", "Beauty", "Glowa", 49.90),
]
# Status values are intentionally inconsistent: cleaning them is part of the silver layer
STATUSES = ["delivered", "Delivered", "DELIVERED", "shipped", "Shipped", "cancelled", "returned", "pending"]
STATUS_WEIGHTS = [30, 8, 4, 20, 5, 8, 5, 6]
CHANNELS = ["web", "app", "app", "web", "marketplace"]
REVIEW_TAGS = ["fast delivery", "good value", "great quality", "broke quickly", "as described",
               "gift", "would buy again", "poor packaging"]

N_CUSTOMERS = 120


def _ensure(folder: str) -> str:
    os.makedirs(folder, exist_ok=True)
    return folder


def write_customers(n_customers: int = N_CUSTOMERS, landing_path: str = None) -> str:
    """customers/customers.csv — one row per customer."""
    landing_path = landing_path or LANDING_PATH
    rnd = random.Random(42)
    file_path = f"{_ensure(f'{landing_path}/customers')}/customers.csv"
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["customer_id", "first_name", "last_name", "email", "country", "city", "is_prime", "signup_date"])
        for i in range(1, n_customers + 1):
            first, last = rnd.choice(FIRST_NAMES), rnd.choice(LAST_NAMES)
            country = rnd.choice(list(COUNTRIES))
            email = f"{first}.{last}{i}@mail.com".lower()
            if i % 37 == 0:
                email = ""                       # some customers have no e-mail
            signup = datetime(2023, 1, 1) + timedelta(days=rnd.randint(0, 1000))
            w.writerow([f"C{i:04d}", first, last, email, country, rnd.choice(COUNTRIES[country]),
                        rnd.random() < 0.4, signup.date().isoformat()])
    return file_path


def write_products(landing_path: str = None) -> str:
    """products/products.csv — the product catalog."""
    landing_path = landing_path or LANDING_PATH
    file_path = f"{_ensure(f'{landing_path}/products')}/products.csv"
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["product_id", "product_name", "category", "brand", "list_price"])
        w.writerows(PRODUCTS)
    return file_path


def write_orders_batch(batch_id: int, n_orders: int = 250, landing_path: str = None) -> str:
    """
    orders/orders_batch_NNN.csv — one file per day (batch 1 = 2026-11-01, batch 2 = 2026-11-02, ...).
    Dirty rows on purpose: missing customer_id, negative quantity, missing unit_price,
    one duplicated line, inconsistent status casing.
    """
    landing_path = landing_path or LANDING_PATH
    rnd = random.Random(1000 + batch_id)
    file_path = f"{_ensure(f'{landing_path}/orders')}/orders_batch_{batch_id:03d}.csv"
    day = datetime(2026, 11, 1) + timedelta(days=batch_id - 1)

    rows = []
    for i in range(n_orders):
        pid, _, _, _, list_price = rnd.choice(PRODUCTS)
        discount = rnd.choice([0, 0, 0, 5, 10, 15, 20])
        rows.append([
            f"O{batch_id:03d}{i:05d}",                                  # order_id
            f"C{rnd.randint(1, N_CUSTOMERS):04d}",                       # customer_id
            (day + timedelta(seconds=rnd.randint(0, 86399))).strftime("%Y-%m-%d %H:%M:%S"),
            pid,
            rnd.choices([1, 1, 1, 2, 2, 3, 4])[0],                      # quantity
            list_price,                                                  # unit_price
            discount,                                                    # discount_pct
            rnd.choices(STATUSES, weights=STATUS_WEIGHTS)[0],
            rnd.choice(CHANNELS),
        ])
    rows[0][1] = ""             # dirty: missing customer_id
    rows[1][4] = -1             # dirty: negative quantity
    rows[2][5] = ""             # dirty: missing unit_price
    rows.append(list(rows[3]))  # dirty: duplicated line

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "customer_id", "order_ts", "product_id", "quantity",
                    "unit_price", "discount_pct", "status", "channel"])
        w.writerows(rows)
    return file_path


def write_reviews_batch(batch_id: int, n_reviews: int = 60, landing_path: str = None) -> str:
    """
    reviews/reviews_batch_NNN.json — JSON Lines, one review per line, with NESTED data:
      {"review_id", "product_id", "rating", "title", "review_ts", "verified_purchase",
       "reviewer": {"customer_id", "country", "is_prime"}, "tags": [...], "helpful_votes"}
    From batch 3 on, a NEW field "language" appears (schema evolution exercise).
    """
    landing_path = landing_path or LANDING_PATH
    rnd = random.Random(5000 + batch_id)
    file_path = f"{_ensure(f'{landing_path}/reviews')}/reviews_batch_{batch_id:03d}.json"
    day = datetime(2026, 11, 1) + timedelta(days=batch_id - 1)
    with open(file_path, "w", encoding="utf-8") as f:
        for i in range(n_reviews):
            rating = rnd.choices([1, 2, 3, 4, 5], weights=[5, 5, 10, 30, 50])[0]
            review = {
                "review_id": f"R{batch_id:03d}{i:04d}",
                "product_id": rnd.choice(PRODUCTS)[0],
                "rating": rating,
                "title": rnd.choice(["Love it", "OK", "Not worth it", "Excellent", "Disappointed", "Great buy"]),
                "review_ts": (day + timedelta(seconds=rnd.randint(0, 86399))).strftime("%Y-%m-%dT%H:%M:%S"),
                "verified_purchase": rnd.random() < 0.85,
                "reviewer": {
                    "customer_id": f"C{rnd.randint(1, N_CUSTOMERS):04d}",
                    "country": rnd.choice(list(COUNTRIES)),
                    "is_prime": rnd.random() < 0.4,
                },
                "tags": rnd.sample(REVIEW_TAGS, k=rnd.randint(0, 3)),
                "helpful_votes": rnd.randint(0, 50),
            }
            if batch_id >= 3:
                review["language"] = rnd.choice(["en", "pt", "es", "fr", "de"])
            f.write(json.dumps(review, ensure_ascii=False) + "\n")
    return file_path


def write_static_sources() -> None:
    """Writes the sources that arrive only once (customers + products)."""
    print(write_customers())
    print(write_products())


def list_landing_files(sub_folder: str = "orders") -> list:
    folder = f"{LANDING_PATH}/{sub_folder}"
    return sorted(os.listdir(folder)) if os.path.exists(folder) else []


def next_batch_id(sub_folder: str = "orders") -> int:
    """Next batch number, based on the files already in the landing folder."""
    return len(list_landing_files(sub_folder)) + 1


def reset_training_schema():
    """DANGER: deletes the whole training schema (tables, volume and files). Call it only on purpose."""
    spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA} CASCADE")
    print(f"Dropped {CATALOG}.{SCHEMA}. Run 00_Setup again to recreate it.")


print(f"Catalog : {CATALOG}")
print(f"Schema  : {SCHEMA}")
print(f"Volume  : {VOLUME_PATH}")
print("Helpers : write_static_sources(), write_customers(), write_products(), write_orders_batch(n),")
print("          write_reviews_batch(n), next_batch_id(), list_landing_files(), reset_training_schema()")
