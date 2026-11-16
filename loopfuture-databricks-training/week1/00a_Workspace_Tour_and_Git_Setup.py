# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 00a · Workspace Tour, Compute and Git Setup
# MAGIC
# MAGIC Before writing any code, get to know the platform:
# MAGIC 1. **Workspace tour** – where everything lives
# MAGIC 2. **Compute** – what runs your code and the serverless *Environment* panel
# MAGIC 3. **Catalog Explorer** – browse data without code
# MAGIC 4. **SQL Editor** – the same query outside a notebook
# MAGIC 5. **GitHub + Git folders** – keep all your training notebooks in your own repository

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Workspace tour (left sidebar)
# MAGIC
# MAGIC | Item | What it is for |
# MAGIC |---|---|
# MAGIC | **Workspace** | Folders with notebooks, files and Git folders. `Home` = your personal folder; `Shared` = everyone |
# MAGIC | **Recents** | Last things you opened |
# MAGIC | **Catalog** | Unity Catalog: catalogs → schemas → tables, views, volumes |
# MAGIC | **Jobs & Pipelines** | Lakeflow Jobs (orchestration) and Declarative Pipelines |
# MAGIC | **Compute** | Clusters and SQL warehouses (in Free Edition: serverless only) |
# MAGIC | **SQL Editor** | Write and run SQL against a SQL warehouse |
# MAGIC | **Dashboards / Genie** | BI and natural-language questions (not used in week 1) |
# MAGIC
# MAGIC 👉 **Try it:** open each item once. Then go to **Workspace → Home** and find this notebook.

# COMMAND ----------

# Who am I and what am I running on?
print("User         :", spark.sql("SELECT current_user()").first()[0])
print("Spark version:", spark.version)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Compute: what runs your code
# MAGIC
# MAGIC | Type | Used for | In Free Edition |
# MAGIC |---|---|---|
# MAGIC | **Serverless (notebooks & jobs)** | Interactive work and jobs; starts in seconds, no setup | ✅ the only option |
# MAGIC | **All-purpose cluster** | Interactive work on a cluster you configure (size, runtime) | ❌ |
# MAGIC | **Job cluster** | Created for one job run, deleted after it | ❌ |
# MAGIC | **SQL warehouse** | SQL Editor, dashboards, BI tools (Power BI) | ✅ serverless starter warehouse |
# MAGIC
# MAGIC ### The *Environment* side panel (right side of a serverless notebook)
# MAGIC
# MAGIC | Setting | Meaning | What we use |
# MAGIC |---|---|---|
# MAGIC | **Usage policy** | Tags usage for cost attribution (which team/project pays) | Auto-select |
# MAGIC | **Accelerator** | GPU (A10 / H100) for machine learning | None |
# MAGIC | **Memory** | Memory of the notebook's Python process: Standard 16 GB or High 32 GB (costs more, does **not** change Spark memory) | Standard |
# MAGIC | **Base environment** | Version of the pre-installed libraries and Python (e.g. *Standard v6 → Python 3.12*) | Latest Standard |
# MAGIC | **Include in source file exports** | Saves the environment version inside the file when exported or pushed to Git | On |
# MAGIC | **Dependencies** | Extra pip libraries (`package==version`) or a `requirements.txt`; see *Installed* and *pip logs* | Empty for now |
# MAGIC
# MAGIC 👉 **Try it:** open the panel, look at *Installed*, close it without changing anything.

# COMMAND ----------

# Notebook-scoped library install (alternative to the Dependencies box)
# %pip install simplejson
# After a %pip install, Python restarts: variables are lost.

# Files: a notebook can list files in a volume (works after 00_Setup has created it)
try:
    display(dbutils.fs.ls("/Volumes/workspace/de_training/raw/"))
except Exception as e:
    print("Volume not created yet: run 00_Setup first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Catalog Explorer
# MAGIC 1. Click **Catalog** → `samples` → `nyctaxi` → `trips`
# MAGIC 2. Look at the tabs: **Overview** (columns and types), **Sample data**, **Details**, **History**, **Lineage**, **Permissions**
# MAGIC 3. Do the same for `workspace` → `de_training` after running `00_Setup`
# MAGIC
# MAGIC ## 4 · SQL Editor
# MAGIC 1. Click **SQL Editor** → choose the **Serverless Starter Warehouse**
# MAGIC 2. Run: `SELECT pickup_zip, COUNT(*) AS trips FROM samples.nyctaxi.trips GROUP BY pickup_zip ORDER BY trips DESC LIMIT 10`
# MAGIC 3. Same result as a `%sql` cell in a notebook: the notebook uses *serverless notebook compute*, the editor uses a *SQL warehouse*.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT pickup_zip, count(*) FROM samples.nyctaxi.trips
# MAGIC group by pickup_zip
# MAGIC having count(*) = 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Your own GitHub repository + Databricks Git folder
# MAGIC
# MAGIC ### Step 1 — Create the repository (on github.com)
# MAGIC 1. Sign in (or create an account) on **github.com**
# MAGIC 2. **New repository** → name: `databricks-de-training` → **Private** → leave it **empty** (no README) → **Create repository**
# MAGIC 3. Click **uploading an existing file** and drag in the **contents** of the training ZIP → **Commit changes** *(details in `GETTING_STARTED.md`)*
# MAGIC 4. Copy the repository URL: **Code → HTTPS** (e.g. `https://github.com/<your-user>/databricks-de-training.git`)
# MAGIC
# MAGIC ### Step 2 — Create a token so Databricks can use your repo
# MAGIC 1. GitHub → profile picture → **Settings** → **Developer settings** → **Personal access tokens** → **Fine-grained tokens** → **Generate new token**
# MAGIC 2. Name: `databricks-training` · Expiration: 90 days · **Only select repositories** → `databricks-de-training`
# MAGIC 3. **Permissions → Repository permissions → Contents: Read and write**
# MAGIC 4. **Generate token** and copy it (you will only see it once!)
# MAGIC
# MAGIC ### Step 3 — Link GitHub in Databricks
# MAGIC 1. Databricks → your profile (top right) → **Settings** → **Linked accounts**
# MAGIC 2. **Add Git credential** → Provider: **GitHub** → **Personal access token**
# MAGIC 3. Username: your GitHub user · Token: paste it → **Save**
# MAGIC
# MAGIC ### Step 4 — Create the Git folder
# MAGIC 1. **Workspace** → **Home** → **Create** → **Git folder**
# MAGIC 2. Paste the repository URL → **Create Git folder**
# MAGIC 3. You now see `databricks-de-training` with `week1/`, `docs/` and the `README.md` from GitHub
# MAGIC
# MAGIC ### Step 5 — First commit & push
# MAGIC 1. Make a small change in a notebook inside the Git folder (e.g. add a Markdown cell with your name)
# MAGIC 2. Click the **branch name** button at the top of the Git folder → the Git dialog opens
# MAGIC 3. Review the changed files → write a message: `My first commit` → **Commit & Push**
# MAGIC 4. Refresh GitHub: your change is there ✅
# MAGIC
# MAGIC ### Daily routine from now on
# MAGIC | When | Action in the Git dialog |
# MAGIC |---|---|
# MAGIC | Start of the day | **Pull** (get the latest version) |
# MAGIC | End of each exercise | **Commit & Push** with a clear message |
# MAGIC | Trying something risky | Create a **branch** first |
# MAGIC
# MAGIC 💡 Notebooks are stored in Git as `.py` files (source format) — that is why they are readable on GitHub.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Check yourself
# MAGIC - Which compute types exist and which one does Free Edition give you?
# MAGIC - What does the *Memory* setting change, and what does it **not** change?
# MAGIC - What is the difference between a normal workspace folder and a **Git folder**?
# MAGIC - Why do we push our notebooks to GitHub?

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Exam-style practice (exam sections 1.2, 5.1)
# MAGIC Pick one answer for each question. We discuss them together.
# MAGIC
# MAGIC **Q1.** A team runs a notebook a few times a day and wants no cluster configuration and no cost while idle. Which compute fits best?
# MAGIC
# MAGIC - A. All-purpose cluster with auto-termination of 120 minutes
# MAGIC - B. Serverless compute for notebooks «--
# MAGIC - C. A job cluster
# MAGIC - D. A Pro SQL warehouse
# MAGIC
# MAGIC **Q2.** A notebook fails with an out-of-memory error while calling `toPandas()` on a large result. Which serverless setting can help?
# MAGIC
# MAGIC - A. Accelerator: A10
# MAGIC - B. Usage policy
# MAGIC - C. Memory: High «--
# MAGIC - D. Base environment: AI
# MAGIC
# MAGIC **Q3.** Where do you commit and push changes made to notebooks inside a Databricks Git folder?
# MAGIC
# MAGIC - A. In the Compute page
# MAGIC - B. In the Git dialog opened from the Git folder's branch button «--
# MAGIC - C. Only with the Databricks CLI
# MAGIC - D. In Catalog Explorer
# MAGIC