# Getting started — your own GitHub repo + Databricks (≈ 20 min)

Goal: by the end of this page you have **your own private GitHub repository** with all the training
material, connected to **your Databricks Free Edition workspace** as a **Git folder**.
From then on you save your work with **Commit & Push** and get updates with **Pull**.

```
 this ZIP ──upload──►  GitHub (your repo)  ◄──pull / push──►  Databricks Git folder (your workspace)
```

---

## Step 1 — Databricks Free Edition account
1. Go to https://www.databricks.com/learn/free-edition → **Sign up**
2. Use Google, Microsoft or e-mail, enter the verification code
3. Your workspace opens. Keep this tab open.

## Step 2 — GitHub account and an empty repository
1. Go to https://github.com and sign in (or **Sign up**, confirm your e-mail)
2. Top right **+** → **New repository**
3. Repository name: `databricks-de-training`
4. Visibility: **Private**
5. ⚠️ Do **not** tick "Add a README", no .gitignore, no license (keep it empty)
6. **Create repository**

## Step 3 — Upload the training material to your repo
1. Unzip the file you received. Open the folder `loopfuture-databricks-training`
2. On the page of your new (empty) repo, click the link **uploading an existing file**
3. Select **everything inside** the folder (`README.md`, `GETTING_STARTED.md`, `.gitignore`, `docs`, `week1`) and **drag it** onto the GitHub page
4. Commit message: `Initial training material` → **Commit changes**
5. Check: you see the folders `week1` and `docs` on GitHub ✅

## Step 4 — Create a GitHub token (so Databricks can use your repo)
1. GitHub → your profile picture → **Settings** → (bottom of the left menu) **Developer settings**
2. **Personal access tokens** → **Fine-grained tokens** → **Generate new token**
3. Token name: `databricks-training` · Expiration: **90 days**
4. Repository access: **Only select repositories** → `databricks-de-training`
5. **Permissions** → **Repository permissions** → **Contents** → **Read and write**
6. **Generate token** → **copy it now** (you will not see it again)

## Step 5 — Link GitHub in Databricks
1. Databricks → your profile icon (top right) → **Settings**
2. **Linked accounts** → **Add Git credential**
3. Git provider: **GitHub** · choose **Personal access token**
4. Git provider username: your GitHub user · Token: paste it → **Save**

## Step 6 — Create the Git folder (clone your repo into Databricks)
1. On GitHub, open your repo → green **Code** button → **HTTPS** → copy the URL
   (`https://github.com/<your-user>/databricks-de-training.git`)
2. Databricks → **Workspace** → **Home** → **Create** → **Git folder**
3. Paste the URL → **Create Git folder**
4. Open it: you see `week1`, `docs`, `README.md` ✅

## Step 7 — Test Pull and Push
1. Open `week1/00a_Workspace_Tour_and_Git_Setup` and run the first cells
2. Click the **branch button** (`main`) at the top of the Git folder → the Git dialog opens
3. **Pull** → "Everything is up to date"
4. Make a tiny change (e.g. add a Markdown cell with your name) → in the Git dialog write
   `My first commit` → **Commit & Push**
5. Refresh your repo on GitHub: your change is there ✅

---

## Daily routine
| When | What |
|------|------|
| Start of the day | Git dialog → **Pull** |
| After each notebook / exercise | **Commit & Push** with a clear message |
| New material from the trainer | Unzip / copy the new week folder into your Git folder → **Commit & Push** |

## Problems?
| Symptom | Fix |
|---------|-----|
| *"Invalid Git credentials"* / 403 on push | Token without **Contents: Read and write**, or for another repo → create a new token (Step 4) and update it in **Linked accounts** |
| Git folder creation fails | Check the URL ends with `.git` and the repo is not empty (Step 3) |
| Upload on GitHub misses folders | Use Chrome/Edge and drag the **contents** of the folder, not the ZIP itself |
| Notebook shows as plain text | Import/create it inside the Git folder; `.py` files with `# Databricks notebook source` open as notebooks |

Post any error message in the Teams channel — we fix it together.
