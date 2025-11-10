# 🧠 Feature Engineering Pipeline — Capstone Project

This document describes the **feature engineering workflow** implemented in  
`etl/feature_engineering.py` for the Capstone Project.  
It converts cleaned datasets into **analysis-ready feature datasets** with rolling averages,  
cumulative metrics, spatial aggregates, and optional dimensionality reduction (PCA).  

All outputs are stored in the **`data_lake/feature_sets/`** directory.

---

## 🌍 Overview

The pipeline automates the process of generating analysis features such as:
- **Rolling averages** (configurable window in months, e.g., 5-year temperature trends)
- **Cumulative metrics** (e.g., annual CO₂ emissions or pollution accumulation)
- **Spatial aggregates** (averages by region or monitoring station)
- **PCA-based dimensionality reduction** (for multi-band satellite data)

The script produces both:
1. A **Parquet dataset** containing derived features.
2. A **metadata JSON** file logging configuration, input details, and feature parameters.

---

## ⚙️ Features Implemented

| Feature | Description |
|----------|--------------|
| **Auto-discovery** | Finds `Cleaned_*.csv` automatically under project root if `--input` not specified |
| **Case-insensitive columns** | Automatically detects `date`, `Date`, or `DATE` etc. |
| **Time dimensions** | Adds `year`, `month`, and `year_month` columns |
| **Rolling averages** | Computes rolling averages (window configurable via `--rolling-window-months`) |
| **Cumulative metrics** | Optional cumulative sums with `--compute-cumulative` |
| **Spatial aggregates** | Averages data by `region` or `location` column |
| **Optional PCA** | Reduces multivariate satellite data to N components |
| **Output formats** | Writes `.parquet` and `.json` (metadata) |
| **Metadata logging** | Records all parameters and statistics in JSON for reproducibility |

---

## 📁 File Locations

| File / Folder | Description |
|----------------|--------------|
| `etl/feature_engineering.py` | Main feature engineering script |
| `data_lake/feature_sets/` | Directory for output feature data |
| `data_lake/feature_sets/features.parquet` | Parquet dataset of engineered features |
| `data_lake/feature_sets/feature_ingest_metadata.json` | Metadata for each run |

---

## ▶️ Usage Examples

### 1️⃣ Basic run (auto-discover CSV)
```bash
python etl/feature_engineering.py \
  --out-root "data_lake/feature_sets" \
  --value-col 03_ug_m3 \
  --date-col date \
  --rolling-window-months 60

python etl/feature_engineering.py \
  --input "Cleaned_EPA_O3_Monthly.csv" \
  --out-root "data_lake/feature_sets" \
  --value-col 03_ug_m3 \
  --date-col Date \
  --location-col location \
  --rolling-window-months 60 \
  --compute-cumulative

python etl/feature_engineering.py \
  --input "Cleaned_Satellite.csv" \
  --out-root "data_lake/feature_sets" \
  --pca-cols band1,band2,band3 \
  --pca-n 3 \
  --pca-model data_lake/models/pca_satellite.joblib

---
