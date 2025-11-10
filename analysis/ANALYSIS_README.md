# 🔬 Analysis Pipeline — Capstone Project

This document describes the **analysis workflow** implemented in the `analysis/` folder of the Capstone Project.

It builds on the feature-engineered datasets generated in `etl/feature_engineering.py` and performs:
- 📈 **Trend Analysis**
- 🔮 **Forecasting**
- 🧩 **Clustering by Climate Similarity**
- ⚙️ **Uncertainty Evaluation**

All analytical outputs (charts, CSV summaries, metadata JSONs) are stored in the `analysis_outputs/` directory.

---

## 🌍 Overview

The analysis stage converts processed features (e.g., air quality, temperature, or emissions data) into insights about **temporal evolution, future projections, and spatial similarity**.

| Step | Objective | Output |
|------|------------|---------|
| **Trend Analysis** | Identify long-term increase/decrease rates | Slope (per year), significance (r-value, p-value), trend charts |
| **Forecasting** | Predict future values (e.g., next 10 years) using Prophet/SARIMAX | Forecast CSVs & PNG charts |
| **Clustering** | Group similar regions based on multi-metric feature vectors | Cluster labels, summary tables, scatter plots |
| **Uncertainty** | Quantify slope/forecast reliability | Confidence intervals, error metrics (RMSE, MAE) |

---

## ⚙️ Trend Analysis

### Script
`analysis/7_trend_analysis.py`

### Description
Performs per-location **linear regression** on the selected variable (e.g., O₃, temperature, CO₂) using NumPy-based OLS.

Automatically:
- Detects `date`, `value`, and `location` columns (case-insensitive)
- Converts timestamps to fractional years
- Fits line: `y = a*x + b`
- Computes:
  - slope per year
  - intercept
  - Pearson correlation (r)
  - standard error of slope
- Saves a PNG plot per location and a CSV summary

### Input
- `data_lake/feature_sets/features.parquet`

### Outputs
| File / Folder | Description |
|----------------|--------------|
| `analysis_outputs/trends/trend_summary.csv` | CSV table with per-location slopes and stats |
| `analysis_outputs/trends/plots/` | PNG charts showing observed vs fitted trends |
| `analysis_outputs/trends/trend_run_metadata.json` | Metadata (columns, inputs, parameters) |

### Example Run
```bash
python analysis/7_trend_analysis.py \
  --value-col o3_ug_m3 \
  --date-col date \
  --location-col location \
  --out-root analysis_outputs/trends
