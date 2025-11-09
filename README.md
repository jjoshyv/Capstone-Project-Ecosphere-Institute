# 🌍 Capstone Project — Ecosphere Institute  
### Automated Data Ingestion and Partitioned Data Lake using Python & PyArrow

---

## 🧠 Project Overview
This project implements an **automated ETL (Extract, Transform, Load) data pipeline** that ingests cleaned environmental datasets and stores them efficiently in a **data lake** (Parquet + DuckDB).  
It is part of the Ecosphere Institute Capstone Initiative to build data-driven, scalable, and query-optimized systems for environmental analytics.

---

## 🎯 Project Goals

✅ Automate ingestion of cleaned datasets into a structured data lake or database  
✅ Partition data **by time (monthly/yearly)** and **by location (region/country)** for faster queries  
✅ Design a **common storage schema** and enforce **schema validation**  
✅ Use **Python scripts** to manage data loading, transformation, and metadata tracking  
✅ Document and version-control all scripts in a **GitHub repository**

---

## ⚙️ Tech Stack

| Category | Technology |
|-----------|-------------|
| **Programming** | Python 3.11 |
| **Libraries** | `pandas`, `pyarrow`, `duckdb`, `uuid`, `argparse`, `logging` |
| **Data Storage** | Parquet files (Data Lake), DuckDB (Local DB) |
| **Version Control** | Git + GitHub |
| **Environment Management** | Conda virtual environment (`modis_env`) |

---

## 🗂️ Directory Structure

Capstone-Project-Ecosphere-Institute/
│
├── etl/
│ ├── py_etl_parquet.py # Core ETL script for Parquet ingestion
│ ├── py_etl_parquet_with_metadata.py # Extended ETL with metadata + validation
│ ├── spark_etl_parquet.py # (Optional) Spark version for big data
│
├── data_lake/ # Auto-created partitioned Parquet data
│ ├── epa_o3_parquet/
│ ├── year=2020/month=01/part-.parquet
│ ├── year=2021/month=02/part-.parquet
│
├── fIGURES/ # Visualizations & diagrams
│
├── capstone.duckdb # Local analytical database
├── requirements.txt # Python dependencies
├── README.md # Project documentation (this file)
├── .gitignore # Ignored data, env, and temp files
└── cleaned_datasets/ # Input cleaned CSVs (EPA, NASA, etc.)


---

## 🧩 Features

- **Automated CSV discovery**  
  Finds cleaned datasets automatically (`Cleaned_*.csv`) unless specified with `--csv`.

- **Schema Validation**  
  Checks for essential columns (`date`, `O3_ug_m3`, `region`, etc.) before ingestion.

- **Data Partitioning**  
  Writes data to Parquet partitioned by `year` and `month` (and optional `region`).

- **Metadata Tracking**  
  Generates metadata in JSON + SQLite for every ingestion run.

- **DuckDB Integration**  
  Optionally loads Parquet data into a local analytical database.

- **Lightweight & Reproducible**  
  Runs fully in Python — no Spark or Hadoop required.

---

## 🧰 Installation

### 1️⃣ Clone the Repository

```bash
git clone https://github.com/jjoshyv/Capstone-Project-Ecosphere-Institute.git
cd Capstone-Project-Ecosphere-Institute


