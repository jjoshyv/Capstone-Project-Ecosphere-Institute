# 🌍 Automated Data Ingestion and Partitioned Data Lake ETL Pipeline

This repository contains a modular and automated **ETL (Extract–Transform–Load)** pipeline built with **Python**, **Pandas**, and **PyArrow**, designed to ingest, transform, and store cleaned environmental datasets (e.g., EPA Ozone Data) into a **partitioned Parquet Data Lake**.  
The project simulates an end-to-end **data engineering workflow** with metadata tracking and optional **DuckDB** integration for fast analytical queries.

---

## 🚀 Project Overview

The pipeline automates the ingestion of cleaned CSV datasets, applies transformations, validates schemas, and loads data into a structured **Parquet Data Lake**.  
It also records metadata for every ETL run and supports partitioning by:
- **Time:** Year and Month  
- **Location:** Region, Country, or Site ID (optional)

This structure improves **query performance**, **data organization**, and **scalability** for analytical workloads.

---

## 🧩 Features

✅ **Automated CSV ingestion** — discovers cleaned CSVs or accepts a file path  
✅ **Case-insensitive schema validation** — handles column naming variations (`Date`, `date`, `DATE`)  
✅ **Partitioned Parquet output** — organized by year, month, and optional location  
✅ **Metadata tracking** — JSON + SQLite/DuckDB logs for auditability  
✅ **Auto-create missing columns** — optional fallback for missing location columns  
✅ **Chunked processing** — handles large datasets efficiently  
✅ **Plug-and-play** — runs standalone with minimal setup  

---

## 🏗️ Project Structure

