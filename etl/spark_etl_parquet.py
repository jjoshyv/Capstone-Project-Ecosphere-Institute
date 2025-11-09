# etl/spark_etl_parquet.py
"""
Robust ETL to read a cleaned CSV (auto-discover if necessary),
transform and write partitioned parquet output to data_lake/epa_o3_parquet.

- Auto-discovers Cleaned_*.csv under project root if CLEANED_CSV is None
- Normalizes column names (lowercase, strip)
- Adds year/month columns, selects a safe set of columns
- Chooses partitions based on available columns (year [+ site_id if present])
- Prints helpful diagnostics for HADOOP_HOME / winutils on Windows
"""

import os
from pathlib import Path
import sys
from typing import Optional, List

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame


# ---------- CONFIG ----------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
# If you want to hardcode a specific CSV path, change this to the absolute path:
CLEANED_CSV: Optional[Path] = None
# Output folder (parquet)
OUT_DIR = PROJECT_ROOT / "data_lake"
OUT_NAME = "epa_o3_parquet"
# ----------------------------

def pretty_print(msg: str):
    print(msg)
    sys.stdout.flush()

def find_cleaned_csv(root: Path) -> List[Path]:
    """Search for candidate cleaned CSVs under project root."""
    patterns = [
        "Cleaned_*.csv",
        "*Cleaned*.csv",
        "**/Cleaned_*.csv",
        "**/*Cleaned*.csv",
        "**/*epa*.csv",
        "**/*o3*.csv",
    ]
    found = []
    for pat in patterns:
        found.extend([p for p in root.glob(pat) if p.is_file()])
    # deduplicate, prefer those at project root
    found = sorted({p.resolve(): p for p in found}.values(), key=lambda p: (len(p.parts), str(p)))
    return found

def normalize_column_names(df: DataFrame) -> DataFrame:
    """Lowercase and strip columns, and rename common 'date' variants to 'date'."""
    old_cols = df.columns
    new_cols = []
    for c in old_cols:
        nc = c.strip().lower()
        # normalize common variants
        if nc in ("date", "dt", "measurement_date", "measurement_date_utc"):
            nc = "date"
        new_cols.append(nc)
    # apply rename mapping
    for oc, nc in zip(old_cols, new_cols):
        if oc != nc:
            df = df.withColumnRenamed(oc, nc)
    return df

def read_cleaned_csv(spark: SparkSession, explicit_path: Optional[Path]) -> DataFrame:
    """Read the cleaned CSV. Use explicit_path if provided else auto-discover."""
    # explicit path first
    if explicit_path:
        p = Path(explicit_path)
        if p.exists():
            pretty_print(f"📁 Reading cleaned CSV from explicit path: {p}")
            df = spark.read.option("header", True).option("inferSchema", True).csv(str(p))
            return normalize_column_names(df)
        else:
            raise FileNotFoundError(f"Cleaned CSV not found: {p}")

    # auto-discover
    candidates = find_cleaned_csv(PROJECT_ROOT)
    if not candidates:
        raise FileNotFoundError(f"No cleaned CSV found under project root: {PROJECT_ROOT}")
    if len(candidates) > 1:
        pretty_print("⚠️ Multiple candidate cleaned CSVs found — using the first one:")
        for c in candidates:
            pretty_print("   " + str(c))
    chosen = candidates[0]
    pretty_print(f"📁 Using discovered CSV: {chosen}")
    df = spark.read.option("header", True).option("inferSchema", True).csv(str(chosen))
    return normalize_column_names(df)


def ensure_dirs(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def main():
    # Create Spark session
    spark = (SparkSession.builder
             .appName("capstone-etl")
             .master("local[*]")
             .config("spark.sql.shuffle.partitions", "4")
             # ensure parquet commit works more predictably on local
             .config("spark.sql.parquet.writeLegacyFormat", "false")
             .getOrCreate())

    pretty_print(f"✅ Spark session started — version: {spark.version}")

    # Show HADOOP_HOME info (Windows users often need winutils)
    hadoop_home = os.environ.get("HADOOP_HOME") or os.environ.get("HADOOPHOME")
    pretty_print(f"HADOOP_HOME = {hadoop_home or 'Not Set'}")
    if os.name == "nt" and not hadoop_home:
        pretty_print("⚠️ Windows: HADOOP_HOME not set. If you see native IO errors when writing parquet,"
                     " download a matching winutils.exe and set HADOOP_HOME pointing to its parent folder.")

    # try to read CSV
    try:
        df = read_cleaned_csv(spark, CLEANED_CSV)
    except FileNotFoundError as e:
        pretty_print(f"❌ ETL process failed: {e}")
        spark.stop()
        return

    # show schema and sample
    pretty_print(f"📄 Input columns: {df.columns}")
    df.printSchema()
    pretty_print("✅ Cleaned sample:")
    df.show(5, truncate=False)

    # Make sure we have a 'date' column in date type
    if "date" not in [c.lower() for c in df.columns]:
        pretty_print("❌ ETL process failed: required 'date' column not found in CSV.")
        spark.stop()
        return

    # convert/ensure date column is date type
    df = df.withColumn("date", F.to_date(F.col("date")))

    # add year / month columns
    df = df.withColumn("year", F.year(F.col("date")).cast("integer")) \
           .withColumn("month", F.month(F.col("date")).cast("integer"))

    # ensure numeric column name(s) - try common ozone column names
    # We'll keep all columns but lowercase normalized earlier
    cols = [c.lower() for c in df.columns]
    # decide partition columns
    partition_cols = ["year"]
    if "site_id" in cols:
        partition_cols.append("site_id")
    elif "site" in cols:
        partition_cols.append("site")
    # final selection: ensure date + partition + at least one measurement column exist
    measurement_candidates = [c for c in cols if any(k in c for k in ("o3", "ozone"))]
    if not measurement_candidates:
        pretty_print("⚠️ No measurement column (o3/ozone) found — will proceed but check results.")
    selected_cols = ["date"] + (partition_cols if "site_id" in partition_cols else []) + measurement_candidates
    # if measurement empty, fallback to keeping all
    if not measurement_candidates:
        selected_cols = df.columns

    # select re-ordered columns if available
    select_present = [c for c in selected_cols if c in df.columns]
    df_out = df.select(*select_present, "year", "month") if "month" not in select_present else df.select(*select_present)

    pretty_print("🔎 Writing EPA Parquet to: " + str(OUT_DIR / OUT_NAME))
    ensure_dirs(OUT_DIR)

    # write partitioned parquet with safe options
    try:
        write_path = str((OUT_DIR / OUT_NAME).resolve())
        # if only partition by year (no site), use that
        partition_by = partition_cols if ("site_id" in partition_cols or len(partition_cols) == 1) else partition_cols
        # remove duplicates
        partition_by = list(dict.fromkeys(partition_by))
        pretty_print(f"Partitioning by: {partition_by}")
        (df_out.write
            .mode("overwrite")
            .partitionBy(*partition_by)
            .parquet(write_path))
        pretty_print("✅ EPA parquet saved -> " + write_path)
    except Exception as e:
        pretty_print("❌ ETL process failed: " + str(e))
        # print Java stacktrace details if available
        import traceback
        traceback.print_exc()
    finally:
        pretty_print("🛑 Spark session stopped.")
        spark.stop()


if __name__ == "__main__":
    main()
 