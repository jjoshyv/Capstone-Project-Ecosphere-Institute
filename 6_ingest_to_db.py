# 6_ingest_to_db.py
"""
Ingest cleaned CSVs into a database.
By default uses SQLite file 'capstone_data.db' in project folder.
Change DATABASE_URL to point to PostgreSQL if needed.
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text

# ---- CONFIG ----
# SQLite (default, file will be created in project folder)
DB_PATH = "capstone_data.db"
DATABASE_URL = f"sqlite:///{DB_PATH}"   # <-- change to PostgreSQL below if needed

# Example PostgreSQL connection string (comment/uncomment as appropriate)
# DATABASE_URL = "postgresql+psycopg2://username:password@host:port/dbname"

# CSV file names (must exist in project folder)
EPA_CSV = "Cleaned_EPA_O3_Monthly.csv"
NASA_CSV = "Cleaned_NASA_POWER_Monthly.csv"
MERGED_CSV = "Merged_Dataset.csv"  # optional

# ---- FUNCTIONS ----
def load_if_exists(path):
    if os.path.exists(path):
        print(f"Loading {path} ...")
        return pd.read_csv(path, parse_dates=["Date"], low_memory=False)
    else:
        print(f"Warning: {path} not found. Skipping.")
        return None

def ingest_df_to_sql(df, table_name, engine, index=False):
    if df is None:
        return
    # Ensure Date column is datetime for SQL friendly type
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"])
    print(f"Writing table '{table_name}' ({len(df)} rows) ...")
    df.to_sql(table_name, con=engine, if_exists="replace", index=index)
    print("Done.")

def create_index_sqlite(engine, table_name, column):
    # SQLite: create index if not exists
    with engine.begin() as conn:
        idx_name = f"idx_{table_name}_{column}"
        sql = text(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name} ({column});")
        conn.execute(sql)
        print(f"Index created (if not exists): {idx_name}")

def create_index_postgres(engine, table_name, column):
    # PostgreSQL index creation (idempotent)
    with engine.begin() as conn:
        idx_name = f"idx_{table_name}_{column}"
        sql = text(
            f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name} ({column});"
        )
        conn.execute(sql)
        print(f"Index created (if not exists): {idx_name}")

def main():
    engine = create_engine(DATABASE_URL, echo=False, future=True)
    print("Connected to:", DATABASE_URL)

    epa = load_if_exists(EPA_CSV)
    nasa = load_if_exists(NASA_CSV)
    merged = load_if_exists(MERGED_CSV)

    # Ingest
    ingest_df_to_sql(epa, "epa_o3", engine)
    ingest_df_to_sql(nasa, "nasa_power", engine)
    ingest_df_to_sql(merged, "merged_data", engine)

    # Create indexes on Date columns (for quicker time-series queries)
    for tbl in ["epa_o3", "nasa_power", "merged_data"]:
        # only create if table exists
        with engine.connect() as conn:
            res = conn.execute(text(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=:t"
            ), {"t": tbl})
            if res.first():
                # choose index function based on dialect
                if engine.dialect.name == "sqlite":
                    create_index_sqlite(engine, tbl, "Date")
                else:
                    create_index_postgres(engine, tbl, "Date")

    print("\nAll done. Database file:", DB_PATH)
    print("You can open it with DB Browser for SQLite or connect via SQLAlchemy")

if __name__ == "__main__":
    main()
