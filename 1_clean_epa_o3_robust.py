# 1_clean_epa_o3_robust.py
"""
Robust EPA O3 loader + monthly aggregator.

Place this file in your project folder and run:
  conda activate modis_env
  python 1_clean_epa_o3_robust.py

Outputs:
  - Cleaned_EPA_O3_Monthly.csv
This script prints a short diagnostics table to the terminal as it runs.
"""

import pandas as pd
import glob, os, re
from dateutil import parser

# --- helper functions -----------------------------------------------------
def find_date_column(cols):
    # prefer exact matches, then keywords
    prefs = ['date local', 'date_local', 'date', 'measurement date', 'utc']
    for p in prefs:
        for c in cols:
            if c.strip().lower() == p:
                return c
    # then search for any column containing 'date'
    for c in cols:
        if re.search(r'date', c, re.I):
            return c
    return None

def find_o3_column(cols):
    # match common O3 names / patterns used in your files
    patterns = [
        r'arithmetic', r'8-?hour', r'daily.*max', r'o3', r'ozone', r'daily.*obs', r'daily.*avg',
        r'Daily Max 8-hour Ozone Concentration', r'PM2.5', r'VALUE'  # some fallbacks
    ]
    # first try exact-case-insensitive match among columns
    for p in patterns:
        for c in cols:
            if re.search(p, c, re.I):
                return c
    return None

def find_unit_column(cols):
    for c in cols:
        if re.search(r'unit', c, re.I) or re.search(r'units', c, re.I):
            return c
    # sometimes unit is part of header like "Units of Measure"
    for c in cols:
        if 'unit' in c.lower() or 'measure' in c.lower():
            return c
    return None

def parse_date_safe(x):
    try:
        return pd.to_datetime(x)
    except Exception:
        try:
            return pd.to_datetime(parser.parse(str(x)))
        except Exception:
            return pd.NaT

def to_ugm3(val, unit):
    if pd.isna(unit):
        return val
    s = str(unit).lower()
    try:
        v = float(val)
    except:
        return float('nan')
    if 'ppm' in s:
        return v * 2140.0
    if 'µg' in s or 'ug' in s or 'ug/m3' in s or 'µg/m' in s:
        return v
    # some datasets store "ppb" — convert 1 ppb O3 ≈ 2.14 µg/m3 (temp dependent). We'll assume 25°C:
    if 'ppb' in s:
        return v * 2.14
    # fallback: assume provided value already in µg/m3
    return v

# --- main ---------------------------------------------------------------
files = sorted(glob.glob("EPAair_O3_GaringerNC*_raw.csv"))
if not files:
    raise SystemExit("No EPA CSV files found with pattern EPAair_O3_GaringerNC*_raw.csv")

print("Found files:", len(files))
all_cols = {}
parsed_frames = []
warnings = []

for f in files:
    print("\n--- Loading:", os.path.basename(f))
    try:
        df = pd.read_csv(f, low_memory=False)
    except Exception as e:
        print("  ERROR reading file:", e)
        warnings.append((f, "read_error", str(e)))
        continue

    cols = list(df.columns)
    all_cols[os.path.basename(f)] = cols
    print("  Columns (first 12):", cols[:12])

    date_col = find_date_column(cols)
    o3_col = find_o3_column(cols)
    unit_col = find_unit_column(cols)

    if date_col is None or o3_col is None:
        # print brief hint and skip file
        print(f"  WARNING: couldn't auto-detect date or O3 col. date_col={date_col}, o3_col={o3_col}")
        warnings.append((f, "detect_failed", f"date:{date_col}, o3:{o3_col}"))
        continue

    # rename to standard names
    df = df.rename(columns={date_col: 'Date_raw', o3_col: 'O3_raw'})
    if unit_col:
        df = df.rename(columns={unit_col: 'Unit_raw'})

    # parse date robustly
    df['Date_parsed'] = df['Date_raw'].apply(parse_date_safe)
    df = df.dropna(subset=['Date_parsed'])
    # convert O3 numeric
    df['O3_raw'] = pd.to_numeric(df['O3_raw'], errors='coerce')
    df = df.dropna(subset=['O3_raw'])

    if 'Unit_raw' in df.columns:
        df['Unit_raw'] = df['Unit_raw'].astype(str)
        df['O3_ug_m3'] = df.apply(lambda r: to_ugm3(r['O3_raw'], r['Unit_raw']), axis=1)
    else:
        # No unit column: assume numeric is already µg/m3 or arithmetic mean; still keep raw
        df['O3_ug_m3'] = df['O3_raw']

    parsed = df[['Date_parsed','O3_ug_m3']].rename(columns={'Date_parsed':'Date'})
    parsed_frames.append(parsed)

# summary of columns detected
print("\n=== Column diagnostics across files ===")
for fname, cols in all_cols.items():
    print(f" {fname}: {len(cols)} cols - {cols[:8]}{' ...' if len(cols)>8 else ''}")

# warnings
if warnings:
    print("\n=== Warnings / skipped files ===")
    for w in warnings:
        print(" ", os.path.basename(w[0]), "-", w[1], "-", w[2])

# concat parsed frames
if not parsed_frames:
    raise SystemExit("No files parsed successfully. See warnings above.")

epa = pd.concat(parsed_frames, ignore_index=True)
epa['Date'] = pd.to_datetime(epa['Date'])
epa = epa.set_index('Date').sort_index()

# resample monthly mean
epa_monthly = epa.resample('M').mean()
epa_monthly = epa_monthly.reset_index()
epa_monthly['Date'] = epa_monthly['Date'].dt.to_period('M').dt.to_timestamp()

# save
out = "Cleaned_EPA_O3_Monthly.csv"
epa_monthly.to_csv(out, index=False)
print(f"\nSaved cleaned monthly EPA O3 -> {out} (rows: {len(epa_monthly)})")
print(epa_monthly.head(12))
