# 3_merge_datasets.py
"""
Merge Cleaned EPA O3 monthly + Cleaned NASA POWER monthly + (optional) MODIS annual landcover.

Usage:
    conda activate modis_env
    python 3_merge_datasets.py

Outputs:
    - Merged_Dataset.csv
"""

import pandas as pd
import os

# --- helper to try multiple filenames for landcover ---
def try_load_landcover(candidates):
    for fn in candidates:
        if os.path.exists(fn):
            print(f"Loading landcover from: {fn}")
            try:
                lc = pd.read_csv(fn)
                return lc, fn
            except Exception as e:
                print(f"  Failed to read {fn}: {e}")
    return None, None

# --- load cleaned monthly EPA O3 ---
epa_fn = "Cleaned_EPA_O3_Monthly.csv"
if not os.path.exists(epa_fn):
    raise SystemExit(f"EPA file not found: {epa_fn} — please run 1_clean_epa_o3_robust.py first")

epa = pd.read_csv(epa_fn)
epa['Date'] = pd.to_datetime(epa['Date'], errors='coerce')
epa = epa.dropna(subset=['Date']).sort_values('Date').reset_index(drop=True)
print(f"Loaded EPA: {epa_fn} rows={len(epa)} columns={epa.columns.tolist()}")

# --- load cleaned NASA POWER monthly ---
nasa_fn = "Cleaned_NASA_POWER_Monthly.csv"
if not os.path.exists(nasa_fn):
    raise SystemExit(f"NASA file not found: {nasa_fn} — please run 2_clean_nasa_power.py first")

nasa = pd.read_csv(nasa_fn)
nasa['Date'] = pd.to_datetime(nasa['Date'], errors='coerce')
nasa = nasa.dropna(subset=['Date']).sort_values('Date').reset_index(drop=True)
print(f"Loaded NASA: {nasa_fn} rows={len(nasa)} columns={nasa.columns.tolist()}")

# --- align ranges: keep intersection (inner join on Date) ---
merged = pd.merge(epa, nasa, on='Date', how='inner', suffixes=('_EPA', '_NASA'))
print(f"Merged EPA+NASA: rows={len(merged)} cols={merged.columns.tolist()}")

# add Year for landcover merging
merged['Year'] = merged['Date'].dt.year

# --- try to find a landcover timeseries CSV (annual) ---
land_candidates = [
    "landcover_timeseries.csv",
    "landcover_timeseries_modis.csv",
    "extract_modis_timeseries.csv",
    "extract_modis_timeseries_out.csv",
    "modis_landcover_yearly.csv",
    "landcover_yearly.csv",
    "landcover_timeseries_garinger.csv",
    "landcover_timeseries_Garinger.csv"
]

landcover, land_fn = try_load_landcover(land_candidates)

if landcover is not None:
    print("Landcover columns:", list(landcover.columns)[:8])
    # Try to find year column in landcover
    year_col_candidates = [c for c in landcover.columns if 'year' in c.lower()]
    if not year_col_candidates:
        # try index or first column
        print("No explicit 'year' column found in landcover — attempting to infer from first column or filename.")
        landcover = landcover.reset_index().rename(columns={'index': 'Year'})
    else:
        ycol = year_col_candidates[0]
        landcover = landcover.rename(columns={ycol: 'Year'})

    # ensure Year is integer
    landcover['Year'] = pd.to_numeric(landcover['Year'], errors='coerce').astype('Int64')
    # Merge by Year (left join keeps monthly rows)
    merged = pd.merge(merged, landcover, on='Year', how='left', suffixes=('','_LC'))
    print(f"Merged with landcover ({land_fn}): rows={len(merged)} columns now: {merged.columns.tolist()}")
else:
    print("No landcover file found. Looked for:", land_candidates)
    print("Proceeding without landcover. You can supply a yearly CSV later and re-run merge.")

# --- diagnostics & export ---
out_fn = "Merged_Dataset.csv"
merged.to_csv(out_fn, index=False)
print(f"\nSaved merged dataset -> {out_fn} (rows: {len(merged)})")

# Print head and missing value summary
print("\nMerged head (first 12 rows):")
print(merged.head(12).to_string(index=False))

print("\nMissing values per column:")
print(merged.isna().sum().sort_values(ascending=False))
