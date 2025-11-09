# 2_clean_nasa_power.py (fixed: build Date from YEAR + DOY)
import pandas as pd
import numpy as np

file = "NASA_POWER_Garinger_2010_2019.csv"

# load (skiprows may need adjustment if file has different header length)
df = pd.read_csv(file, skiprows=10, low_memory=False)
print("Columns found:", list(df.columns))
print("\nFirst few rows:\n", df.head())

# Ensure YEAR and DOY exist
if not {'YEAR','DOY'}.issubset(set(df.columns)):
    raise SystemExit("Expected YEAR and DOY columns in NASA POWER file. Found: " + ", ".join(df.columns))

# Build Date from YEAR + DOY (DOY = day of year)
# Some DOY values may be floats -> convert to int
df['YEAR'] = df['YEAR'].astype(int)
df['DOY'] = df['DOY'].astype(int)

# Create Date: Jan 1st of YEAR + (DOY - 1) days
df['Date'] = pd.to_datetime(df['YEAR'].astype(str) + '-01-01') + pd.to_timedelta(df['DOY'] - 1, unit='D')

# Pick relevant variables (adjust names if your file uses different codes)
# T2M = temperature at 2m, PRECTOTCORR = precipitation corrected
keep = ['Date']
for c in df.columns:
    up = c.upper()
    if 'T2M' in up or 'PRECTOT' in up or 'TEMP' in up or 'PRCP' in up or 'PRECIP' in up:
        keep.append(c)

df = df[keep].dropna(subset=['Date'])
df = df.sort_values('Date').reset_index(drop=True)

# Normalize column names
df = df.rename(columns={c: c.strip() for c in df.columns})

# Detect temperature columns and convert Kelvin->C if needed
temp_cols = [c for c in df.columns if 'T2M' in c.upper() or 'TEMP' in c.upper()]
for col in temp_cols:
    col_mean = df[col].dropna().mean() if not df[col].dropna().empty else np.nan
    if not np.isnan(col_mean) and col_mean > 100:  # likely Kelvin
        print(f"Converting {col} from Kelvin to Celsius (mean {col_mean:.1f})")
        df[col] = df[col] - 273.15
    else:
        print(f"{col} appears to be in Celsius (mean {col_mean:.2f}) or empty")

# For precipitation columns, assume units are mm/day (verify in file if needed)
precip_cols = [c for c in df.columns if 'PRECTOT' in c.upper() or 'PRECIP' in c.upper() or 'PRCP' in c.upper()]

# Resample to monthly:
# - Temperature: mean
# - Precipitation: sum
df = df.set_index('Date')

agg_map = {}
for c in df.columns:
    if c in temp_cols:
        agg_map[c] = 'mean'
    elif c in precip_cols:
        agg_map[c] = 'sum'
    else:
        agg_map[c] = 'mean'  # fallback

nasa_monthly = df.resample('M').agg(agg_map).reset_index()
# convert index to month start for consistency (optional)
nasa_monthly['Date'] = nasa_monthly['Date'].dt.to_period('M').dt.to_timestamp()

out = 'Cleaned_NASA_POWER_Monthly.csv'
nasa_monthly.to_csv(out, index=False)
print(f"\n✅ Saved {out} (rows: {len(nasa_monthly)})")
print(nasa_monthly.head(12))
