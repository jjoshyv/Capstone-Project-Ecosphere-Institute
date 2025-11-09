# extract_modis_timeseries.py
"""
Multi-year extractor for MODIS MCD12Q1 LC_Type1 (IGBP) values at a point.

Requirements:
  - rasterio, numpy, pandas, matplotlib
  - Run inside the conda environment where GDAL has HDF4 support (modis_env)

Usage:
  1) Put all your MCD12Q1 *.hdf files (2010..2019) into folder "MODIS_LandCover" inside project folder.
  2) Run: python extract_modis_timeseries.py
  3) Outputs:
       - landcover_timeseries.csv
       - a plotted trend chart (and saved PNG)
"""

import os
import glob
import rasterio
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from rasterio.warp import transform

# --- USER SETTINGS ---
DATA_FOLDER = "MODIS_LandCover"   # folder with all your MCD12Q1 .hdf files
OUTPUT_CSV = "landcover_timeseries.csv"
OUTPUT_PNG = "landcover_trend.png"

# coordinate to sample (Garinger, NC)
LAT = 35.23
LON = -80.80

# if True, compute mean of a small NxN window (must be odd). If False, sample single pixel.
NEIGHBORHOOD_SIZE = 3   # 1 => single pixel; 3 => 3x3 average, etc.

# IGBP legend
IGBP = {
    0: "Water",
    1: "Evergreen Needleleaf Forest",
    2: "Evergreen Broadleaf Forest",
    3: "Deciduous Needleleaf Forest",
    4: "Deciduous Broadleaf Forest",
    5: "Mixed Forests",
    6: "Closed Shrublands",
    7: "Open Shrublands",
    8: "Woody Savannas",
    9: "Savannas",
    10: "Grasslands",
    11: "Permanent Wetlands",
    12: "Croplands",
    13: "Urban and Built-up",
    14: "Cropland/Natural Vegetation Mosaic",
    15: "Snow and Ice",
    16: "Barren or Sparsely Vegetated",
    254: "Unclassified",
    255: "Fill Value"
}

def find_lc_subdataset(hdf_path):
    """Return the subdataset string for LC_Type1 (if exists) else None."""
    with rasterio.open(hdf_path) as hdf:
        for s in hdf.subdatasets:
            if "LC_Type1" in s or s.endswith(":LC_Type1"):
                return s
    return None

def sample_value_from_subdataset(subdataset, lon, lat, neighbourhood=1):
    """Open the subdataset and sample value(s) at lon, lat (WGS84). Returns numeric value or mean."""
    with rasterio.open(subdataset) as src:
        # ensure there is at least one band
        if src.count < 1:
            raise RuntimeError(f"Subdataset has no bands: {subdataset}")

        # Convert lon/lat -> dataset CRS if needed
        ds_crs = src.crs
        if ds_crs is None:
            # no CRS, assume lon/lat (rare)
            x_src, y_src = lon, lat
        else:
            # transform from EPSG:4326 to dataset CRS
            xs, ys = transform("EPSG:4326", ds_crs, [lon], [lat])
            x_src, y_src = xs[0], ys[0]

        # get raster indices (row, col)
        try:
            row, col = src.index(x_src, y_src)
        except Exception as e:
            raise RuntimeError(f"Failed to convert coordinates to pixel index: {e}")

        # boundary check
        if not (0 <= row < src.height and 0 <= col < src.width):
            raise RuntimeError("Coordinate is outside the raster bounds.")

        arr = src.read(1)  # safe because src.count >= 1

        if neighbourhood <= 1:
            val = arr[row, col]
            return int(val)
        else:
            half = neighbourhood // 2
            r0 = max(0, row - half)
            r1 = min(src.height, row + half + 1)
            c0 = max(0, col - half)
            c1 = min(src.width, col + half + 1)
            window = arr[r0:r1, c0:c1]
            # ignore fill values 255 and 254 as needed
            valid = window[np.isin(window, list(IGBP.keys()), invert=False)]
            if valid.size == 0:
                return int(np.nan)
            mean_val = np.round(np.nanmean(valid)).astype(int)
            return int(mean_val)

def extract_timeseries(data_folder, lon, lat, neighbourhood=1):
    pattern = os.path.join(data_folder, "*.hdf")
    files = sorted(glob.glob(pattern))
    if len(files) == 0:
        raise SystemExit(f"No .hdf files found in {data_folder}. Put your MCD12Q1 files there.")

    rows = []
    for f in files:
        # infer year from filename if possible (looks for AYYYYxxx)
        basename = os.path.basename(f)
        year = None
        # try patterns like A2019001 or .A2019...
        import re
        m = re.search(r"A(\d{4})", basename)
        if m:
            year = int(m.group(1))
        else:
            # fallback to file modified year
            year = int(pd.to_datetime(os.path.getmtime(f), unit="s").year)

        print(f"Processing {basename} -> year {year} ...")
        lc_sub = find_lc_subdataset(f)
        if lc_sub is None:
            print("  WARNING: LC_Type1 subdataset not found — skipping file.")
            continue

        try:
            val = sample_value_from_subdataset(lc_sub, lon, lat, neighbourhood=neighbourhood)
        except Exception as e:
            print(f"  ERROR reading {basename}: {e}")
            val = np.nan

        rows.append({"Year": year, "File": basename, "LC_Code": val, "LC_Name": IGBP.get(int(val), "Unknown" if not np.isnan(val) else np.nan)})

    df = pd.DataFrame(rows).sort_values("Year").reset_index(drop=True)
    return df

def plot_timeseries(df, out_png):
    if df.empty:
        print("No data to plot.")
        return
    # plot LC code (discrete) and annotate class names
    plt.figure(figsize=(8,4))
    plt.plot(df["Year"], df["LC_Code"], marker="o", linestyle="-")
    for i, r in df.iterrows():
        plt.text(r["Year"], r["LC_Code"] + 0.2, str(r["LC_Code"]) + " - " + str(r["LC_Name"]), fontsize=9, ha="center")
    plt.xlabel("Year")
    plt.ylabel("MODIS LC_Type1 Code")
    plt.title(f"Land Cover (LC_Type1) at ({LAT}, {LON})")
    plt.grid(axis="y", linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(out_png, dpi=200)
    plt.show()

if __name__ == "__main__":
    df = extract_timeseries(DATA_FOLDER, LON, LAT, neighbourhood=NEIGHBORHOOD_SIZE)
    print("\nExtracted results:")
    print(df)
    # Save CSV
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"\nSaved CSV -> {OUTPUT_CSV}")
    # Plot
    plot_timeseries(df, OUTPUT_PNG)
    print(f"Saved plot -> {OUTPUT_PNG}")
