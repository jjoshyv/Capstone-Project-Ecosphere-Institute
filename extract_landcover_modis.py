# extract_landcover_modis_fixed.py
import rasterio
from rasterio.plot import show
from rasterio.warp import transform
import matplotlib.pyplot as plt

file_path = "MCD12Q1.A2019001.h10v05.061.2022169160646.hdf"

# 1) List subdatasets and pick LC_Type1
with rasterio.open(file_path) as hdf:
    print("🔍 Available subdatasets:")
    for i, s in enumerate(hdf.subdatasets):
        print(i, s)
    # find the one that endswith or contains 'LC_Type1' (IGBP)
    lc_sub = None
    for s in hdf.subdatasets:
        if "LC_Type1" in s or ":LC_Type1" in s:
            lc_sub = s
            break

if lc_sub is None:
    raise SystemExit("Could not find an LC_Type1 subdataset in the HDF file. Check subs dataset listing above.")

print("\n▶ Opening subdataset:", lc_sub)

# 2) Open the LC_Type1 subdataset
with rasterio.open(lc_sub) as src:
    print("CRS:", src.crs)
    print("Width x Height:", src.width, "x", src.height)
    print("Band count:", src.count)
    if src.count < 1:
        raise SystemExit("The opened subdataset has no raster bands (count=0).")

    # 3) Read band 1 safely
    band1 = src.read(1)   # now safe
    plt.figure(figsize=(10,6))
    show(band1, cmap="tab20")
    plt.title("MODIS LC_Type1 (IGBP) - 2019 (tile h10v05)")
    plt.show()

    # 4) Query at lat/lon (WGS84)
    lat, lon = 35.23, -80.80   # user coordinates
    src_crs = src.crs

    # If dataset CRS is not EPSG:4326, transform the lon/lat into dataset CRS
    if src_crs is not None and src_crs.to_string() != "EPSG:4326":
        xs, ys = transform({"init": "EPSG:4326"}, src_crs, [lon], [lat])  # returns lists
        x_src, y_src = xs[0], ys[0]
    else:
        x_src, y_src = lon, lat

    try:
        # src.index expects (x, y) in dataset CRS
        row, col = src.index(x_src, y_src)
    except Exception as e:
        raise SystemExit(f"Error converting coordinates to raster index: {e}")

    # boundary check
    if not (0 <= row < src.height and 0 <= col < src.width):
        raise SystemExit("Coordinate is outside the raster bounds. Check the coordinate or tile selection.")

    value = band1[row, col]
    print(f"\n🗺 Land cover value at (lat={lat}, lon={lon}): {value}")

# 5) Decode IGBP classes
igbp = {
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
    255: "Fill Value",
}

print("🏷 Land cover class:", igbp.get(int(value), "Unknown"))
