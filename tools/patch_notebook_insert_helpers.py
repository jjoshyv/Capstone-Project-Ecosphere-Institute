#!/usr/bin/env python3
"""
tools/patch_notebook_insert_helpers.py

Usage:
    python tools/patch_notebook_insert_helpers.py path/to/notebook.ipynb

This script:
 - makes a timestamped backup of the notebook
 - inserts two cells after the first cell:
    1) robust helpers: try_read_parquet, try_read_csv
    2) a safe load cell that sets common paths and uses the helpers to load:
       df_features, trend_summary, forecast_summary
 - writes the modified notebook back to the same path.
"""
import json
import sys
from pathlib import Path
from datetime import datetime

HELPER_CELL_SOURCE = [
    "# Robust file-read helpers for the notebook\n",
    "from pathlib import Path\n",
    "import pandas as pd\n",
    "import traceback\n",
    "\n",
    "def try_read_parquet(path):\n",
    "    \"\"\"\n",
    "    Return a DataFrame if parquet exists and is readable, otherwise None.\n",
    "    \"\"\"\n",
    "    p = Path(path)\n",
    "    try:\n",
    "        if p.exists():\n",
    "            df = pd.read_parquet(p)\n",
    "            return df\n",
    "        return None\n",
    "    except Exception as e:\n",
    "        print(f\"⚠️ Failed to read parquet {p}: {e}\")\n",
    "        traceback.print_exc(limit=1)\n",
    "        return None\n",
    "\n",
    "def try_read_csv(path, parse_dates=None, nrows_preview=0):\n",
    "    \"\"\"\n",
    "    Read CSV if present. If parse_dates is given, inspect file columns first\n",
    "    and only pass the parse_dates entries that actually exist (avoids ValueError).\n",
    "    Returns DataFrame or None on missing file / failure.\n",
    "    - path: Path or str\n",
    "    - parse_dates: None or list/tuple/str of column(s) to parse as dates\n",
    "    \"\"\"\n",
    "    p = Path(path)\n",
    "    try:\n",
    "        if not p.exists():\n",
    "            return None\n",
    "\n",
    "        # If parse_dates is provided, check which of those columns exist in the file\n",
    "        cols_to_parse = None\n",
    "        if parse_dates:\n",
    "            # read only header to inspect column names (fast)\n",
    "            preview = pd.read_csv(p, nrows=nrows_preview)\n",
    "            available_cols = set(preview.columns.tolist())\n",
    "            if isinstance(parse_dates, (list, tuple)):\n",
    "                cols_to_parse = [c for c in parse_dates if c in available_cols]\n",
    "            else:\n",
    "                cols_to_parse = [parse_dates] if parse_dates in available_cols else []\n",
    "        # actually read with guarded parse_dates\n",
    "        if cols_to_parse:\n",
    "            return pd.read_csv(p, parse_dates=cols_to_parse)\n",
    "        else:\n",
    "            # read without parse_dates to avoid ValueError\n",
    "            return pd.read_csv(p)\n",
    "    except Exception as e:\n",
    "        print(f\"⚠️ Failed to read CSV {p}: {e}\")\n",
    "        traceback.print_exc(limit=1)\n",
    "        return None\n",
]

LOAD_CELL_SOURCE = [
    "# Auto-detected paths and safe loading using the helpers above\n",
    "from pathlib import Path\n",
    "cwd = Path.cwd()\n",
    "if (cwd / 'data_lake').exists():\n",
    "    project_root = cwd\n",
    "elif (cwd.parent / 'data_lake').exists():\n",
    "    project_root = cwd.parent\n",
    "else:\n",
    "    # fallback: if PROJECT_ROOT variable already set in notebook use it\n",
    "    try:\n",
    "        project_root\n",
    "    except NameError:\n",
    "        project_root = cwd  # last resort: assume current working dir\n",
    "    # we don't raise here to keep notebook non-fatal; prints follow\n",
    "\n",
    "print('Detected project root:', project_root)\n",
    "\n",
    "# default paths (adjust if your pipeline writes elsewhere)\n",
    "features_path = project_root / 'data_lake/feature_sets/features.parquet'\n",
    "trend_summary_path = project_root / 'analysis_outputs/trends/trend_summary.csv'\n",
    "forecast_summary_path = project_root / 'analysis_outputs/forecasts/forecast_summary.csv'\n",
    "\n",
    "# Use the robust helpers\n",
    "df_features = try_read_parquet(features_path)\n",
    "trend_summary = try_read_csv(trend_summary_path, parse_dates=['date'])\n",
    "forecast_summary = try_read_csv(forecast_summary_path, parse_dates=['date'])\n",
    "\n",
    "print('features:', 'found' if df_features is not None else 'missing')\n",
    "print('trend_summary:', 'found' if trend_summary is not None else 'missing')\n",
    "print('forecast_summary:', 'found' if forecast_summary is not None else 'missing')\n",
    "\n",
    "if df_features is not None:\n",
    "    print('✅ features shape:', df_features.shape)\n",
    "    print('Columns:', df_features.columns.tolist())\n",
    "\n",
    "# If a CSV was read without parse_dates but you want to parse later safely:\n",
    "if trend_summary is not None and 'date' in trend_summary.columns and not pd.api.types.is_datetime64_any_dtype(trend_summary['date']):\n",
    "    trend_summary['date'] = pd.to_datetime(trend_summary['date'], errors='coerce')\n",
]

def make_cell(source_lines, cell_type="code"):
    """
    Create a notebook cell structure with source_lines (list of strings).
    """
    return {
        "cell_type": cell_type,
        "metadata": {},
        "source": source_lines,
        # code cells typically include execution_count and outputs,
        # but leaving them out is acceptable; we include empty outputs/execution_count
        **({"outputs": [], "execution_count": None} if cell_type == "code" else {})
    }

def main(nb_path_str):
    nb_path = Path(nb_path_str)
    if not nb_path.exists():
        print("Notebook not found:", nb_path)
        return 1

    # load notebook JSON
    with nb_path.open("r", encoding="utf-8") as f:
        nb = json.load(f)

    # backup
    ts = datetime.now().strftime("%Y%m%dT%H%M%S")
    bak_path = nb_path.with_suffix(nb_path.suffix + f".bak.{ts}")
    with bak_path.open("w", encoding="utf-8") as f:
        json.dump(nb, f, indent=1, ensure_ascii=False)
    print("Backup written to:", bak_path)

    # Build cells
    helper_cell = make_cell(HELPER_CELL_SOURCE, cell_type="code")
    load_cell = make_cell(LOAD_CELL_SOURCE, cell_type="code")

    # Insert after first cell (index 1)
    cells = nb.get("cells", [])
    insert_index = 1 if len(cells) >= 1 else 0
    cells.insert(insert_index, helper_cell)
    cells.insert(insert_index + 1, load_cell)
    nb["cells"] = cells

    # write modified notebook back
    with nb_path.open("w", encoding="utf-8") as f:
        json.dump(nb, f, indent=1, ensure_ascii=False)
    print("Patched notebook written to:", nb_path)
    print("Inserted helper cell and load cell at index", insert_index)

    return 0

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python tools/patch_notebook_insert_helpers.py path/to/notebook.ipynb")
        sys.exit(2)
    sys.exit(main(sys.argv[1]))
