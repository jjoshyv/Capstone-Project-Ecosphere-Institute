#!/usr/bin/env python3
"""
analysis/9_clustering.py

Clustering workflow for Capstone project.

Reads engineered features (Parquet), optionally reduces dimensionality with PCA,
clusters locations using KMeans, and writes outputs to an analysis_outputs directory.

Key features:
 - Safe PCA: n_components auto-adjusted to min(n_samples, n_features).
 - Skips PCA if not possible (e.g., too few samples/features).
 - Graceful early-exit if not enough locations to cluster.
 - Writes per-location cluster assignments, summary CSV, PCA model and metadata JSON.

Usage examples:
    python analysis/9_clustering.py \
      --input data_lake/feature_sets/features.parquet \
      --value-col o3_ug_m3 \
      --date-col date \
      --location-col location \
      --out-root analysis_outputs/clusters \
      --pca-cols o3_ug_m3,o3_ug_m3_rolling_60m \
      --pca-n 2 \
      --k 4
"""

from __future__ import annotations
import argparse
import json
import logging
from pathlib import Path
from typing import List, Optional, Dict

import numpy as np
import pandas as pd

from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import joblib
import datetime

# -------------------- Logging --------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")
log = logging.getLogger("clustering")

# -------------------- Helpers --------------------
def parse_list_arg(s: Optional[str]) -> Optional[List[str]]:
    if s is None:
        return None
    if isinstance(s, list):
        return s
    return [c.strip() for c in s.split(",") if c.strip()]

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def read_features(path: Path) -> pd.DataFrame:
    log.info(f"Loading input: {path}")
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")
    df = pd.read_parquet(path)
    log.info(f"Loaded dataframe with columns: {list(df.columns)}")
    return df

def pick_columns_case_insensitive(df: pd.DataFrame, col_name: str) -> Optional[str]:
    """Return actual column in df that matches col_name case-insensitively, or None."""
    if col_name in df.columns:
        return col_name
    lower_map = {c.lower(): c for c in df.columns}
    return lower_map.get(col_name.lower())

# -------------------- Main functionality --------------------
def prepare_feature_matrix(
    df: pd.DataFrame,
    value_col: str,
    date_col: str,
    location_col: str,
    pca_cols: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Prepare the per-location feature matrix for clustering.
    By default, we will aggregate to one row per location using last available values
    of the requested columns (or mean if multiple).
    """
    # normalize column names using case-insensitive match
    used_cols = {}
    for key in (value_col, date_col, location_col):
        if key is None:
            continue
        actual = pick_columns_case_insensitive(df, key)
        if actual is None:
            raise ValueError(f"Column '{key}' not found in dataframe. Available: {list(df.columns)}")
        used_cols[key] = actual

    # If pca_cols provided, ensure they exist (case-insensitive)
    actual_pca_cols = []
    if pca_cols:
        for c in pca_cols:
            actual = pick_columns_case_insensitive(df, c)
            if actual is None:
                log.warning(f"PCA column '{c}' not found in dataframe; skipping it.")
            else:
                actual_pca_cols.append(actual)

    # Ensure date is datetime
    if used_cols.get(date_col) is not None:
        df[used_cols[date_col]] = pd.to_datetime(df[used_cols[date_col]], errors="coerce")

    # If we have PCA columns, use those aggregated per-location.
    # Otherwise, fallback to using provided value_col (one numeric per location).
    group_by = used_cols[location_col]
    if actual_pca_cols:
        # Aggregate by mean per location for each selected pca col
        agg_df = df.groupby(group_by)[actual_pca_cols].mean().reset_index()
        log.info(f"Prepared matrix using PCA columns: {actual_pca_cols}. Shape: {agg_df.shape}")
        return agg_df.set_index(group_by)
    else:
        # Use the single value_col: aggregate mean per location
        actual_value_col = used_cols[value_col]
        agg_df = df.groupby(group_by)[actual_value_col].mean().reset_index()
        agg_df = agg_df.rename(columns={actual_value_col: "value"})
        log.info(f"Prepared matrix using value column '{actual_value_col}'. Shape: {agg_df.shape}")
        return agg_df.set_index(group_by)

def safe_pca_transform(X: np.ndarray, requested_n: Optional[int], random_state: int = 0):
    """
    Perform PCA safely:
     - compute max_allowed = min(n_samples, n_features)
     - set pca_n = min(requested_n or default 2, max_allowed)
     - if pca_n <= 0 or pca_n == n_features (no reduction), optionally skip PCA.
    Returns (X_transformed, pca_model or None, pca_n_used)
    """
    n_samples, n_features = X.shape
    max_allowed = min(n_samples, n_features)
    if requested_n is None:
        requested_n = 2  # sensible default
    pca_n = min(requested_n, max_allowed)
    if pca_n <= 0:
        log.info("PCA skipped: not enough samples/features.")
        return X, None, 0
    # If pca_n equals n_features, PCA would not reduce dimensionality — still allowed but unnecessary.
    if pca_n == n_features:
        log.info("PCA skipped: requested components equal number of features (no reduction).")
        return X, None, n_features
    log.info(f"Running PCA with n_components={pca_n} (requested={requested_n}, max_allowed={max_allowed})")
    pca = PCA(n_components=pca_n, random_state=random_state)
    X_pca = pca.fit_transform(X)
    return X_pca, pca, pca_n

def run_clustering(
    input_path: Path,
    value_col: str,
    date_col: str,
    location_col: str,
    out_root: Path,
    pca_cols: Optional[List[str]],
    pca_n: Optional[int],
    k: int,
    random_state: int = 0,
) -> Dict:
    df = read_features(input_path)

    # Prepare matrix (index = location)
    matrix_df = prepare_feature_matrix(df, value_col, date_col, location_col, pca_cols=pca_cols)

    n_locations = matrix_df.shape[0]
    log.info(f"Prepared dataframe with {n_locations} locations for clustering.")

    if n_locations < 2:
        log.warning("Not enough distinct locations to perform clustering (need >=2). Exiting gracefully.")
        meta = {
            "input": str(input_path),
            "n_locations": n_locations,
            "status": "skipped_not_enough_locations",
            "timestamp": datetime.datetime.utcnow().isoformat(),
        }
        # ensure outputs directory exists and write metadata
        ensure_dir(out_root)
        (out_root / "clustering_metadata.json").write_text(json.dumps(meta, indent=2))
        return meta

    # Build feature matrix X (rows=locations, cols=features)
    X_raw = matrix_df.values.astype(float)  # may raise if non-numeric; that's reasonable
    loc_index = matrix_df.index.astype(str).tolist()

    # Standardize
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_raw)
    log.info(f"Standardized features (shape={X_scaled.shape}).")

    # Safe PCA
    X_transformed, pca_model, pca_n_used = safe_pca_transform(X_scaled, pca_n, random_state=random_state)

    # Choose data to cluster
    X_for_clustering = X_transformed if pca_model is not None else X_scaled

    # Safety: ensure k is not larger than number of samples - 1
    max_k = max(1, n_locations - 1)
    if k > max_k:
        log.warning(f"Requested k={k} is too large for n_locations={n_locations}. Reducing k -> {max_k}.")
        k = max_k

    # Run clustering
    log.info(f"Running KMeans with k={k}")
    km = KMeans(n_clusters=k, random_state=random_state, n_init=10)
    labels = km.fit_predict(X_for_clustering)

    # Prepare outputs
    ensure_dir(out_root)
    clusters_df = pd.DataFrame({"location": loc_index, "cluster": labels})
    clusters_csv = out_root / "clusters_by_location.csv"
    clusters_df.to_csv(clusters_csv, index=False)
    log.info(f"Wrote cluster assignments: {clusters_csv}")

    # Summary
    summary = clusters_df.groupby("cluster").size().reset_index(name="n_locations")
    summary_csv = out_root / "cluster_summary.csv"
    summary.to_csv(summary_csv, index=False)
    log.info(f"Wrote cluster summary: {summary_csv}")

    # Save PCA and scaler if used
    if pca_model is not None:
        models_dir = out_root / "models"
        ensure_dir(models_dir)
        pca_path = models_dir / "pca_model.joblib"
        scaler_path = models_dir / "scaler.joblib"
        joblib.dump(pca_model, pca_path)
        joblib.dump(scaler, scaler_path)
        log.info(f"Wrote PCA model and scaler to {models_dir}")
    else:
        pca_path = None
        scaler_path = out_root / "scaler.joblib"
        joblib.dump(scaler, scaler_path)
        log.info(f"Wrote scaler to {scaler_path}")

    # Save KMeans model
    km_path = out_root / "models" / "kmeans_model.joblib"
    ensure_dir(km_path.parent)
    joblib.dump(km, km_path)
    log.info(f"Wrote KMeans model: {km_path}")

    # Metadata
    metadata = {
        "input": str(input_path),
        "n_locations": n_locations,
        "value_col": value_col,
        "date_col": date_col,
        "location_col": location_col,
        "pca_cols": pca_cols,
        "pca_requested_n": pca_n,
        "pca_n_used": int(pca_n_used) if pca_n_used is not None else None,
        "k": int(k),
        "outputs": {
            "clusters_csv": str(clusters_csv),
            "cluster_summary_csv": str(summary_csv),
            "models_dir": str((out_root / "models").resolve()),
        },
        "timestamp": datetime.datetime.utcnow().isoformat(),
    }
    (out_root / "clustering_metadata.json").write_text(json.dumps(metadata, indent=2))
    log.info("Clustering completed.")

    return metadata

# -------------------- CLI --------------------
def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Clustering pipeline (safe PCA).")
    p.add_argument("--input", "-i", required=True, help="Input features parquet (per-record features).")
    p.add_argument("--value-col", default="o3_ug_m3", help="Primary numeric column (fallback when pca-cols not provided).")
    p.add_argument("--date-col", default="date", help="Date column name.")
    p.add_argument("--location-col", default="location", help="Location/region column name.")
    p.add_argument("--out-root", default="analysis_outputs/clusters", help="Output directory root.")
    p.add_argument("--pca-cols", default=None, help="Comma-separated list of columns to use for PCA (overrides --value-col).")
    p.add_argument("--pca-n", type=int, default=None, help="Requested number of PCA components (auto-limited).")
    p.add_argument("--k", type=int, default=4, help="Number of clusters for KMeans.")
    p.add_argument("--random-state", type=int, default=0, help="Random seed.")
    return p

def main():
    parser = build_argparser()
    args = parser.parse_args()

    input_path = Path(args.input)
    out_root = Path(args.out_root)

    pca_cols = parse_list_arg(args.pca_cols)

    try:
        meta = run_clustering(
            input_path=input_path,
            value_col=args.value_col,
            date_col=args.date_col,
            location_col=args.location_col,
            out_root=out_root,
            pca_cols=pca_cols,
            pca_n=args.pca_n,
            k=args.k,
            random_state=args.random_state,
        )
        log.info(json.dumps(meta, indent=2))
    except Exception as e:
        log.exception("Clustering run failed.")
        raise

if __name__ == "__main__":
    main()
