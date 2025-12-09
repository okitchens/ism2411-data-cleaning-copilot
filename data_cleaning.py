"""
data_cleaning.py

Purpose:
This script loads the raw sales data, cleans it, and saves a processed
version ready for analysis.
"""

import os
import pandas as pd


# This function loads the raw CSV file and returns a pandas DataFrame.
# Keeping it separate makes the loading logic reusable and easy to test.
def load_data(file_path: str) -> pd.DataFrame:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    df = pd.read_csv(file_path)
    return df


# This function standardizes column names by lowercasing and replacing spaces with underscores.
# This helps avoid bugs later when referencing columns in code.
def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.str.strip()        # remove spaces around names
                 .str.lower()         # make all lowercase
                 .str.replace(" ", "_")
    )
    return df


# This function trims whitespace from key string columns like product name and category.
# Extra spaces can cause duplicated categories that should really be the same.
def strip_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ["prodname", "category"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    return df


# This function handles missing prices and quantities in a consistent way.
# Here we drop rows where price or quantity is missing, to avoid incorrect totals.
def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # rename columns defensively in case the raw file has odd capitalization
    possible_price_cols = [c for c in df.columns if c.lower() == "price"]
    possible_qty_cols = [c for c in df.columns if c.lower() in ("qty", "quantity")]

    if possible_price_cols:
        price_col = possible_price_cols[0]
    else:
        price_col = "price"

    if possible_qty_cols:
        qty_col = possible_qty_cols[0]
    else:
        qty_col = "qty"

    # ensure numeric types
    df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
    df[qty_col] = pd.to_numeric(df[qty_col], errors="coerce")

    # drop rows where either price or quantity is missing
    df = df.dropna(subset=[price_col, qty_col])

    return df


# This function removes rows with clearly invalid numeric values.
# Negative prices or quantities are treated as data entry errors and dropped.
def remove_invalid_rows(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    price_col = [c for c in df.columns if c.lower() == "price"][0]
    qty_col = [c for c in df.columns if c.lower() in ("qty", "quantity")][0]

    df = df[(df[price_col] >= 0) & (df[qty_col] >= 0)]
    return df


if __name__ == "__main__":
    raw_path = "data/raw/sales_data_raw.csv"
    cleaned_path = "data/processed/sales_data_clean.csv"

    df_raw = load_data(raw_path)
    df_clean = clean_column_names(df_raw)
    df_clean = strip_whitespace(df_clean)
    df_clean = handle_missing_values(df_clean)
    df_clean = remove_invalid_rows(df_clean)

    # make sure processed folder exists
    os.makedirs(os.path.dirname(cleaned_path), exist_ok=True)

    df_clean.to_csv(cleaned_path, index=False)
    print("Cleaning complete. First few rows:")
    print(df_clean.head())



