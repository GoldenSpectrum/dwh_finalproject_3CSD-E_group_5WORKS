import pandas as pd
from sqlalchemy import create_engine

# ============================================================
# DATABASE CONNECTION
# ============================================================
def get_engine():
    """
    Returns SQLAlchemy engine for connecting to the Postgres container.
    """
    return create_engine(
        "postgresql+psycopg2://postgres:postgres@postgres:5432/shopzada"
    )

# ============================================================
# BASIC DB OPERATIONS
# ============================================================
def truncate_table(table_name: str):
    """
    TRUNCATE a table before loading new data.
    """
    engine = get_engine()
    engine.execute(f"TRUNCATE TABLE {table_name}")

def load_to_staging(df: pd.DataFrame, table_name: str):
    """
    Append dataframe rows into a staging table.
    Uses multi-row insert for speed.
    """
    engine = get_engine()
    df.to_sql(
        table_name,
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5000,
    )

# ============================================================
# DATA NORMALIZATION (Bronze → Silver)
# ============================================================
def normalize_df(df, expected_cols):
    """
    Clean incoming messy raw files by:
    - removing pandas merge suffixes (_x, _y, _m123)
    - stripping stray tab characters
    - renaming unnamed columns to index_raw
    - ensuring index_raw exists
    - keeping only expected columns
    """
    # remove suffixes like _m123
    df.columns = [c.split("_m")[0] for c in df.columns]
    df.columns = [c.split(".")[0] for c in df.columns]
    df.columns = [c.replace("\t", "") for c in df.columns]

    # rename unnamed columns
    for col in list(df.columns):
        if col.lower().startswith("unnamed"):
            df = df.rename(columns={col: "index_raw"})

    # ensure index_raw exists
    if "index_raw" not in df.columns:
        df.insert(0, "index_raw", range(len(df)))

    # keep expected schema only
    df = df[[c for c in df.columns if c in expected_cols]]

    return df

# ============================================================
# DEDUPE FOR BRONZE → SILVER
# ============================================================
def dedupe(df, key, ts_col=None):
    # Normalize key to a list
    if isinstance(key, str):
        subset = [key]
    else:
        subset = key  # already a list

    # Sort
    if ts_col and ts_col in df.columns:
        df = df.sort_values(ts_col)
    else:
        df = df.sort_values(subset)

    return df.drop_duplicates(subset=subset, keep="last")
