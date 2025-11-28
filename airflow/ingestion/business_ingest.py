import pandas as pd
from ingestion.utils import load_to_staging, truncate_table, dedupe

DATA_PATH = "/opt/airflow/data_raw/business/"

def ingest_product_list():
    # Clear staging
    truncate_table("stg_product_list")

    # Load raw data
    df = pd.read_excel(f"{DATA_PATH}product_list.xlsx")

    # Standardize columns
    df = df.rename(columns={
        "Unnamed: 0": "index_raw",
        "product_id": "product_id",
        "product_name": "product_name",
        "product_type": "product_type",
        "price": "price"
    })

    # Fill missing product types
    df["product_type"] = df["product_type"].fillna("Unknown")

    # Dedupe by product_id (last version wins)
    df = dedupe(df, key="product_id")

    # Load into staging
    load_to_staging(df, "stg_product_list")
