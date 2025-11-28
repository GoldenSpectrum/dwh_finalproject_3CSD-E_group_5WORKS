import pandas as pd
from ingestion.utils import load_to_staging, truncate_table, dedupe   # updated import

DATA_PATH = "/opt/airflow/data_raw/enterprise/"


def ingest_merchant_data():
    truncate_table("stg_merchant_data")

    df = pd.read_html(DATA_PATH + "merchant_data.html")[0]
    df = df.drop(columns=["Unnamed: 0"])

    # safety dedupe
    df = dedupe(df, key="merchant_id")

    load_to_staging(df, "stg_merchant_data")


def ingest_staff_data():
    truncate_table("stg_staff_data")

    df = pd.read_html(DATA_PATH + "staff_data.html")[0]
    df = df.drop(columns=["Unnamed: 0"])

    # safety dedupe
    df = dedupe(df, key="staff_id")

    load_to_staging(df, "stg_staff_data")


def ingest_order_with_merchant():
    truncate_table("stg_order_with_merchant")

    files = [
        "order_with_merchant_data1.parquet",
        "order_with_merchant_data2.parquet",
        "order_with_merchant_data3.csv",
    ]

    combined = []  # collect all rows before dedupe

    for file in files:
        path = DATA_PATH + file

        if file.endswith(".csv"):
            df = pd.read_csv(path)
            if "Unnamed: 0" in df.columns:
                df = df.drop(columns=["Unnamed: 0"])
        else:
            df = pd.read_parquet(path)

        combined.append(df)

    # Combine files and dedupe by order_id
    df = pd.concat(combined, ignore_index=True)

    df = dedupe(df, key="order_id")  # ensure no duplicate order rows

    load_to_staging(df, "stg_order_with_merchant")
