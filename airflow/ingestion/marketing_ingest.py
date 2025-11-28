import pandas as pd
from ingestion.utils import load_to_staging, truncate_table, dedupe, normalize_df   # updated import

DATA_PATH = "/opt/airflow/data_raw/marketing/"

def ingest_campaign_data():
    truncate_table("stg_campaign_data")

    df = pd.read_csv(DATA_PATH + "campaign_data.csv", sep="\t")

    expected = [
        "index_raw",
        "campaign_id",
        "campaign_name",
        "campaign_description",
        "discount"
    ]

    df = normalize_df(df, expected)

    # ensure no duplicate campaign IDs
    df = dedupe(df, key="campaign_id")

    load_to_staging(df, "stg_campaign_data")


def ingest_transactional_campaign_data():
    truncate_table("stg_transactional_campaign")

    df = pd.read_csv(DATA_PATH + "transactional_campaign_data.csv")

    expected = [
        "index_raw",
        "transaction_date",
        "campaign_id",
        "order_id",
        "estimated arrival",
        "availed"
    ]

    df = normalize_df(df, expected)

    # dedupe based on campaign + order + date combo (best surrogate key)
    df = dedupe(
        df,
        key=["campaign_id", "order_id", "transaction_date"]
    )

    load_to_staging(df, "stg_transactional_campaign")
