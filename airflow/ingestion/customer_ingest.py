import pandas as pd
from ingestion.utils import load_to_staging, truncate_table, dedupe

DATA_PATH = "/opt/airflow/data_raw/customer/"

def ingest_user_job():
    truncate_table("stg_user_job")

    df = pd.read_csv(DATA_PATH + "user_job.csv")
    df = df.drop(columns=["Unnamed: 0"])

    df["job_level"] = df["job_level"].fillna("Unknown")

    # Dedupe on user_id (safe choice)
    df = dedupe(df, key="user_id")

    load_to_staging(df, "stg_user_job")


def ingest_user_data():
    truncate_table("stg_user_data")

    df = pd.read_json(DATA_PATH + "user_data.json")

    # Dedupe user data
    df = dedupe(df, key="user_id")

    load_to_staging(df, "stg_user_data")


def ingest_user_credit_card():
    truncate_table("stg_user_credit_card")

    df = pd.read_pickle(DATA_PATH + "user_credit_card.pickle")

    # Dedupe card table too (yes, some people apparently get cloned in your dataset 👀)
    df = dedupe(df, key="user_id")

    load_to_staging(df, "stg_user_credit_card")
