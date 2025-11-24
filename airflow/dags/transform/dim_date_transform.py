from transform.utils import fetch_df, load_df, truncate_table
import pandas as pd

def transform_dim_date():
    truncate_table("dim_date")

    order_dates = fetch_df("SELECT transaction_date FROM stg_order_data")
    dates = pd.to_datetime(order_dates["transaction_date"].unique())

    df = pd.DataFrame({"date": dates})

    df["year"] = df["date"].dt.year
    df["quarter"] = df["date"].dt.quarter
    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day
    df["day_of_week"] = df["date"].dt.weekday
    df["is_weekend"] = df["day_of_week"] >= 5

    load_df(df, "dim_date")
