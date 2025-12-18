from transform.utils import fetch_df, load_df_insert_only, truncate_table
import pandas as pd

def transform_dim_date():

    # Fetch raw transaction dates from staging
    order_dates = fetch_df("SELECT transaction_date FROM stg_order_data")

    # Clean, sort, unique
    dates = (
        pd.to_datetime(order_dates["transaction_date"])
        .dropna()
        .sort_values()
        .unique()
    )

    df = pd.DataFrame({"date_value": dates})

    # Basic components
    df["date_year"]      = df["date_value"].dt.year
    df["date_quarter"]   = df["date_value"].dt.quarter
    df["date_month"]     = df["date_value"].dt.month
    df["date_day"]       = df["date_value"].dt.day
    df["day_of_week"]    = df["date_value"].dt.weekday
    df["is_weekend"]     = df["day_of_week"] >= 5

    # NEW fields
    df["week_start_date"] = df["date_value"].dt.to_period("W").apply(lambda p: p.start_time.date())
    df["week_of_year"] = df["date_value"].dt.isocalendar().week.astype(int)

    # Load into dimension table
    load_df_insert_only(df, "dim_date")
