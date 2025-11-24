from transform.utils import fetch_df, load_df, truncate_table
import pandas as pd

def transform_fact_orders():
    truncate_table("fact_orders")

    orders = fetch_df("SELECT * FROM stg_order_data")
    merchant = fetch_df("SELECT * FROM stg_order_with_merchant")
    delays = fetch_df("SELECT * FROM stg_order_delays")

    df = (
        orders
        .merge(merchant, on="order_id", how="left")
        .merge(delays, on="order_id", how="left")
    )

    df = df.rename(columns={
        "estimated arrival": "estimated_arrival",
        "delay in days": "delay_in_days"
    })

    # CLEAN estimated_arrival
    df["estimated_arrival"] = (
        df["estimated_arrival"]
        .astype(str)
        .str.extract(r"(\d+)")
    )[0]

    df["estimated_arrival"] = (
        pd.to_numeric(df["estimated_arrival"], errors="coerce")
        .fillna(0)
        .astype(int)
    )

    # CLEAN delay_in_days
    df["delay_in_days"] = (
        df["delay_in_days"]
        .astype(str)
        .str.extract(r"(\d+)")
    )[0]

    df["delay_in_days"] = (
        pd.to_numeric(df["delay_in_days"], errors="coerce")
        .fillna(0)
        .astype(int)
    )

    df = df[[
        "order_id", "user_id",
        "transaction_date", "estimated_arrival",
        "merchant_id", "staff_id", "delay_in_days"
    ]]

    load_df(df, "fact_orders")
