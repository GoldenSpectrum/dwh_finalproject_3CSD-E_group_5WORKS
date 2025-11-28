from transform.utils import fetch_df, load_df, truncate_table
import pandas as pd

def transform_fact_orders():
    truncate_table("fact_orders")

    # load tables
    orders = fetch_df("SELECT * FROM stg_order_data")
    merchant = fetch_df("SELECT * FROM stg_order_with_merchant")

    # MERGE orders + merchant 
    df = (
        orders
        .merge(merchant, on="order_id", how="left")
    )

    # rename / clean estimated arrival
    df = df.rename(columns={
        "estimated arrival": "order_estimated_arrival",
        "transaction_date": "order_transaction_date"
    })

    # clean estimated arrival
    df["order_estimated_arrival"] = (
        df["order_estimated_arrival"]
        .astype(str)
        .str.extract(r"(\d+)")
        .astype(float)
        .fillna(0)
        .astype(int)
    )

    # final columns
    df = df[[
        "order_id",
        "user_id",
        "order_transaction_date",
        "order_estimated_arrival",
        "merchant_id",
        "staff_id"
    ]]

    load_df(df, "fact_orders")
