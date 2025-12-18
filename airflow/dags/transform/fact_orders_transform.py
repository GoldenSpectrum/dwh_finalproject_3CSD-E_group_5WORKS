from transform.utils import fetch_df, load_df
import pandas as pd

def transform_fact_orders():

    # Load staging
    orders = fetch_df("SELECT * FROM stg_order_data")
    merchant = fetch_df("SELECT * FROM stg_order_with_merchant")

    # Merge staging sources
    df = orders.merge(merchant, on="order_id", how="left")

    # Rename
    df = df.rename(columns={
        "estimated arrival": "order_estimated_arrival",
        "transaction_date": "order_transaction_date"
    })

    # Clean estimated arrival
    df["order_estimated_arrival"] = (
        df["order_estimated_arrival"]
        .astype(str)
        .str.extract(r"(\d+)")
        .astype(float)
        .fillna(0)
        .astype(int)
    )

    # 🔑 Resolve USER surrogate key
    user_dim = fetch_df("""
        SELECT user_sk, user_id
        FROM dim_user
        WHERE is_current = true
    """)

    df = df.merge(user_dim, on="user_id", how="left")

    # 🔑 Resolve MERCHANT surrogate key
    merchant_dim = fetch_df("""
        SELECT merchant_sk, merchant_id
        FROM dim_merchant
        WHERE is_current = true
    """)

    df = df.merge(merchant_dim, on="merchant_id", how="left")

    # 🔑 Resolve STAFF surrogate key
    staff_dim = fetch_df("""
        SELECT staff_sk, staff_id
        FROM dim_staff
        WHERE is_current = true
    """)

    df = df.merge(staff_dim, on="staff_id", how="left")

    # Final fact shape
    df = df[[
        "order_id",
        "user_sk",
        "merchant_sk",
        "staff_sk",
        "order_transaction_date",
        "order_estimated_arrival"
    ]]

    load_df(
        df,
        "fact_orders",
        conflict_cols=["order_id"]
    )
