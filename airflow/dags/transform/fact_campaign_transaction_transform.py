from transform.utils import fetch_df, load_df, truncate_table

def transform_fact_campaign_transaction():

    df = fetch_df("SELECT * FROM stg_transactional_campaign")

    df["txn_estimated_arrival"] = (
        df["estimated arrival"]
        .astype(str)
        .str.extract(r"(\d+)")
        .astype(int)
    )

    df = df.rename(columns={
        "availed": "transaction_availed"
    })

    df = df[[
        "transaction_date",
        "campaign_id",
        "order_id",
        "txn_estimated_arrival",
        "transaction_availed"
    ]]

    load_df(
        df,
        "fact_campaign_transactions",
        conflict_cols=["transaction_date", "campaign_id", "order_id"]
    )
