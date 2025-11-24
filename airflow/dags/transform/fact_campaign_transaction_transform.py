from transform.utils import fetch_df, load_df, truncate_table

def transform_fact_campaign_transaction():
    truncate_table("fact_campaign_transactions")

    df = fetch_df("SELECT * FROM stg_transactional_campaign")

    # Clean "estimated arrival" BEFORE renaming
    df["estimated_arrival"] = (
        df["estimated arrival"]
        .astype(str)
        .str.extract(r"(\d+)")
        .astype(int)
    )

    # Now select the final columns (and drop the old name)
    df = df[[
        "transaction_date",
        "campaign_id",
        "order_id",
        "estimated_arrival",
        "availed"
    ]]

    load_df(df, "fact_campaign_transactions")
