from transform.utils import fetch_df, load_df_insert_only, truncate_table
import pandas as pd

def transform_dim_campaign():

    df = fetch_df("SELECT * FROM stg_campaign_data")

    df = df.drop_duplicates(subset=["campaign_id"])

    df["campaign_discount"] = (
        df["discount"]
        .astype(str)
        .str.replace("%", "", regex=False)
        .str.strip()
    )

    df["campaign_discount"] = (
        pd.to_numeric(df["campaign_discount"], errors="coerce") / 100
    )

    df = df.rename(columns={
        "campaign_name": "campaign_name",
        "campaign_description": "campaign_description"
    })

    df = df[[
        "campaign_id", "campaign_name",
        "campaign_description", "campaign_discount"
    ]]

    load_df_insert_only(df, "dim_campaign")
