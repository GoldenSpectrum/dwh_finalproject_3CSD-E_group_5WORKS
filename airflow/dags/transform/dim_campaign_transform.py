from transform.utils import fetch_df, load_df, truncate_table
import pandas as pd

def transform_dim_campaign():
    truncate_table("dim_campaign")

    df = fetch_df("SELECT * FROM stg_campaign_data")
    
    df = df.drop_duplicates(subset=["campaign_id"])

    # Clean discount column
    df["discount"] = (
        df["discount"]
        .astype(str)
        .str.replace("%", "", regex=False)
        .str.strip()
    )

    # Convert to numeric, coercing errors to NaN
    df["discount"] = (
    pd.to_numeric(df["discount"].str.replace("%", ""), errors="coerce") / 100
)


    df = df[[
        "campaign_id", "campaign_name",
        "campaign_description", "discount"
    ]]

    load_df(df, "dim_campaign")
