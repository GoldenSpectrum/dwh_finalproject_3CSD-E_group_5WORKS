from transform.utils import fetch_df, load_df, truncate_table

def transform_dim_merchant():
    truncate_table("dim_merchant")

    df = fetch_df("SELECT * FROM stg_merchant_data")

    df = df.rename(columns={"merchant_name": "name"})

    df = (
        df.sort_values("creation_date")
          .drop_duplicates(subset=["merchant_id"], keep="last")
    )

    df = df[[
        "merchant_id", "creation_date", "name",
        "street", "state", "city", "country",
        "contact_number"
    ]]

    load_df(df, "dim_merchant")
