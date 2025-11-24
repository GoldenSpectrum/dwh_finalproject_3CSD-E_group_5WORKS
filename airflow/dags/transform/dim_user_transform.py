from transform.utils import fetch_df, load_df, truncate_table

def transform_dim_user():

    truncate_table("dim_user")

    user = dedupe(fetch_df("SELECT * FROM stg_user_data"), key="user_id", ts_col="creation_date")
    job = dedupe(fetch_df("SELECT * FROM stg_user_job"), key="user_id")
    card = dedupe(fetch_df("SELECT * FROM stg_user_credit_card"), key="user_id")


    df = (
        user
        .merge(job, on="user_id", how="left")
        .merge(card, on="user_id", how="left")
    )

    df = df[[
        "user_id", "creation_date", "name", "street", "state", "city",
        "country", "birthdate", "gender", "device_address", "user_type",
        "job_title", "job_level", "credit_card_number", "issuing_bank"
    ]]

    load_df(df, "dim_user")

def dedupe(df, key="user_id", ts_col=None):
    df = df.drop_duplicates()
    
    if ts_col and ts_col in df.columns:
        # sort by timestamp so latest wins
        df = df.sort_values(ts_col)
    else:
        # fallback deterministic sort
        df = df.sort_values(key)

    return df.drop_duplicates(subset=[key], keep="last")
