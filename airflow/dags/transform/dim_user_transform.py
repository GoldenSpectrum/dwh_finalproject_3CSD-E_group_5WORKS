from transform.utils import fetch_df, load_df_scd2

def transform_dim_user():

    user = dedupe(fetch_df("SELECT * FROM stg_user_data"), key="user_id", ts_col="creation_date")
    job = dedupe(fetch_df("SELECT * FROM stg_user_job"), key="user_id")
    card = dedupe(fetch_df("SELECT * FROM stg_user_credit_card"), key="user_id")

    df = (
        user
        .merge(job, on="user_id", how="left")
        .merge(card, on="user_id", how="left")
    )

    df = df.rename(columns={
        "creation_date": "user_creation_date",
        "name": "user_full_name",
        "street": "user_street_address",
        "state": "user_state",
        "city": "user_city",
        "country": "user_country",
        "birthdate": "user_birthdate",
        "gender": "user_gender",
        "device_address": "user_device_address",
        "user_type": "user_type",
        "job_title": "user_job_title",
        "job_level": "user_job_level",
        "credit_card_number": "user_credit_card_number",
        "issuing_bank": "user_issuing_bank"
    })

    df = df[[
        "user_id", "user_creation_date", "user_full_name",
        "user_street_address", "user_state", "user_city",
        "user_country", "user_birthdate", "user_gender",
        "user_device_address", "user_type",
        "user_job_title", "user_job_level",
        "user_credit_card_number", "user_issuing_bank"
    ]]

    load_df_scd2(
    df,
    table_name="dim_user",
    business_key="user_id",
    tracked_cols=[
        "user_full_name",
        "user_street_address",
        "user_city",
        "user_state",
        "user_country",
        "user_job_title",
        "user_job_level",
        "user_credit_card_number",
        "user_issuing_bank"
    ]
)



def dedupe(df, key="user_id", ts_col=None):
    df = df.drop_duplicates()
    
    if ts_col and ts_col in df.columns:
        df = df.sort_values(ts_col)
    else:
        df = df.sort_values(key)

    return df.drop_duplicates(subset=[key], keep="last")
