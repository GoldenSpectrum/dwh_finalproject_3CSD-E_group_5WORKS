from transform.utils import fetch_df, load_df_scd2

def transform_dim_merchant():

    df = fetch_df("SELECT * FROM stg_merchant_data")

    # dedupe on latest creation_date
    df = (
        df.sort_values("creation_date")
          .drop_duplicates(subset=["merchant_id"], keep="last")
    )

    df = df.rename(columns={
        "creation_date": "merchant_creation_date",
        "name": "merchant_name",
        "street": "merchant_street_address",
        "state": "merchant_state",
        "city": "merchant_city",
        "country": "merchant_country",
        "contact_number": "merchant_contact_number"
    })

    df = df[[
        "merchant_id",
        "merchant_creation_date",
        "merchant_name",
        "merchant_street_address",
        "merchant_state",
        "merchant_city",
        "merchant_country",
        "merchant_contact_number"
    ]]

    load_df_scd2(
        df, 
        table_name="dim_merchant", 
        business_key="merchant_id",
        tracked_cols=[
            "merchant_name",
            "merchant_street_address",
            "merchant_state",
            "merchant_city",
            "merchant_country",
            "merchant_contact_number"
        ]
    )
