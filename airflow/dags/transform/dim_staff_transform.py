from transform.utils import fetch_df, load_df_scd2

def transform_dim_staff():

    df = fetch_df("SELECT * FROM stg_staff_data")

    df = (
        df.sort_values("creation_date")
          .drop_duplicates(subset=["staff_id"], keep="last")
    )

    df = df.rename(columns={
        "name": "staff_full_name",
        "job_level": "staff_job_level",
        "street": "staff_street_address",
        "state": "staff_state",
        "city": "staff_city",
        "country": "staff_country",
        "contact_number": "staff_contact_number",
        "creation_date": "staff_creation_date"
    })

    df = df[[
        "staff_id", "staff_full_name", "staff_job_level",
        "staff_street_address", "staff_state", "staff_city",
        "staff_country", "staff_contact_number", "staff_creation_date"
    ]]

    load_df_scd2(
        df,
        table_name="dim_staff",
        business_key="staff_id",
        tracked_cols=[
            "staff_full_name",
            "staff_job_level",
            "staff_street_address",
            "staff_state",
            "staff_city",
            "staff_country",
            "staff_contact_number"
        ]
    )
