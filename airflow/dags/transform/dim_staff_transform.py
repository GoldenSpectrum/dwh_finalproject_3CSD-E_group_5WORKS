from transform.utils import fetch_df, load_df, truncate_table

def transform_dim_staff():
    truncate_table("dim_staff")

    df = fetch_df("SELECT * FROM stg_staff_data")
    
    df = (
        df.sort_values("creation_date")
          .drop_duplicates(subset=["staff_id"], keep="last")
    )

    df = df[[
        "staff_id", "name", "job_level", "street", "state",
        "city", "country", "contact_number", "creation_date"
    ]]

    load_df(df, "dim_staff")
