from transform.utils import fetch_df, load_df, truncate_table

def transform_dim_product():
    truncate_table("dim_product")

    df = fetch_df("SELECT * FROM stg_product_list")

    df = df.rename(columns={"price": "price"})

    df = df.drop_duplicates(subset=["product_id"])

    df = df[[
        "product_id", "product_name", "product_type", "price"
    ]]

    load_df(df, "dim_product")
