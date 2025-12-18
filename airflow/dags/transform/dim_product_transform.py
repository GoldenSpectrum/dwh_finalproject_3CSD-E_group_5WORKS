from transform.utils import fetch_df, load_df, truncate_table

def transform_dim_product():

    df = fetch_df("SELECT * FROM stg_product_list")

    df = df.drop_duplicates(subset=["product_id"])

    df = df.rename(columns={
        "price": "product_price"
    })

    df = df[[
        "product_id", "product_name", "product_type", "product_price"
    ]]

    load_df(df, "dim_product", conflict_cols=["product_id"])
