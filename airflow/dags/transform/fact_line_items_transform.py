from transform.utils import fetch_df, load_df, truncate_table

def transform_fact_line_items():

    prices = fetch_df("SELECT * FROM stg_line_item_prices")
    products = fetch_df("SELECT * FROM stg_line_item_products")

    prices["line_item_quantity"] = (
        prices["quantity"]
        .astype(str)
        .str.extract(r"(\d+)")
        .astype(int)
    )

    prices = (
        prices.sort_values("order_id")
              .drop_duplicates(subset=["order_id"], keep="last")
    )

    products = (
        products.sort_values(["order_id", "product_id"])
                .drop_duplicates(subset=["order_id", "product_id"], keep="last")
    )

    df = prices.merge(products, on="order_id", how="left")

    df = df.rename(columns={
        "price": "line_item_price",
        "product_name": "line_item_product_name"
    })

    df = df[[
        "order_id", "product_id",
        "line_item_price", "line_item_quantity",
        "line_item_product_name"
    ]]

    load_df(
    df,
    "fact_line_items",
    conflict_cols=["order_id", "product_id"]
)

