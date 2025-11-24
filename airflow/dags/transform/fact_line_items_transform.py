from transform.utils import fetch_df, load_df, truncate_table

def transform_fact_line_items():
    truncate_table("fact_line_items")

    prices = fetch_df("SELECT * FROM stg_line_item_prices")
    products = fetch_df("SELECT * FROM stg_line_item_products")

    # Fix quantity
    prices["quantity"] = (
        prices["quantity"]
        .astype(str)
        .str.extract(r"(\d+)")
        .astype(int)
    )

    # DEDUPE
    # Prices: one row per (order_id)
    prices = (
        prices
        .sort_values("order_id")
        .drop_duplicates(subset=["order_id"], keep="last")
    )

    # Products: one row per (order_id, product_id)
    products = (
        products
        .sort_values(["order_id", "product_id"])
        .drop_duplicates(subset=["order_id", "product_id"], keep="last")
    )

    # Merge WITH the correct join
    df = prices.merge(products, on="order_id", how="left")

    df["line_total"] = df["price"] * df["quantity"]

    df = df[[
        "order_id", "product_id", "price", "quantity", "product_name"
    ]]

    load_df(df, "fact_line_items")
