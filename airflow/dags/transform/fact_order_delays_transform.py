from transform.utils import fetch_df, load_df, truncate_table

def transform_fact_order_delays():
    truncate_table("fact_order_delays")

    delays = fetch_df("SELECT * FROM stg_order_delays")
    orders = fetch_df("SELECT order_id FROM fact_orders")

    # Rename properly
    delays = delays.rename(columns={"delay in days": "delay_in_days"})

    # Keep only order_ids that exist in the fact_orders table
    delays = delays[delays["order_id"].isin(orders["order_id"])]

    delays = delays[["order_id", "delay_in_days"]]

    load_df(delays, "fact_order_delays")
