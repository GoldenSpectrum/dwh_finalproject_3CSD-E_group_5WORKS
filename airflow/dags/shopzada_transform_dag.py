from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# import your transform functions
from transform.dim_user_transform import transform_dim_user
from transform.dim_merchant_transform import transform_dim_merchant
from transform.dim_staff_transform import transform_dim_staff
from transform.dim_product_transform import transform_dim_product
from transform.dim_campaign_transform import transform_dim_campaign
from transform.dim_date_transform import transform_dim_date

from transform.fact_orders_transform import transform_fact_orders
from transform.fact_line_items_transform import transform_fact_line_items
from transform.fact_campaign_transaction_transform import transform_fact_campaign_transaction
from transform.fact_order_delays_transform import transform_fact_order_delays


with DAG(
    dag_id="shopzada_transform_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,    # Only triggered by ingestion DAG
    catchup=False
) as dag:

    # =============== DIMENSIONS ===============
    dim_user = PythonOperator(
        task_id="transform_dim_user",
        python_callable=transform_dim_user
    )

    dim_merchant = PythonOperator(
        task_id="transform_dim_merchant",
        python_callable=transform_dim_merchant
    )

    dim_staff = PythonOperator(
        task_id="transform_dim_staff",
        python_callable=transform_dim_staff
    )

    dim_product = PythonOperator(
        task_id="transform_dim_product",
        python_callable=transform_dim_product
    )

    dim_campaign = PythonOperator(
        task_id="transform_dim_campaign",
        python_callable=transform_dim_campaign
    )

    dim_date = PythonOperator(
        task_id="transform_dim_date",
        python_callable=transform_dim_date
    )

    dim_tasks = [
        dim_user,
        dim_merchant,
        dim_staff,
        dim_product,
        dim_campaign,
        dim_date
    ]

    # =============== FACT TABLES ===============
    fact_orders = PythonOperator(
        task_id="transform_fact_orders",
        python_callable=transform_fact_orders
    )

    fact_line_items = PythonOperator(
        task_id="transform_fact_line_items",
        python_callable=transform_fact_line_items
    )

    fact_campaign = PythonOperator(
        task_id="transform_fact_campaign",
        python_callable=transform_fact_campaign_transaction
    )

    fact_delays = PythonOperator(
        task_id="transform_fact_delays",
        python_callable=transform_fact_order_delays
    )

    # =============== DAG DEPENDENCIES ===============

    # All dims run first in parallel
    for dim in dim_tasks:
        dim

    # Then facts depend on ALL dims
    dim_tasks >> fact_orders
    fact_orders >> fact_line_items
    fact_orders >> fact_campaign
    fact_orders >> fact_delays
