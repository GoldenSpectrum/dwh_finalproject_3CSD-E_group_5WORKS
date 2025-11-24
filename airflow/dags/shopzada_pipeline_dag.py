import sys
import os
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import your ingestion functions
from ingestion.business_ingest import ingest_product_list
from ingestion.customer_ingest import (
    ingest_user_job,
    ingest_user_data,
    ingest_user_credit_card
)
from ingestion.enterprise_ingest import (
    ingest_merchant_data,
    ingest_staff_data,
    ingest_order_with_merchant
)
from ingestion.marketing_ingest import (
    ingest_campaign_data,
    ingest_transactional_campaign_data
)
from ingestion.ops_ingest import (
    ingest_line_item_prices,
    ingest_line_item_products,
    ingest_order_data,
    ingest_order_delays
)


# -----------------------------------------
# DAG DEFAULT ARGS
# -----------------------------------------
default_args = {
    "owner": "shopzada_data_team",
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
}

# -----------------------------------------
# DAG DECLARATION
# -----------------------------------------
with DAG(
    dag_id="shopzada_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="End-to-end staging ingestion for ShopZada DWH",
) as dag:

    # -----------------------------------------
    # BUSINESS DEPARTMENT TASKS
    # -----------------------------------------
    ingest_product_list_task = PythonOperator(
        task_id="ingest_product_list",
        python_callable=ingest_product_list
    )

    # -----------------------------------------
    # CUSTOMER MANAGEMENT TASKS
    # -----------------------------------------
    ingest_user_job_task = PythonOperator(
        task_id="ingest_user_job",
        python_callable=ingest_user_job
    )

    ingest_user_data_task = PythonOperator(
        task_id="ingest_user_data",
        python_callable=ingest_user_data
    )

    ingest_user_credit_card_task = PythonOperator(
        task_id="ingest_user_credit_card",
        python_callable=ingest_user_credit_card
    )

    # -----------------------------------------
    # ENTERPRISE TASKS
    # -----------------------------------------
    ingest_merchant_data_task = PythonOperator(
        task_id="ingest_merchant_data",
        python_callable=ingest_merchant_data
    )

    ingest_staff_data_task = PythonOperator(
        task_id="ingest_staff_data",
        python_callable=ingest_staff_data
    )

    ingest_order_with_merchant_task = PythonOperator(
        task_id="ingest_order_with_merchant",
        python_callable=ingest_order_with_merchant
    )


    # -----------------------------------------
    # MARKETING TASKS
    # -----------------------------------------
    ingest_campaign_data_task = PythonOperator(
        task_id="ingest_campaign_data",
        python_callable=ingest_campaign_data
    )

    ingest_transactional_campaign_data_task = PythonOperator(
        task_id="ingest_transactional_campaign_data",
        python_callable=ingest_transactional_campaign_data
    )

    # -----------------------------------------
    # OPERATIONS TASKS
    # -----------------------------------------
    ingest_line_item_prices_task = PythonOperator(
        task_id="ingest_line_item_prices",
        python_callable=ingest_line_item_prices
    )

    ingest_line_item_products_task = PythonOperator(
        task_id="ingest_line_item_products",
        python_callable=ingest_line_item_products
    )

    ingest_order_data_task = PythonOperator(
        task_id="ingest_order_data",
        python_callable=ingest_order_data
    )

    ingest_order_delays_task = PythonOperator(
        task_id="ingest_order_delays",
        python_callable=ingest_order_delays
    )

    finish_ingestion = EmptyOperator(
        task_id="finish_ingestion"
    )

    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform_pipeline",
        trigger_dag_id="shopzada_transform_dag"
    )

    # ===================================================
    # DAG DEPENDENCIES
    # ===================================================
    
    (
        ingest_product_list_task
        >> [
            ingest_user_job_task,
            ingest_user_data_task,
            ingest_user_credit_card_task,
            ingest_merchant_data_task,
            ingest_staff_data_task,
            ingest_order_with_merchant_task,
            ingest_campaign_data_task,
            ingest_transactional_campaign_data_task,
            ingest_line_item_prices_task,
            ingest_line_item_products_task,
            ingest_order_data_task,
            ingest_order_delays_task,
        ]
         >> finish_ingestion
         >> trigger_transform
    )
