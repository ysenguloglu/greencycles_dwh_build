from greencycles_dwh_build_scripts import source_to_staging
from greencycles_dwh_build_scripts import staging_to_dwh
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

with DAG(
    dag_id="greencycles_dwh_build", 
    schedule="@daily", 
    start_date=datetime.today(), 
    catchup=False, 
    tags=["greencycles"]
    ) as dag:
    
    # Source to Staging tasks
    source_to_stg_customer_ = PythonOperator(
        task_id="source_to_stg_customer",
        python_callable=source_to_staging.source_to_stg_customer
    )

    source_to_stg_film_ = PythonOperator(
        task_id="source_to_stg_film",
        python_callable=source_to_staging.source_to_stg_film
    )

    source_to_stg_staff_ = PythonOperator(
        task_id="source_to_stg_staff",
        python_callable=source_to_staging.source_to_stg_staff
    )

    source_to_stg_payment_ = PythonOperator(
        task_id="source_to_stg_payment",
        python_callable=source_to_staging.source_to_stg_payment
    )
    # Staging to DWH tasks
    stg_to_dwh_customer_ = PythonOperator(
        task_id="stg_to_dwh_customer",
        python_callable=staging_to_dwh.stg_to_dwh_customer
    )

    stg_to_dwh_film_ = PythonOperator(
        task_id="stg_to_dwh_film",
        python_callable=staging_to_dwh.stg_to_dwh_film
    )

    stg_to_dwh_staff_ = PythonOperator(
        task_id="stg_to_dwh_staff",
        python_callable=staging_to_dwh.stg_to_dwh_staff
    )

    stg_to_dwh_payment_ = PythonOperator(
        task_id="stg_to_dwh_payment",
        python_callable=staging_to_dwh.stg_to_dwh_payment
    )

    source_to_stg_customer_ >> stg_to_dwh_customer_
    source_to_stg_film_ >> stg_to_dwh_film_
    source_to_stg_staff_ >> stg_to_dwh_staff_
    source_to_stg_payment_ >> stg_to_dwh_payment_