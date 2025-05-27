import os
from dotenv import load_dotenv
from google.cloud import bigquery
import psycopg2
import pandas as pd
import json
load_dotenv()

bq_client = bigquery.Client.from_service_account_info(json.loads(os.getenv("G_CLOUD_KEY")))

dim_customer_ref = f"{os.getenv('G_CLOUD_PROJECT')}.{os.getenv('G_CLOUD_DATASET')}.dim_customer"
dim_film_ref = f"{os.getenv('G_CLOUD_PROJECT')}.{os.getenv('G_CLOUD_DATASET')}.dim_film"
dim_staff_ref = f"{os.getenv('G_CLOUD_PROJECT')}.{os.getenv('G_CLOUD_DATASET')}.dim_staff"
fact_payment_ref = f"{os.getenv('G_CLOUD_PROJECT')}.{os.getenv('G_CLOUD_DATASET')}.fact_payment"

def connect_to_stg_db():
    conn = psycopg2.connect(
        dbname=os.getenv("DB_STG_DATABASE"),
        user=os.getenv("DB_STG_USER"),
        password=os.getenv("DB_STG_PASS"),
        host=os.getenv("DB_STG_HOST"), # Use the local IP address
        port=os.getenv("DB_STG_PORT"),
    )
    return conn

def stg_to_dwh_customer():
    conn = connect_to_stg_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM stg_customer ORDER BY 1, insertion_date
                   """)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    conn.close()
    cursor.close()
    # Convert to DataFrame
    df = pd.DataFrame(rows, columns=columns)
    df["create_date"] = pd.to_datetime(df["create_date"])

    # Insert data into BigQuery
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=False
    )
    load_job = bq_client.load_table_from_dataframe(df, dim_customer_ref, job_config=job_config)
    load_job.result()

def stg_to_dwh_film():
    conn = connect_to_stg_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM stg_film ORDER BY 1, insertion_date
                   """)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    conn.close()
    cursor.close()
    # Convert to DataFrame
    df = pd.DataFrame(rows, columns=columns)

    # Insert data into BigQuery
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=False
    )
    load_job = bq_client.load_table_from_dataframe(df, dim_film_ref, job_config=job_config)
    load_job.result()

def stg_to_dwh_staff():
    conn = connect_to_stg_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM stg_staff ORDER BY 1, insertion_date
                   """)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    conn.close()
    cursor.close()
    # Convert to DataFrame
    df = pd.DataFrame(rows, columns=columns)

    # Insert data into BigQuery
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=False
    )
    load_job = bq_client.load_table_from_dataframe(df, dim_staff_ref, job_config=job_config)
    load_job.result()

def stg_to_dwh_payment():
    conn = connect_to_stg_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM stg_payment ORDER BY 1, insertion_date
                   """)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    conn.close()
    cursor.close()
    # Convert to DataFrame
    df = pd.DataFrame(rows, columns=columns)

    # Insert data into BigQuery
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=False
    )
    load_job = bq_client.load_table_from_dataframe(df, fact_payment_ref, job_config=job_config)
    load_job.result()
