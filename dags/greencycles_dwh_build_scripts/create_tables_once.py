import os
import psycopg2
from dotenv import load_dotenv
from google.cloud import bigquery
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

def create_stg_tables():
    conn = connect_to_stg_db()
    cursor = conn.cursor()
    # Create staging tables if they do not exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stg_customer (
            customer_id integer,
            first_name text,
            last_name text,
            email text,
            address text,
            district text,
            city text,
            country text,
            postal_code text,
            phone text,
            active boolean,
            create_date date,
            insertion_date timestamp
        );
        CREATE TABLE IF NOT EXISTS stg_film (
            film_id integer,
            title text,
            description text,
            release_year integer,
            language text,
            rental_duration integer,
            rental_rate numeric(4,2),
            length integer,
            replacement_cost numeric(5,2),
            rating text,
            category text,
            insertion_date timestamp
        );
        CREATE TABLE IF NOT EXISTS stg_staff (
            staff_id integer,
            first_name text,
            last_name text,
            address text,
            district text,
            city text,
            country text,
            postal_code text,
            phone text,
            email text,
            active boolean,
            user_name text,
            password text,
            insertion_date timestamp
        );
        CREATE TABLE IF NOT EXISTS stg_payment (
            payment_id integer,
            customer_id integer,
            staff_id integer,
            rental_id integer,
            inventory_id integer,
            film_id integer,
            store_id integer,
            amount numeric(5,2),
            payment_date timestamp,
            rental_date timestamp,
            return_date timestamp,
            insertion_date timestamp
        );
    """)

    conn.commit()
    cursor.close()
    conn.close()

def create_dwh_tables():

    # Schema definitions for dimension and fact table
    dim_customer_schema = [
        bigquery.SchemaField("customer_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("first_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("last_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("address", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("district", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("postal_code", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("phone", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("active", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("create_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("insertion_date", "TIMESTAMP", mode="NULLABLE")
    ]

    dim_film_schema = [
        bigquery.SchemaField("film_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("release_year", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("language", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("rental_duration", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("rental_rate", "NUMERIC", mode="NULLABLE"),
        bigquery.SchemaField("length", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("replacement_cost", "NUMERIC", mode="NULLABLE"),
        bigquery.SchemaField("rating", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("insertion_date", "TIMESTAMP", mode="NULLABLE")
    ]

    dim_staff_schema = [
        bigquery.SchemaField("staff_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("first_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("last_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("address", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("district", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("postal_code", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("phone", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("active", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("user_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("password", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("insertion_date", "TIMESTAMP", mode="NULLABLE")
    ]

    fact_payment_schema = [
        bigquery.SchemaField("payment_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("customer_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("staff_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("rental_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("inventory_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("film_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("store_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("amount", "NUMERIC", mode="NULLABLE"),
        bigquery.SchemaField("payment_date", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("rental_date", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("return_date", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("insertion_date", "TIMESTAMP", mode="NULLABLE")
    ]
    
    # Create dimension tables
    dim_customer = bigquery.Table(dim_customer_ref, schema=dim_customer_schema)
    dim_film = bigquery.Table(dim_film_ref, schema=dim_film_schema)
    dim_staff = bigquery.Table(dim_staff_ref, schema=dim_staff_schema)
    fact_payment = bigquery.Table(fact_payment_ref, schema=fact_payment_schema)

    # Set table partitioning

    fact_payment.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="payment_date"  # Partition by payment_date
    )

    # Create tables in BigQuery
    bq_client.create_table(dim_customer, exists_ok=True)
    bq_client.create_table(dim_film, exists_ok=True)
    bq_client.create_table(dim_staff, exists_ok=True)
    bq_client.create_table(fact_payment, exists_ok=True)

def create_dim_date_view():
    view_id = "dim_date"
    query = f"""
        WITH date_range AS (
        SELECT 
            DATE '2010-01-01' + INTERVAL day_num DAY AS full_date
        FROM 
            UNNEST(GENERATE_ARRAY(0, 365 * 30)) AS day_num
        )
        SELECT 
        FORMAT_DATE('%Y-%m-%d', full_date) AS date_id,
        FORMAT_DATE('%Y-%m-%d', full_date) AS full_date,
        EXTRACT(DAY FROM full_date) AS day,
        EXTRACT(DAYOFWEEK FROM full_date) AS day_of_week,
        FORMAT_DATE('%A', full_date) AS day_name,
        CASE 
            WHEN EXTRACT(DAYOFWEEK FROM full_date) IN (1, 7) THEN 1 
            ELSE 0 
        END AS is_weekend,
        EXTRACT(WEEK FROM full_date) AS week_of_year,
        EXTRACT(MONTH FROM full_date) AS month,
        FORMAT_DATE('%B', full_date) AS month_name,
        EXTRACT(QUARTER FROM full_date) AS quarter,
        EXTRACT(YEAR FROM full_date) AS year,
        FORMAT_DATE('%G', full_date) AS iso_year
        FROM 
        date_range
    """
    view = bigquery.Table(f"{os.getenv('G_CLOUD_PROJECT')}.{os.getenv('G_CLOUD_DATASET')}.{view_id}")
    view.view_query = query

    view = bq_client.create_table(view, exists_ok=True)

create_stg_tables()
create_dwh_tables()
create_dim_date_view()
print("Staging, DWH tables and date dimension view created successfully.")
