import psycopg2
import psycopg2.extras
import os
from dotenv import load_dotenv
load_dotenv()

def connect_to_source_db():
    conn = psycopg2.connect(
        dbname=os.getenv("DB_SOURCE_DATABASE"),
        user=os.getenv("DB_SOURCE_USER"),
        password=os.getenv("DB_SOURCE_PASS"),
        host=os.getenv("DB_SOURCE_HOST"), # Use the local IP address
        port=os.getenv("DB_SOURCE_PORT"),
    )
    return conn

def connect_to_stg_db():
    conn = psycopg2.connect(
        dbname=os.getenv("DB_STG_DATABASE"),
        user=os.getenv("DB_STG_USER"),
        password=os.getenv("DB_STG_PASS"),
        host=os.getenv("DB_STG_HOST"), # Use the local IP address
        port=os.getenv("DB_STG_PORT"),  
    )
    return conn

def source_to_stg_customer():
    conn = connect_to_source_db()
    cursor = conn.cursor()
    # Fetch data from source tables
    cursor.execute("""
        select
            c.customer_id,
            c.first_name,
            c.last_name,
            c.email,
            a.address,
            a.district,
            ci.city,
            co.country,
            a.postal_code,
            a.phone,
            c.activebool,
            c.create_date,
            current_timestamp as insertion_date
        from customer c
        left join address a on c.address_id = a.address_id
        left join city ci on a.city_id = ci.city_id
        left join country co on ci.country_id = co.country_id
    """)
    stg_customer = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close()

    conn_stg = connect_to_stg_db()
    cursor_stg = conn_stg.cursor()
    # Insert data into staging table batch insert
    query = """
        INSERT INTO stg_customer(customer_id, first_name, last_name, email, address, district, city, country, postal_code, phone, active, create_date, insertion_date)
        VALUES %s
    """
    psycopg2.extras.execute_values(cursor_stg, query, stg_customer, template=None, page_size=1000)
    conn_stg.commit()
    cursor_stg.close()
    conn_stg.close()

def source_to_stg_film():
    conn = connect_to_source_db()
    cursor = conn.cursor()
    # Fetch data from source tables
    cursor.execute("""
        select
            f.film_id,
            f.title,
            f.description,
            f.release_year,
            l.name as language,
            f.rental_duration,
            f.rental_rate,
            f.length,
            f.replacement_cost,
            f.rating,
            c.name as category,
            current_timestamp as insertion_date
        from film f
        left join language l on f.language_id = l.language_id
        left join film_category fc on f.film_id = fc.film_id
        left join category c on fc.category_id = c.category_id
    """)
    stg_film = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close()

    conn_stg = connect_to_stg_db()
    cursor_stg = conn_stg.cursor()
    # Insert data into staging table batch insert
    query = """
        INSERT INTO stg_film(film_id, title, description, release_year, language, rental_duration, rental_rate, length, replacement_cost, rating, category, insertion_date)
        VALUES %s
    """
    psycopg2.extras.execute_values(cursor_stg, query, stg_film, template=None, page_size=1000)
    conn_stg.commit()
    cursor_stg.close()
    conn_stg.close()  

def source_to_stg_staff():
    conn = connect_to_source_db()
    cursor = conn.cursor()
    # Fetch data from source tables
    cursor.execute("""
        select
            s.staff_id,
            s.first_name,
            s.last_name,
            a.address,
            a.district,
            ci.city,
            co.country,
            a.postal_code,
            a.phone,
            s.email,
            s.active,
            s.username as user_name,
            s.password,
            current_timestamp as insertion_date
        from staff s
        left join address a on s.address_id = a.address_id
        left join city ci on a.city_id = ci.city_id
        left join country co on ci.country_id = co.country_id
    """)
    stg_staff = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close()

    conn_stg = connect_to_stg_db()
    cursor_stg = conn_stg.cursor()
    # Insert data into staging table batch insert
    query = """
        INSERT INTO stg_staff(staff_id, first_name, last_name, address, district, city, country, postal_code, phone, email, active, user_name, password, insertion_date)
        VALUES %s
    """
    psycopg2.extras.execute_values(cursor_stg, query, stg_staff, template=None, page_size=1000)
    conn_stg.commit()
    cursor_stg.close()
    conn_stg.close()

def source_to_stg_payment():
    conn = connect_to_source_db()
    cursor = conn.cursor()
    # Fetch data from source tables
    cursor.execute("""
        select
            p.payment_id,
            p.customer_id,
            p.staff_id,
            p.rental_id,
            i.inventory_id,
            i.film_id,
            i.store_id,
            p.amount,
            p.payment_date,
            r.rental_date,
            r.return_date,
            current_timestamp as insertion_date
        from payment p
        left join rental r on p.rental_id = r.rental_id
        left join inventory i on r.inventory_id = i.inventory_id
        left join film f on i.film_id = f.film_id
    """)
    stg_payment = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close()

    conn_stg = connect_to_stg_db()
    cursor_stg = conn_stg.cursor()
    # Insert data into staging table batch insert
    query = """
        INSERT INTO stg_payment(payment_id, customer_id, staff_id, rental_id, inventory_id, film_id, store_id, amount, payment_date, rental_date, return_date, insertion_date)
        VALUES %s
    """
    psycopg2.extras.execute_values(cursor_stg, query, stg_payment, template=None, page_size=1000)
    conn_stg.commit()
    cursor_stg.close()
    conn_stg.close()
