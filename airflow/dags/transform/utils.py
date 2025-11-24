import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

def get_conn():
    return psycopg2.connect(
        host="postgres",
        database="shopzada",
        user="postgres",
        password="postgres"
    )

def truncate_table(table_name: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;")
    conn.commit()
    cur.close()
    conn.close()


def fetch_df(query):
    conn = get_conn()
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def load_df(df, table_name):
    conn = get_conn()
    cur = conn.cursor()

    columns = ",".join(df.columns)
    values = [tuple(x) for x in df.to_numpy()]

    query = f"INSERT INTO {table_name} ({columns}) VALUES %s"
    execute_values(cur, query, values)

    conn.commit()
    conn.close()
