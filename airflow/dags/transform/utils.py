import pandas as pd
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor

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

def load_df(df, table_name, conflict_cols):
    conn = get_conn()
    cur = conn.cursor()

    columns = list(df.columns)
    col_str = ",".join(columns)

    values = [tuple(x) for x in df.to_numpy()]

    update_cols = [
        f"{col}=EXCLUDED.{col}"
        for col in columns
        if col not in conflict_cols
    ]

    update_str = ", ".join(update_cols)
    conflict_str = ",".join(conflict_cols)

    query = f"""
        INSERT INTO {table_name} ({col_str})
        VALUES %s
        ON CONFLICT ({conflict_str})
        DO UPDATE SET {update_str}
    """

    execute_values(cur, query, values)
    conn.commit()
    conn.close()

def load_df_insert_only(df, table_name):
    conn = get_conn()
    cur = conn.cursor()

    columns = ",".join(df.columns)
    values = [tuple(x) for x in df.to_numpy()]

    query = f"""
        INSERT INTO {table_name} ({columns})
        VALUES %s
        ON CONFLICT DO NOTHING
    """

    execute_values(cur, query, values)
    conn.commit()
    conn.close()

from psycopg2.extras import RealDictCursor
from datetime import date

def load_df_scd2(df, table_name, business_key, tracked_cols):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    insert_cols = [col for col in df.columns if col != business_key]
    insert_cols_sql = ", ".join(insert_cols)
    placeholders = ", ".join(["%s"] * len(insert_cols))

    for _, row in df.iterrows():
        bk_value = row[business_key]

        cur.execute(
            f"""
            SELECT *
            FROM {table_name}
            WHERE {business_key} = %s
              AND is_current = true
            """,
            (bk_value,)
        )
        current = cur.fetchone()

        if current is None:
            cur.execute(
                f"""
                INSERT INTO {table_name} (
                    {business_key},
                    {insert_cols_sql},
                    effective_start_date,
                    effective_end_date,
                    is_current
                )
                VALUES (
                    %s,
                    {placeholders},
                    CURRENT_DATE,
                    '9999-12-31',
                    true
                )
                """,
                [bk_value] + [row[col] for col in insert_cols]
            )
            continue

        has_changed = any(row[col] != current[col] for col in tracked_cols)

        if has_changed:
            # 🚨 THIS IS THE FIX
            if current["effective_start_date"] == date.today():
                continue

            cur.execute(
                f"""
                UPDATE {table_name}
                SET effective_end_date = CURRENT_DATE - INTERVAL '1 day',
                    is_current = false
                WHERE {business_key} = %s
                  AND is_current = true
                """,
                (bk_value,)
            )

            cur.execute(
                f"""
                INSERT INTO {table_name} (
                    {business_key},
                    {insert_cols_sql},
                    effective_start_date,
                    effective_end_date,
                    is_current
                )
                VALUES (
                    %s,
                    {placeholders},
                    CURRENT_DATE,
                    '9999-12-31',
                    true
                )
                """,
                [bk_value] + [row[col] for col in insert_cols]
            )

    conn.commit()
    conn.close()
