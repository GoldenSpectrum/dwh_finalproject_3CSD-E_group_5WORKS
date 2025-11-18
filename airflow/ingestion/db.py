import pandas as pd
from sqlalchemy import create_engine

def get_engine():
    return create_engine(
        "postgresql+psycopg2://postgres:postgres@postgres:5432/shopzada"
    )


def load_to_staging(df, table_name, truncate=True):
    engine = get_engine()

    if truncate:
        with engine.connect() as conn:
            conn.execute(f"TRUNCATE TABLE {table_name};")

    df.to_sql(
        table_name,
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5000,
    )
